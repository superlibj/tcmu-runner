/*
 * Copyright 2016, China Mobile, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

#define _GNU_SOURCE
#include <stdint.h>
#include <pthread.h>
#include "libtcmu_log.h"
#include "libtcmu_config.h"

/* tcmu ring buffer for log */
#define LOG_ENTRY_LEN 256 /* the max length each log pri + message */
#define LOG_ENTRYS 10240

struct log_buf_mailbox {
	bool thread_active;
	pthread_cond_t cond;
	pthread_mutex_t lock;
	
	unsigned int log_head;
	unsigned int log_tail;
	char log_buf[LOG_ENTRYS][LOG_ENTRY_LEN];
};

struct log_buf_mailbox *tcmu_log_buf = NULL;

static int tcmu_log_level = TCMU_LOG_WARN;

static bool tcmu_log_initialize(void);

static inline int tcmu_log_level_conf_to_syslog(int level)
{
	switch (level) {
		case TCMU_CONF_LOG_ERROR:
			return TCMU_LOG_ERROR;
		case TCMU_CONF_LOG_WARN:
			return TCMU_LOG_WARN;
		case TCMU_CONF_LOG_INFO:
			return TCMU_LOG_INFO;
		case TCMU_CONF_LOG_DEBUG:

			return TCMU_LOG_DEBUG;
		default:
			return TCMU_LOG_WARN;
	}
}

/* get the log level of tcmu-runner */
unsigned int tcmu_get_log_level(void)
{
	return tcmu_log_level;
}

/* covert log level from tcmu config to syslog */
void tcmu_set_log_level(int level)
{
	tcmu_log_level = tcmu_log_level_conf_to_syslog(level);
}

void tcmu_log_open_syslog(const char *ident, int option, int facility)
{
	const char *id = TCMU_IDENT;

	if (ident)
		id = ident;

	openlog(id, option, facility);
}

void tcmu_log_close_syslog(void)
{
	closelog();
}

static inline void tcmu_log_to_syslog(int pri, const char *logbuf)
{
	syslog(pri, "%s", logbuf);
}

static inline uint8_t log_get_pri(unsigned int cur)
{
	return tcmu_log_buf->log_buf[cur][0];
}

static inline void log_set_pri(unsigned int cur, uint8_t pri)
{
	tcmu_log_buf->log_buf[cur][0] = (char)pri;
}

static inline char *log_get_msg(unsigned int cur)
{
	return tcmu_log_buf->log_buf[cur] + 1;
}

static inline void update_log_tail(void)
{
	tcmu_log_buf->log_tail = (tcmu_log_buf->log_tail + 1) % LOG_ENTRYS;
}

static inline void update_log_head(void)
{
	tcmu_log_buf->log_head = (tcmu_log_buf->log_head + 1) % LOG_ENTRYS;

	/* when the ring buffer is full, the oldest log will be dropped */
	if (tcmu_log_buf->log_head == tcmu_log_buf->log_tail)
		update_log_tail();
}

static void
tcmu_log_internal(int pri,
		  const char *funcname,
		  int linenr,
		  const char *fmt,
		  va_list args)
{
	unsigned int head;
	char *msg;
	int n;

	if (!tcmu_log_initialize())
		return;

	if (pri > tcmu_log_level)
		return;

	if (!fmt)
		return;

	pthread_mutex_lock(&tcmu_log_buf->lock);

	head = tcmu_log_buf->log_head;
	log_set_pri(head, pri);
	msg = log_get_msg(head);
	n = sprintf(msg, "%s:%d : ", funcname, linenr);
	vsnprintf(msg + n, LOG_ENTRY_LEN - n - 1, fmt, args);

	update_log_head();
	if (tcmu_log_buf->thread_active == false)
		pthread_cond_signal(&tcmu_log_buf->cond);
	pthread_mutex_unlock(&tcmu_log_buf->lock);
}

void tcmu_err_message(const char *funcname, int linenr, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	tcmu_log_internal(TCMU_LOG_ERROR, funcname, linenr, fmt, args);
	va_end(args);
}

void tcmu_warn_message(const char *funcname, int linenr, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	tcmu_log_internal(TCMU_LOG_WARN, funcname, linenr, fmt, args);
	va_end(args);
}

void tcmu_info_message(const char *funcname, int linenr, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	tcmu_log_internal(TCMU_LOG_INFO, funcname, linenr, fmt, args);
	va_end(args);
}

void tcmu_dbg_message(const char *funcname, int linenr, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	tcmu_log_internal(TCMU_LOG_DEBUG, funcname, linenr, fmt, args);
	va_end(args);
}

static void log_output(int pri, const char *msg)
{
	tcmu_log_to_syslog(pri, msg);
}

static bool log_buf_not_empty_output(void)
{
	unsigned int tail;
	uint8_t pri;
	char *msg;
	
	pthread_mutex_lock(&tcmu_log_buf->lock);
	if (tcmu_log_buf->log_tail == tcmu_log_buf->log_head) {
		pthread_mutex_unlock(&tcmu_log_buf->lock);
		return false;
	}

	tail = tcmu_log_buf->log_tail;
	pri = log_get_pri(tail);
	msg = log_get_msg(tail);
	log_output(pri, msg);

	update_log_tail();
	pthread_mutex_unlock(&tcmu_log_buf->lock);

	return true;
}

static bool finish_initialize = false;
static void log_thread_cleanup(void *arg)
{
	struct log_buf_mailbox *mailbox = arg;

	pthread_cond_destroy(&tcmu_log_buf->cond);
	pthread_mutex_destroy(&tcmu_log_buf->lock);
	free(mailbox);
	finish_initialize = false;
}


static void *log_thread_start(void *arg)
{
	pthread_cleanup_push(log_thread_cleanup, tcmu_log_buf);

	while (1) {
		pthread_mutex_lock(&tcmu_log_buf->lock);
		tcmu_log_buf->thread_active = false;
		pthread_cond_wait(&tcmu_log_buf->cond, &tcmu_log_buf->lock);
		tcmu_log_buf->thread_active = true;
		pthread_mutex_unlock(&tcmu_log_buf->lock);

		while (log_buf_not_empty_output());
	}

	pthread_cleanup_pop(1);
	return NULL;
}

static bool tcmu_log_initialize(void)
{
	pthread_t thread_id;
	int ret;

	if (finish_initialize)
		return true;

	tcmu_log_buf = malloc(sizeof(*tcmu_log_buf));
	if (!tcmu_log_buf)
		return false;

	tcmu_log_buf->thread_active = false;
	tcmu_log_buf->log_head = 0;
	tcmu_log_buf->log_tail = 0;
	pthread_cond_init(&tcmu_log_buf->cond, NULL);
	pthread_mutex_init(&tcmu_log_buf->lock, NULL);

	ret = pthread_create(&thread_id, NULL, log_thread_start, NULL);
	if (ret) {
		free(tcmu_log_buf);
		return false;
	}

	finish_initialize = true;

	return true;
}
