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
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>

#include "darray.h"
#include "libtcmu_log.h"
#include "libtcmu_config.h"

/* tcmu ring buffer for log */
#define LOG_ENTRY_LEN 256 /* entry[0] is reserved for pri */
#define LOG_MSG_LEN (LOG_ENTRY_LEN - 1) /* the length of the log message */
#define LOG_ENTRYS (1024 * 32)

#define LOG_BUF_SIZE (LOG_ITEMS * LOG_ITEM_LEN)

struct log_buf {
	unsigned int head;
	unsigned int tail;
	char buf[LOG_ENTRYS][LOG_ENTRY_LEN];
};

struct log_entry {
	pthread_cond_t cond;
	pthread_mutex_t lock;

	bool thread_active;
	bool finish_initialize;
	pthread_t thread_id;
	pid_t pid;

	struct log_buf *logbuf;
};

static darray(struct log_entry) log_entrys = darray_new();

static int tcmu_log_level = TCMU_LOG_WARN;
static struct log_entry *tcmu_log_initialize(void);

static inline struct log_entry *current_entry(void)
{
	struct log_entry *e;

	darray_foreach(e, log_entrys) {
		if (e->pid == getpid())
			return e;
	}

	return NULL;
}

/* covert log level from tcmu config to syslog */
static inline int to_syslog_level(int level)
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

void tcmu_set_log_level(int level)
{
	tcmu_log_level = to_syslog_level(level);
}

static void open_syslog(const char *ident, int option, int facility)
{
#define ID_MAX_LEN 16
	char id[ID_MAX_LEN] = {0}, path[128];
	int fd, len;

	if (!ident) {
		sprintf(path, "/proc/%d/comm", getpid());
		fd = open(path, O_RDONLY);
			if (fd < 0)
				return;
		len = read(fd, id, ID_MAX_LEN);
		if (len < 0) {
			close(fd);
			return;
		}
		close(fd);
	} else {
		strncpy(id, ident, ID_MAX_LEN);
	}

	openlog(id, option, facility);
}

static void close_syslog(void)
{
	closelog();
}

static inline void log_to_syslog(int pri, const char *logbuf)
{
	syslog(pri, "%s", logbuf);
}

static inline uint8_t log_get_pri(struct log_entry *e, unsigned int cur)
{
	return e->logbuf->buf[cur][0];
}

static inline void log_set_pri(struct log_entry *e, unsigned int cur, uint8_t pri)
{
	e->logbuf->buf[cur][0] = (char)pri;
}

static inline char *log_get_msg(struct log_entry *e, unsigned int cur)
{
	return e->logbuf->buf[cur] + 1;
}

static inline void update_log_tail(struct log_entry *e)
{
	e->logbuf->tail = (e->logbuf->tail + 1) % LOG_ENTRYS;
}

static inline void update_log_head(struct log_entry *e)
{
	e->logbuf->head = (e->logbuf->head + 1) % LOG_ENTRYS;

	/* when the ring buffer is full, the oldest log will be dropped */
	if (e->logbuf->head == e->logbuf->tail)
		update_log_tail(e);
}

static void
log_internal(int pri,
	     const char *funcname,
	     int linenr,
	     const char *fmt,
	     va_list args)
{
	struct log_entry *e;
	unsigned int head;
	char *msg;
	int n;

	if (pri > tcmu_log_level)
		return;

	if (!fmt)
		return;

	if (!(e = tcmu_log_initialize()))
		return;

	pthread_mutex_lock(&e->lock);

	head = e->logbuf->head;
	log_set_pri(e, head, pri);
	msg = log_get_msg(e, head);
	n = sprintf(msg, "%s:%d : ", funcname, linenr);
	vsnprintf(msg + n, LOG_MSG_LEN - n, fmt, args);

	update_log_head(e);

	if (e->thread_active == false)
		pthread_cond_signal(&e->cond);

	pthread_mutex_unlock(&e->lock);
}

void tcmu_err_message(const char *funcname, int linenr, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	log_internal(TCMU_LOG_ERROR, funcname, linenr, fmt, args);
	va_end(args);
}

void tcmu_warn_message(const char *funcname, int linenr, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	log_internal(TCMU_LOG_WARN, funcname, linenr, fmt, args);
	va_end(args);
}

void tcmu_info_message(const char *funcname, int linenr, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	log_internal(TCMU_LOG_INFO, funcname, linenr, fmt, args);
	va_end(args);
}

void tcmu_dbg_message(const char *funcname, int linenr, const char *fmt, ...)
{
	va_list args;

	va_start(args, fmt);
	log_internal(TCMU_LOG_DEBUG, funcname, linenr, fmt, args);
	va_end(args);
}

static void log_output(int pri, const char *msg)
{
	log_to_syslog(pri, msg);
}

static bool log_buf_not_empty_output(void)
{	
	struct log_entry *e;
	unsigned int tail;
	uint8_t pri;
	char *msg, buf[LOG_MSG_LEN];
	
	e = current_entry();
	if (!e)
		return false;

	pthread_mutex_lock(&e->lock);
	if (e->logbuf->tail == e->logbuf->head) {
		pthread_mutex_unlock(&e->lock);
		return false;
	}

	tail = e->logbuf->tail;
	pri = log_get_pri(e, tail);
	msg = log_get_msg(e, tail);
	memcpy(buf, msg, LOG_MSG_LEN);
	update_log_tail(e);
	pthread_mutex_unlock(&e->lock);

	/*
 	 * This may block due to rsyslog and syslog-ng, etc.
 	 * And the log productors could still insert their log
 	 * messages into the ring buffer without blocking. But
 	 * the ring buffer may lose some old log entrys if the
 	 * ring buffer is full.
 	 */
	log_output(pri, buf);

	return true;
}

static void log_entry_remove(struct log_entry *e)
{
	unsigned int i = 0;

	darray_foreach(e, log_entrys) {
		if (e->pid != getpid())
			i++;
		else
			darray_remove(log_entrys, i);
	}
}

static void cancel_log_thread(pthread_t thread)
{
	void *join_retval;
	int ret;

	ret = pthread_cancel(thread);
	if (ret) {
		/* TODO: will default to log file */
		return;
	}

	ret = pthread_join(thread, &join_retval);
	if (ret) {
		/* TODO: will default to log file */
		return;
	}

	if (join_retval != PTHREAD_CANCELED);
		/* TODO: will default to log file */
}

void tcmu_cancel_log_thread(void)
{
	struct log_entry *e;

	darray_foreach(e, log_entrys) {
		if (e->pid == getpid()) {
			cancel_log_thread(e->thread_id);
			break;
		}
	}
}

static void log_thread_cleanup(void *arg)
{
	struct log_entry *e = arg;

	pthread_cond_destroy(&e->cond);
	pthread_mutex_destroy(&e->lock);
	free(e->logbuf);
	log_entry_remove(e);
	close_syslog();
}


static void *log_thread_start(void *arg)
{
	struct log_entry *e = arg;

	pthread_cleanup_push(log_thread_cleanup, arg);

	while (1) {
		pthread_mutex_lock(&e->lock);
		if (!e->finish_initialize) {
			e->finish_initialize = true;
			pthread_cond_signal(&e->cond);
		}

		e->thread_active = false;
		pthread_cond_wait(&e->cond, &e->lock);
		e->thread_active = true;
		pthread_mutex_unlock(&e->lock);

		while (log_buf_not_empty_output());
	}

	pthread_cleanup_pop(1);
	return NULL;
}

static struct log_entry *tcmu_log_initialize(void)
{
	struct log_entry entry, *e;
	int ret;

	e = current_entry();
	if (e)
		return e;

	open_syslog(NULL, 0, 0);

	entry.pid = getpid();

	entry.logbuf = malloc(sizeof(struct log_buf));
	if (!entry.logbuf)
		return NULL;

	entry.thread_active = false;
	entry.finish_initialize = false;
	entry.logbuf->head = 0;
	entry.logbuf->tail = 0;
	pthread_cond_init(&entry.cond, NULL);
	pthread_mutex_init(&entry.lock, NULL);

	darray_append(log_entrys, entry);
	e = current_entry();

	ret = pthread_create(&e->thread_id, NULL, log_thread_start, e);
	if (ret) {
		free(e->logbuf);
		log_entry_remove(e);
		return NULL;
	}

	pthread_mutex_lock(&e->lock);
	if (!e->finish_initialize)
		pthread_cond_wait(&e->cond, &e->lock);
	pthread_mutex_unlock(&e->lock);

	return e;
}
