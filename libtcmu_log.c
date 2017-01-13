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

pthread_mutex_t g_mutex;  

/* Atomic Operations */
typedef struct {
       volatile int counter;
} atomic_t;

#define ATOMIC_INIT(i) {(i)}
atomic_t g_mutex_initializer = ATOMIC_INIT(0);
static inline int atomic_compare_and_set(atomic_t *v, int oldval, int newval)
{
	return __sync_bool_compare_and_swap(&v->counter, oldval, newval);
}

/* tcmu ring buffer for log */
#define LOG_ENTRY_LEN 256 /* rb[0] is reserved for pri */
#define LOG_MSG_LEN (LOG_ENTRY_LEN - 1) /* the length of the log message */
#define LOG_ENTRYS (1024 * 32)

struct log_buf {
	pthread_cond_t cond;
	pthread_mutex_t lock;

	bool thread_active;
	bool finish_initialize;
	bool rb_closed;

	unsigned int head;
	unsigned int tail;
	char buf[LOG_ENTRYS][LOG_ENTRY_LEN];
};

struct log_rb {
	pthread_t thread_id;
	pid_t pid;
	struct log_buf *logbuf;
};

static darray(struct log_rb) log_rbs = darray_new();

static int tcmu_log_level = TCMU_LOG_WARN;
static struct log_buf *tcmu_log_initialize(void);

static inline struct log_buf *current_rb(void)
{
	struct log_rb *r;

	darray_foreach(r, log_rbs) {
		if (r->pid == getpid())
			return r->logbuf;
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

static inline uint8_t rb_get_pri(struct log_buf *logbuf, unsigned int cur)
{
	return logbuf->buf[cur][0];
}

static inline void rb_set_pri(struct log_buf *logbuf, unsigned int cur, uint8_t pri)
{
	logbuf->buf[cur][0] = (char)pri;
}

static inline char *rb_get_msg(struct log_buf *logbuf, unsigned int cur)
{
	return logbuf->buf[cur] + 1;
}

static inline bool rb_is_empty(struct log_buf *logbuf)
{
	return logbuf->tail == logbuf->head;
}

static inline bool rb_is_full(struct log_buf *logbuf)
{
	return logbuf->tail == (logbuf->head + 1) % LOG_ENTRYS;
}

static inline void rb_update_tail(struct log_buf *logbuf)
{
	logbuf->tail = (logbuf->tail + 1) % LOG_ENTRYS;
}

static inline void rb_update_head(struct log_buf *logbuf)
{
	/* when the ring buffer is full, the oldest log will be dropped */
	if (rb_is_full(logbuf))
		rb_update_tail(logbuf);

	logbuf->head = (logbuf->head + 1) % LOG_ENTRYS;
}

static void
log_internal(int pri,
	     const char *funcname,
	     int linenr,
	     const char *fmt,
	     va_list args)
{
	struct log_buf *logbuf;
	unsigned int head;
	char *msg;
	int n;

	if (pri > tcmu_log_level)
		return;

	if (!fmt)
		return;

	if (!(logbuf = tcmu_log_initialize()))
		return;

	pthread_mutex_lock(&logbuf->lock);

	if (logbuf->rb_closed) {
		pthread_mutex_unlock(&logbuf->lock);
		return;
	}

	head = logbuf->head;
	rb_set_pri(logbuf, head, pri);
	msg = rb_get_msg(logbuf, head);
	n = sprintf(msg, "%s:%d : ", funcname, linenr);
	vsnprintf(msg + n, LOG_MSG_LEN - n, fmt, args);

	rb_update_head(logbuf);

	if (logbuf->thread_active == false)
		pthread_cond_signal(&logbuf->cond);

	pthread_mutex_unlock(&logbuf->lock);
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
	struct log_buf *logbuf;
	unsigned int tail;
	uint8_t pri;
	char *msg, buf[LOG_MSG_LEN];
	
	pthread_mutex_lock(&g_mutex);
	logbuf = current_rb();
	if (!logbuf) {
		pthread_mutex_unlock(&g_mutex);
		return false;
	}
	pthread_mutex_unlock(&g_mutex);

	pthread_mutex_lock(&logbuf->lock);
	if (rb_is_empty(logbuf)) {
		pthread_mutex_unlock(&logbuf->lock);
		return false;
	}

	tail = logbuf->tail;
	pri = rb_get_pri(logbuf, tail);
	msg = rb_get_msg(logbuf, tail);
	memcpy(buf, msg, LOG_MSG_LEN);
	rb_update_tail(logbuf);
	pthread_mutex_unlock(&logbuf->lock);

	/*
 	 * This may block due to rsyslog and syslog-ng, etc.
 	 * And the log productors could still insert their log
 	 * messages into the ring buffer without blocking. But
 	 * the ring buffer may lose some old log rbs if the
 	 * ring buffer is full.
 	 */
	log_output(pri, buf);

	return true;
}

static void log_rb_remove(void)
{
	struct log_rb *r;
	unsigned int i = 0;

	darray_foreach(r, log_rbs) {
		if (r->pid != getpid())
			i++;
		else
			darray_remove(log_rbs, i);
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

	if (join_retval != PTHREAD_CANCELED)
		;
		/* TODO: will default to log file */
}

void tcmu_cancel_log_thread(void)
{
	struct log_rb *r;

	pthread_mutex_lock(&g_mutex);
	darray_foreach(r, log_rbs) {
		if (r->pid == getpid()) {
			pthread_t thread_id = r->thread_id;
			
			pthread_mutex_unlock(&g_mutex);
			cancel_log_thread(thread_id);
			return;
		}
	}
	pthread_mutex_unlock(&g_mutex);
}

static void log_thread_cleanup(void *arg)
{
	struct log_buf *logbuf = arg;

	pthread_mutex_lock(&g_mutex);
	pthread_cond_destroy(&logbuf->cond);
	pthread_mutex_destroy(&logbuf->lock);
	pthread_mutex_destroy(&logbuf->lock);
	log_rb_remove();
	free(logbuf);
	pthread_mutex_unlock(&g_mutex);

	close_syslog();
}


static void *log_thread_start(void *arg)
{
	struct log_buf *logbuf = arg;

	pthread_cleanup_push(log_thread_cleanup, arg);

	open_syslog(NULL, 0, 0);

	while (1) {
		pthread_mutex_lock(&logbuf->lock);
		if (!logbuf->finish_initialize) {
			logbuf->finish_initialize = true;
			pthread_cond_signal(&logbuf->cond);
		}

		logbuf->thread_active = false;
		pthread_cond_wait(&logbuf->cond, &logbuf->lock);
		logbuf->thread_active = true;
		pthread_mutex_unlock(&logbuf->lock);

		while (log_buf_not_empty_output());
	}

	pthread_cleanup_pop(1);
	return NULL;
}

static inline bool g_mutex_has_initialized(void)
{
	return !atomic_compare_and_set(&g_mutex_initializer, 0, 1);
}

static void g_mutex_init(void)
{
	int ret;

	if (g_mutex_has_initialized())
		return;

	pthread_mutexattr_t attr;
	pthread_mutexattr_init(&attr);
	ret = pthread_mutexattr_setpshared(&attr,PTHREAD_PROCESS_SHARED);
	if( ret != 0 )
	{
		printf("pthread_mutexattr_setpshared error\n");
		exit(1);
	}
	pthread_mutex_init(&g_mutex, &attr);
}

static struct log_buf *tcmu_log_initialize(void)
{
	struct log_rb rb;
	struct log_buf *logbuf;
	int ret;

	g_mutex_init();

	pthread_mutex_lock(&g_mutex);
	logbuf = current_rb();
	if (logbuf) {
		pthread_mutex_unlock(&g_mutex);
		return logbuf;
	}

	rb.logbuf = malloc(sizeof(struct log_buf));
	if (!rb.logbuf) {
		pthread_mutex_unlock(&g_mutex);
		return NULL;
	}

	rb.pid = getpid();
	rb.logbuf->thread_active = false;
	rb.logbuf->finish_initialize = false;
	rb.logbuf->rb_closed = false;
	rb.logbuf->head = 0;
	rb.logbuf->tail = 0;
	pthread_cond_init(&rb.logbuf->cond, NULL);
	pthread_mutex_init(&rb.logbuf->lock, NULL);

	ret = pthread_create(&rb.thread_id, NULL, log_thread_start, rb.logbuf);
	if (ret) {
		log_rb_remove();
		free(rb.logbuf);
		pthread_mutex_unlock(&g_mutex);
		return NULL;
	}

	darray_append(log_rbs, rb);

	pthread_mutex_unlock(&g_mutex);

	pthread_mutex_lock(&rb.logbuf->lock);
	while (!rb.logbuf->finish_initialize)
		pthread_cond_wait(&rb.logbuf->cond, &rb.logbuf->lock);
	pthread_mutex_unlock(&rb.logbuf->lock);

	return rb.logbuf;
}
