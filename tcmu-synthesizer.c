/*
 * Copyright 2016, Red Hat, Inc.
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
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <assert.h>
#include <glib.h>
#include <gio/gio.h>
#include <getopt.h>
#include <pthread.h>
#include <signal.h>
#include <scsi/scsi.h>
#include "scsi_defs.h"

#include "libtcmu.h"
#include "version.h"
#include "libtcmu_config.h"

typedef struct {
	GIOChannel *gio;
	int watcher_id;
} syn_dev_t;

static bool syn_check_config(const char *cfgstring, char **reason)
{
	tcmu_dbg("syn check config\n");
	if (strcmp(cfgstring, "syn/null")) {
		asprintf(reason, "invalid option");
		return false;
	}
	return true;
}

static int syn_handle_cmd(struct tcmu_device *dev, uint8_t *cdb,
			  struct iovec *iovec, size_t iov_cnt,
			  uint8_t *sense)
{
	uint8_t cmd;

	cmd = cdb[0];
	tcmu_dbg("syn handle cmd %d\n", cmd);

	switch (cmd) {
	case INQUIRY:
		return tcmu_emulate_inquiry(dev, cdb, iovec, iov_cnt, sense);
		break;
	case TEST_UNIT_READY:
		return tcmu_emulate_test_unit_ready(cdb, iovec, iov_cnt, sense);
		break;
	case SERVICE_ACTION_IN_16:
		if (cdb[1] == READ_CAPACITY_16)
			return tcmu_emulate_read_capacity_16(1 << 20,
							     512,
							     cdb, iovec,
							     iov_cnt, sense);
		else
			return TCMU_NOT_HANDLED;
		break;
	case MODE_SENSE:
	case MODE_SENSE_10:
		return tcmu_emulate_mode_sense(cdb, iovec, iov_cnt, sense);
		break;
	case MODE_SELECT:
	case MODE_SELECT_10:
		return tcmu_emulate_mode_select(cdb, iovec, iov_cnt, sense);
		break;
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
		return SAM_STAT_GOOD;

	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
		return SAM_STAT_GOOD;

	default:
		tcmu_dbg("unknown command %x\n", cdb[0]);
		return TCMU_NOT_HANDLED;
	}
}

static gboolean syn_dev_callback(GIOChannel *source,
				 GIOCondition condition,
				 gpointer data)
{
	int ret;
	struct tcmu_device *dev = data;
	struct tcmulib_cmd *cmd;

	tcmu_dbg("dev fd cb\n");
	tcmulib_processing_start(dev);

	while ((cmd = tcmulib_get_next_command(dev)) != NULL) {
		ret = syn_handle_cmd(dev,
				     cmd->cdb,
				     cmd->iovec,
				     cmd->iov_cnt,
				     cmd->sense_buf);
		tcmulib_command_complete(dev, cmd, ret);
	}

	tcmulib_processing_complete(dev);
	return TRUE;
}

static int syn_added(struct tcmu_device *dev)
{
	syn_dev_t *s = g_new0(syn_dev_t, 1);

	tcmu_dbg("added %s\n", tcmu_get_dev_cfgstring(dev));
	tcmu_set_dev_private(dev, s);
	s->gio = g_io_channel_unix_new(tcmu_get_dev_fd(dev));
	s->watcher_id = g_io_add_watch(s->gio, G_IO_IN,
				       syn_dev_callback, dev);
	return 0;
}

static void syn_removed(struct tcmu_device *dev)
{
	syn_dev_t *s = tcmu_get_dev_private(dev);
	tcmu_dbg("removed %s\n", tcmu_get_dev_cfgstring(dev));
	g_source_remove(s->watcher_id);
	g_io_channel_unref(s->gio);
	g_free(s);
}

struct tcmulib_handler syn_handler = {
	.name = "syn",
	.subtype = "syn",
	.cfg_desc = "valid options:\n"
		    "null: a nop storage where R/W requests are completed "
		    "immediately, like the null_blk device.",
	.check_config = syn_check_config,
	.added = syn_added,
	.removed = syn_removed,
};

gboolean tcmulib_callback(GIOChannel *source,
			  GIOCondition condition,
			  gpointer data)
{
	struct tcmulib_context *ctx = data;

	tcmu_dbg("master fd ready\n");
	tcmulib_master_fd_ready(ctx);

	return TRUE;
}

static void sighandler(int signal)
{
	tcmu_err("signal %d received!\n", signal);

	tcmu_cancel_log_thread();

	exit(1);
}

static struct sigaction sync_sigaction = {
	.sa_handler = sighandler,
};

static void usage(void) {
	printf("\nusage:\n");
	printf("\ttcmu-synthesizer [options]\n");
	printf("\noptions:\n");
	printf("\t-h, --help: print this message and exit\n");
	printf("\t-V, --version: print version and exit\n");
	printf("\t-d, --debug: enable debug messages\n");
	printf("\n");
}

static struct option long_options[] = {
	{"debug", no_argument, 0, 'd'},
	{"help", no_argument, 0, 'h'},
	{"version", no_argument, 0, 'V'},
	{0, 0, 0, 0},
};

int main(int argc, char **argv)
{
	GMainLoop *loop;
	GIOChannel *libtcmu_gio;
	struct tcmulib_context *ctx;
	int ret;

	while (1) {
		int c;
		int option_index = 0;

		c = getopt_long(argc, argv, "dhV",
				long_options, &option_index);
		if (c == -1)
			break;

		switch (c) {
		case 'd':
			tcmu_set_log_level(TCMU_CONF_LOG_DEBUG);
			break;
		case 'V':
			printf("tcmu-synthesizer %s\n", TCMUR_VERSION);
			exit(1);
		default:
		case 'h':
			usage();
			exit(1);
		}
	}

	ctx = tcmulib_initialize(&syn_handler, 1);
	if (!ctx) {
		tcmu_err("tcmulib_initialize failed\n");
		exit(1);
	}

	ret = sigaction(SIGINT, &sync_sigaction, NULL);
	if (ret) {
		tcmu_err("couldn't set sigaction\n");
		exit(1);
	}

	tcmulib_register(ctx);
	/* Set up event for libtcmu */
	libtcmu_gio = g_io_channel_unix_new(tcmulib_get_master_fd(ctx));
	g_io_add_watch(libtcmu_gio, G_IO_IN, tcmulib_callback, ctx);
	loop = g_main_loop_new(NULL, FALSE);
	g_main_loop_run(loop);
	g_main_loop_unref(loop);
	return 0;
}
