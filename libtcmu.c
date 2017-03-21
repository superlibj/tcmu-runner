/*
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
#define _BITS_UIO_H
#include <memory.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>
#include <sys/mman.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <errno.h>
#include <dirent.h>
#include <scsi/scsi.h>

#include <linux/target_core_user.h>

#include <libnl3/netlink/genl/genl.h>
#include <libnl3/netlink/genl/mngt.h>
#include <libnl3/netlink/genl/ctrl.h>

#include "libtcmu.h"
#include "libtcmu_log.h"
#include "libtcmu_priv.h"
#include "tcmu-runner.h"

#define ARRAY_SIZE(X) (sizeof(X) / sizeof((X)[0]))

static struct nla_policy tcmu_attr_policy[TCMU_ATTR_MAX+1] = {
	[TCMU_ATTR_DEVICE]	= { .type = NLA_STRING },
	[TCMU_ATTR_MINOR]	= { .type = NLA_U32 },
};

static darray(struct tcmu_thread) g_threads = darray_new();

static int add_device(struct tcmulib_context *ctx, char *dev_name, char *cfgstring);
static void remove_device(struct tcmulib_context *ctx, char *dev_name, char *cfgstring);

static int handle_netlink(struct nl_cache_ops *unused, struct genl_cmd *cmd,
			  struct genl_info *info, void *arg)
{
	struct tcmulib_context *ctx = arg;
	char buf[32];

	if (!info->attrs[TCMU_ATTR_MINOR] || !info->attrs[TCMU_ATTR_DEVICE]) {
		tcmu_err("TCMU_ATTR_MINOR or TCMU_ATTR_DEVICE not set, doing nothing\n");
		return 0;
	}

	snprintf(buf, sizeof(buf), "uio%d", nla_get_u32(info->attrs[TCMU_ATTR_MINOR]));

	switch (cmd->c_id) {
	case TCMU_CMD_ADDED_DEVICE:
		add_device(ctx, buf, nla_get_string(info->attrs[TCMU_ATTR_DEVICE]));
		break;
	case TCMU_CMD_REMOVED_DEVICE:
		remove_device(ctx, buf, nla_get_string(info->attrs[TCMU_ATTR_DEVICE]));
		break;
	default:
		tcmu_err("Unknown notification %d\n", cmd->c_id);
	}

	return 0;
}

static struct genl_cmd tcmu_cmds[] = {
	{
		.c_id		= TCMU_CMD_ADDED_DEVICE,
		.c_name		= "ADDED DEVICE",
		.c_msg_parser	= handle_netlink,
		.c_maxattr	= TCMU_ATTR_MAX,
		.c_attr_policy	= tcmu_attr_policy,
	},
	{
		.c_id		= TCMU_CMD_REMOVED_DEVICE,
		.c_name		= "REMOVED DEVICE",
		.c_msg_parser	= handle_netlink,
		.c_maxattr	= TCMU_ATTR_MAX,
		.c_attr_policy	= tcmu_attr_policy,
	},
};

static struct genl_ops tcmu_ops = {
	.o_name		= "TCM-USER",
	.o_cmds		= tcmu_cmds,
	.o_ncmds	= ARRAY_SIZE(tcmu_cmds),
};

static struct nl_sock *setup_netlink(struct tcmulib_context *ctx)
{
	struct nl_sock *sock;
	int ret;

	sock = nl_socket_alloc();
	if (!sock) {
		tcmu_err("couldn't alloc socket\n");
		return NULL;
	}

	nl_socket_disable_seq_check(sock);

	nl_socket_modify_cb(sock, NL_CB_VALID, NL_CB_CUSTOM, genl_handle_msg, ctx);

	ret = genl_connect(sock);
	if (ret < 0) {
		tcmu_err("couldn't connect\n");
		goto err_free;
	}

	ret = genl_register_family(&tcmu_ops);
	if (ret < 0) {
		tcmu_err("couldn't register family\n");
		goto err_close;
	}

	ret = genl_ops_resolve(sock, &tcmu_ops);
	if (ret < 0) {
		tcmu_err("couldn't resolve ops, is target_core_user.ko loaded?\n");
		goto err_close;
	}

	ret = genl_ctrl_resolve_grp(sock, "TCM-USER", "config");

	ret = nl_socket_add_membership(sock, ret);
	if (ret < 0) {
		tcmu_err("couldn't add membership\n");
		goto err_close;
	}

	return sock;

err_close:
	nl_close(sock);
err_free:
	nl_socket_free(sock);

	return NULL;
}

static void teardown_netlink(struct nl_sock *sock)
{
	nl_close(sock);
	nl_socket_free(sock);
}

static void cancel_thread(pthread_t thread)
{
	void *join_retval;
	int ret;

	ret = pthread_cancel(thread);
	if (ret) {
		tcmu_err("pthread_cancel failed with value %d\n", ret);
		return;
	}

	ret = pthread_join(thread, &join_retval);
	if (ret) {
		tcmu_err("pthread_join failed with value %d\n", ret);
		return;
	}

	if (join_retval != PTHREAD_CANCELED)
		tcmu_err("unexpected join retval: %p\n", join_retval);
}

static struct tcmulib_handler *find_handler(struct tcmulib_context *ctx,
					    char *cfgstring)
{
	struct tcmulib_handler *handler;
	size_t len;
	char *found_at;

	found_at = strchrnul(cfgstring, '/');
	len = found_at - cfgstring;

	darray_foreach(handler, ctx->handlers) {
		if (!strncmp(cfgstring, handler->subtype, len))
		    return handler;
	}

	return NULL;
}

/*
 * convert errno to closest possible SAM status code.
 * (add more conversions as required)
 */
static int errno_to_sam_status(int rc, uint8_t *sense)
{
	if (rc == -ENOMEM) {
		return SAM_STAT_TASK_SET_FULL;
	} else if (rc == -EIO) {
		return tcmu_set_sense_data(sense, MEDIUM_ERROR,
					   ASC_READ_ERROR, NULL);
	} else if (rc < 0) {
		return TCMU_NOT_HANDLED;
	} else {
		return SAM_STAT_GOOD;
	}
}

static void cmdproc_thread_cleanup(void *arg)
{
	struct tcmu_device *dev = arg;
	struct tcmulib_handler *handler = tcmu_get_dev_handler(dev);
	struct tcmur_handler *r_handler = handler->hm_private;

	r_handler->close(dev);
	free(dev);
}

static int generic_handle_cmd(struct tcmu_device *dev,
			      struct tcmulib_cmd *tcmulib_cmd)
{
	struct tcmulib_handler *handler = tcmu_get_dev_handler(dev);
	struct tcmur_handler *store = handler->hm_private;
	uint8_t *cdb = tcmulib_cmd->cdb;
	struct iovec *iovec = tcmulib_cmd->iovec;
	size_t iov_cnt = tcmulib_cmd->iov_cnt;
	uint8_t *sense = tcmulib_cmd->sense_buf;
	uint32_t block_size = tcmu_get_dev_block_size(dev);
	uint64_t num_lbas = tcmu_get_dev_num_lbas(dev);
	uint8_t cmd;
	ssize_t ret = TCMU_NOT_HANDLED, l = tcmu_iovec_length(iovec, iov_cnt);
	off_t offset = block_size * tcmu_get_lba(cdb);
	struct iovec iov;
	size_t half = l / 2;
	uint32_t cmp_offset;

	if (store->handle_cmd)
		ret = store->handle_cmd(dev, tcmulib_cmd);

	if (ret != TCMU_NOT_HANDLED)
		return ret;

	cmd = cdb[0];
	switch (cmd) {
	case INQUIRY:
		return tcmu_emulate_inquiry(dev, cdb, iovec, iov_cnt, sense);
	case TEST_UNIT_READY:
		return tcmu_emulate_test_unit_ready(cdb, iovec, iov_cnt, sense);
	case SERVICE_ACTION_IN_16:
		if (cdb[1] == READ_CAPACITY_16)
			return tcmu_emulate_read_capacity_16(num_lbas,
							     block_size,
							     cdb, iovec,
							     iov_cnt, sense);
		else
			return TCMU_NOT_HANDLED;
	case READ_CAPACITY:
		if ((cdb[1] & 0x01) || (cdb[8] & 0x01))
			/* Reserved bits for MM logical units */
			return tcmu_set_sense_data(sense, ILLEGAL_REQUEST,
						   ASC_INVALID_FIELD_IN_CDB,
						   NULL);
		else
			return tcmu_emulate_read_capacity_10(num_lbas,
							     block_size,
							     cdb, iovec,
							     iov_cnt, sense);
	case MODE_SENSE:
	case MODE_SENSE_10:
		return tcmu_emulate_mode_sense(cdb, iovec, iov_cnt, sense);
	case START_STOP:
		return tcmu_emulate_start_stop(dev, cdb, sense);
	case MODE_SELECT:
	case MODE_SELECT_10:
		return tcmu_emulate_mode_select(cdb, iovec, iov_cnt, sense);
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
		ret = store->read(dev, iovec, iov_cnt, offset);
		if (ret != l) {
			tcmu_err("Error on read %x, %x\n", ret, l);
			return tcmu_set_sense_data(sense, MEDIUM_ERROR,
						   ASC_READ_ERROR, NULL);
		} else
			return SAM_STAT_GOOD;
	case WRITE_VERIFY:
		return tcmu_emulate_write_verify(dev, tcmulib_cmd,
						 store->read,
						 store->write,
						 iovec, iov_cnt, offset);
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
		ret = store->write(dev, iovec, iov_cnt, offset);
		if (ret != l) {
			tcmu_err("Error on write %x, %x\n", ret, l);
			return tcmu_set_sense_data(sense, MEDIUM_ERROR,
						   ASC_READ_ERROR, NULL);
		} else
			return SAM_STAT_GOOD;
	case SYNCHRONIZE_CACHE:
	case SYNCHRONIZE_CACHE_16:
		ret = store->flush(dev);
		if (ret < 0) {
			tcmu_err("Error on flush %x\n", ret);
			return tcmu_set_sense_data(sense, MEDIUM_ERROR,
						   ASC_READ_ERROR, NULL);
		} else
			return SAM_STAT_GOOD;
	case COMPARE_AND_WRITE:
		iov.iov_base = malloc(half);
		if (!iov.iov_base) {
			tcmu_err("out of memory\n");
			return tcmu_set_sense_data(sense, MEDIUM_ERROR,
						   ASC_READ_ERROR, NULL);
		}
		iov.iov_len = half;
		ret = store->read(dev, &iov, 1, offset);
		if (ret != l) {
			tcmu_err("Error on read %x, %x\n", ret, l);
			return tcmu_set_sense_data(sense, MEDIUM_ERROR,
						   ASC_READ_ERROR, NULL);
		}
		cmp_offset = tcmu_compare_with_iovec(iov.iov_base, iovec, half);
		if (cmp_offset != -1) {
			return tcmu_set_sense_data(sense, MISCOMPARE,
					ASC_MISCOMPARE_DURING_VERIFY_OPERATION,
					&cmp_offset);
		}
		free(iov.iov_base);

		tcmu_seek_in_iovec(iovec, half);
		ret = store->write(dev, iovec, iov_cnt, offset);
		if (ret != half) {
			tcmu_err("Error on write %x, %x\n", ret, half);
			return tcmu_set_sense_data(sense, MEDIUM_ERROR,
						   ASC_READ_ERROR, NULL);
		} else
			return SAM_STAT_GOOD;
	default:
		tcmu_err("unknown command %x\n", cdb[0]);
		return TCMU_NOT_HANDLED;
	}
}

#define CDB_TO_BUF_SIZE(bytes) ((bytes) * 3 + 1)
#define CDB_FIX_BYTES 64 /* 64 bytes for default */
#define CDB_FIX_SIZE CDB_TO_BUF_SIZE(CDB_FIX_BYTES)
static void tcmu_cdb_debug_info(const struct tcmulib_cmd *cmd)
{
	int i, n, bytes;
	char fix[CDB_FIX_SIZE], *buf;
	uint8_t group_code = cmd->cdb[0] >> 5;

	buf = fix;

	switch (group_code) {
	case 0: /*000b for 6 bytes commands */
		bytes = 6;
		break;
	case 1: /*001b for 10 bytes commands */
	case 2: /*010b for 10 bytes commands */
		bytes = 10;
		break;
	case 3: /*011b Reserved ? */
		if (cmd->cdb[0] == 0x7f) {
			bytes = 7 + cmd->cdb[7];
			if (bytes > CDB_FIX_SIZE) {
				buf = malloc(CDB_TO_BUF_SIZE(bytes));
				if (!buf) {
					tcmu_err("out of memory\n");
					return;
				}
			}
		} else {
			bytes = 6;
		}
		break;
	case 4: /*100b for 16 bytes commands */
		bytes = 16;
		break;
	case 5: /*101b for 12 bytes commands */
		bytes = 12;
		break;
	case 6: /*110b Vendor Specific */
	case 7: /*111b Vendor Specific */
	default:
		/* TODO: */
		bytes = 6;
	}

	for (i = 0, n = 0; i < bytes; i++) {
		n += sprintf(buf + n, "%x ", cmd->cdb[i]);
	}
	sprintf(buf + n, "\n");

	tcmu_dbg_cdb(buf);

	if (bytes > CDB_FIX_SIZE)
		free(buf);
}

static void _cleanup_mutex_lock(void *arg)
{
	pthread_mutex_unlock(arg);
}

static void _cleanup_spin_lock(void *arg)
{
	pthread_spin_unlock(arg);
}

static void tcmulib_track_aio_request_start(struct tcmu_device *dev)
{
	struct tcmu_track_aio *aio_track = &dev->track_queue;

	pthread_cleanup_push(_cleanup_spin_lock, (void *)&aio_track->track_lock);
	pthread_spin_lock(&aio_track->track_lock);

	++aio_track->tracked_aio_ops;

	pthread_spin_unlock(&aio_track->track_lock);
	pthread_cleanup_pop(0);
}

static void tcmulib_track_aio_request_finish(struct tcmu_device *dev, int *is_idle)
{
	struct tcmu_track_aio *aio_track = &dev->track_queue;

	pthread_cleanup_push(_cleanup_spin_lock, (void *)&aio_track->track_lock);
	pthread_spin_lock(&aio_track->track_lock);

	assert(aio_track->tracked_aio_ops > 0);

	--aio_track->tracked_aio_ops;
	if (is_idle) {
		*is_idle = (aio_track->tracked_aio_ops == 0) ? 1 : 0;
	}

	pthread_spin_unlock(&aio_track->track_lock);
	pthread_cleanup_pop(0);

}

static void _untrack_in_flight_io(void *arg)
{
	int wakeup;
	tcmulib_track_aio_request_finish(arg, &wakeup);
	if (wakeup) {
		tcmulib_processing_complete(arg);
	}
}

static int invokecmd(struct tcmu_device *dev, struct tcmulib_cmd *cmd)
{
	struct tcmulib_handler *handler = tcmu_get_dev_handler(dev);
	struct tcmur_handler *r_handler = handler->hm_private;

	int (*func)(struct tcmu_device *, struct tcmulib_cmd *) =
		(r_handler->write || r_handler->read || r_handler->flush) ?
		generic_handle_cmd : r_handler->handle_cmd;
	return func(dev, cmd);
}

static void *io_work_queue(void *arg)
{
	struct tcmu_device *dev = arg;
	struct tcmu_io_queue *io_wq = &dev->work_queue;

	while (1) {
		int ret;
		struct tcmu_io_entry *io_entry;

		pthread_cleanup_push(_cleanup_mutex_lock, &io_wq->io_lock);
		pthread_mutex_lock(&io_wq->io_lock);

		while (list_empty(&io_wq->io_queue)) {
			pthread_cond_wait(&io_wq->io_cond,
					  &io_wq->io_lock);
		}

		io_entry = list_first_entry(&io_wq->io_queue,
					    struct tcmu_io_entry, entry);
		list_del(&io_entry->entry);

		pthread_mutex_unlock(&io_wq->io_lock);
		pthread_cleanup_pop(0);

		/* kick start I/O request */
		pthread_cleanup_push(_untrack_in_flight_io, dev);
		tcmulib_track_aio_request_start(dev);

		ret = invokecmd(dev, io_entry->cmd);
		tcmulib_command_complete(dev, io_entry->cmd, ret);

		pthread_cleanup_pop(1); /* untrack aio */
		free(io_entry);
	}

	return NULL;
}

static int aio_schedule(struct tcmu_device *dev, struct tcmulib_cmd *cmd)
{
	struct tcmu_io_entry *io_entry;
	uint8_t *sense = cmd->sense_buf;
	struct tcmu_io_queue *io_wq = &dev->work_queue;

	io_entry = malloc(sizeof(*io_entry));
	if (!io_entry) {
		return errno_to_sam_status(-ENOMEM, sense);
	}

	io_entry->cmd = cmd;
	list_node_init(&io_entry->entry);

	/* cleanup push/pop not _really_ required here atm */
	pthread_cleanup_push(_cleanup_mutex_lock, &io_wq->io_lock);
	pthread_mutex_lock(&io_wq->io_lock);

	list_add_tail(&io_wq->io_queue, &io_entry->entry);
	pthread_cond_signal(&io_wq->io_cond); // TODO: conditional

	pthread_mutex_unlock(&io_wq->io_lock);
	pthread_cleanup_pop(0);

	return TCMU_ASYNC_HANDLED;
}

static int setup_io_work_queue(struct tcmu_device *dev)
{
	int ret;
	struct tcmu_io_queue *io_wq = &dev->work_queue;

	list_head_init(&io_wq->io_queue);

	ret = pthread_mutex_init(&io_wq->io_lock, NULL);
	if (ret < 0) {
		goto out;
	}
	ret = pthread_cond_init(&io_wq->io_cond, NULL);
	if (ret < 0) {
		goto cleanup_lock;
	}

	// TODO: >1 worker threads (per device via config)
	ret = pthread_create(&io_wq->io_wq_thread, NULL, io_work_queue, dev);
	if (ret < 0) {
		goto cleanup_cond;
	}

	return 0;

cleanup_cond:
	pthread_cond_destroy(&io_wq->io_cond);
cleanup_lock:
	pthread_mutex_destroy(&io_wq->io_lock);
out:
	return ret;
}

static void cleanup_io_work_queue(struct tcmu_device *dev,
				  bool cancel)
{
	int ret;
	struct tcmu_io_queue *io_wq = &dev->work_queue;

	if (cancel) {
		cancel_thread(io_wq->io_wq_thread);
	}

	/*
	 * Note that there's no need to drain ->io_queue at this point
	 * as it _should_ be empty (target layer would call this path
	 * when no commands are running - thanks Mike).
	 *
	 * Out of tree handlers which do not use the aio code are not
	 * supported in this path.
	 */

	ret = pthread_mutex_destroy(&io_wq->io_lock);
	if (ret != 0) {
		tcmu_err("failed to destroy io workqueue lock\n");
	}

	ret = pthread_cond_destroy(&io_wq->io_cond);
	if (ret != 0) {
		tcmu_err("failed to destroy io workqueue cond\n");
	}
}

static int setup_aio_tracking(struct tcmu_device *dev)
{
	int ret;
	struct tcmu_track_aio *aio_track = &dev->track_queue;

	aio_track->tracked_aio_ops = 0;
	ret = pthread_spin_init(&aio_track->track_lock, 0);
	if (ret < 0) {
		return ret;
	}

	return 0;
}

static void cleanup_aio_tracking(struct tcmu_device *dev)
{
	int ret;
	struct tcmu_track_aio *aio_track = &dev->track_queue;

	assert(aio_track->tracked_aio_ops == 0);

	ret = pthread_spin_destroy(&aio_track->track_lock);
	if (ret < 0) {
		tcmu_err("failes to destroy track lock\n");
	}
}

static void async_call_command(struct tcmu_device *dev,
			       struct tcmulib_cmd *cmd)
{
	int ret;
	struct tcmulib_handler *handler = tcmu_get_dev_handler(dev);
	struct tcmur_handler *r_handler = handler->hm_private;

	if (r_handler->aio_supported) {
		ret = invokecmd(dev, cmd);
	} else {
		ret = aio_schedule(dev, cmd);
	}

	/*
	 * command (processing) completion is done when one of the
	 * following scenario occurs:
	 *  - synchronous handler:
	 *	only if aio_schedule() returns an error
	 *  - asynchronous handler:
	 *	on an error or if the command was not asynchronously
	 *	handled (see generic_handle_cmd(), non store callouts)
	 */
	if (ret != TCMU_ASYNC_HANDLED) {
		tcmulib_command_complete(dev, cmd, ret);
		tcmulib_processing_complete(dev);
	}
}

static void *tcmu_cmdproc_thread(void *arg)
{
	struct tcmu_device *dev = arg;
	struct pollfd pfd;

	pthread_cleanup_push(cmdproc_thread_cleanup, dev);

	while (1) {
		struct tcmulib_cmd *cmd;

		tcmulib_processing_start(dev);

		while ((cmd = tcmulib_get_next_command(dev)) != NULL) {
			if (tcmu_get_log_level() == TCMU_LOG_DEBUG)
				tcmu_cdb_debug_info(cmd);

			/* call async command */
			async_call_command(dev, cmd);
		}

		pfd.fd = tcmu_get_dev_fd(dev);
		pfd.events = POLLIN;
		pfd.revents = 0;

		poll(&pfd, 1, -1);

		if (pfd.revents != POLLIN) {
			tcmu_err("poll received unexpected revent: 0x%x\n", pfd.revents);
			break;
		}
	}

	tcmu_err("thread terminating, should never happen\n");

	pthread_cleanup_pop(1);

	return NULL;
}

static int add_device(struct tcmulib_context *ctx,
		      char *dev_name, char *cfgstring)
{
	struct tcmu_device *dev;
	struct tcmu_mailbox *mb;
	char str_buf[256];
	int fd;
	int ret;
	char *ptr, *oldptr;
	char *reason = NULL;
	int len;

	dev = calloc(1, sizeof(*dev));
	if (!dev) {
		tcmu_err("calloc failed in add_device\n");
		return -ENOMEM;
	}

	snprintf(dev->dev_name, sizeof(dev->dev_name), "%s", dev_name);

	oldptr = cfgstring;
	ptr = strchr(oldptr, '/');
	if (!ptr) {
		tcmu_err("invalid cfgstring\n");
		goto err_free;
	}

	if (strncmp(cfgstring, "tcm-user", ptr-oldptr)) {
		tcmu_err("invalid cfgstring\n");
		goto err_free;
	}

	/* Get HBA name */
	oldptr = ptr+1;
	ptr = strchr(oldptr, '/');
	if (!ptr) {
		tcmu_err("invalid cfgstring\n");
		goto err_free;
	}
	len = ptr-oldptr;
	snprintf(dev->tcm_hba_name, sizeof(dev->tcm_hba_name), "user_%.*s", len, oldptr);

	/* Get device name */
	oldptr = ptr+1;
	ptr = strchr(oldptr, '/');
	if (!ptr) {
		tcmu_err("invalid cfgstring\n");
		goto err_free;
	}
	len = ptr-oldptr;
	snprintf(dev->tcm_dev_name, sizeof(dev->tcm_dev_name), "%.*s", len, oldptr);

	/* The rest is the handler-specific cfgstring */
	oldptr = ptr+1;
	ptr = strchr(oldptr, '/');
	snprintf(dev->cfgstring, sizeof(dev->cfgstring), "%s", oldptr);

	dev->handler = find_handler(ctx, dev->cfgstring);
	if (!dev->handler) {
		tcmu_err("could not find handler for %s\n", dev->dev_name);
		goto err_free;
	}

	if (dev->handler->check_config &&
	    !dev->handler->check_config(dev->cfgstring, &reason)) {
		/* It may be handled by other handlers */
		tcmu_err("check_config failed for %s because of %s\n", dev->dev_name, reason);
		free(reason);
		goto err_free;
	}

	snprintf(str_buf, sizeof(str_buf), "/dev/%s", dev_name);

	dev->fd = open(str_buf, O_RDWR | O_NONBLOCK | O_CLOEXEC);
	if (dev->fd == -1) {
		tcmu_err("could not open %s\n", str_buf);
		goto err_free;
	}

	snprintf(str_buf, sizeof(str_buf), "/sys/class/uio/%s/maps/map0/size", dev->dev_name);
	fd = open(str_buf, O_RDONLY);
	if (fd == -1) {
		tcmu_err("could not open %s\n", str_buf);
		goto err_fd_close;
	}

	ret = read(fd, str_buf, sizeof(str_buf));
	close(fd);
	if (ret <= 0) {
		tcmu_err("could not read size of map0\n");
		goto err_fd_close;
	}
	str_buf[ret-1] = '\0'; /* null-terminate and chop off the \n */

	dev->map_len = strtoull(str_buf, NULL, 0);
	if (dev->map_len == ULLONG_MAX) {
		tcmu_err("could not get map length\n");
		goto err_fd_close;
	}

	dev->map = mmap(NULL, dev->map_len, PROT_READ|PROT_WRITE, MAP_SHARED, dev->fd, 0);
	if (dev->map == MAP_FAILED) {
		tcmu_err("could not mmap: %m\n");
		goto err_fd_close;
	}

	mb = dev->map;
	if (mb->version != KERN_IFACE_VER) {
		tcmu_err("Kernel interface version mismatch: wanted %d got %d\n",
			KERN_IFACE_VER, mb->version);
		goto err_munmap;
	}

	ret = pthread_spin_init(&dev->lock, 0);
	if (ret < 0) {
		tcmu_err("failed to initialize mailbox lock\n");
		goto err_munmap;
	}

	ret = setup_io_work_queue(dev);
	if (ret < 0) {
		goto cleanup_lock;
	}

	ret = setup_aio_tracking(dev);
	if (ret < 0) {
		goto cleanup_io_work_queue;
	}

	dev->cmd_tail = mb->cmd_tail;

	dev->ctx = ctx;

	darray_append(ctx->devices, dev);

	ret = dev->handler->added(dev);
	if (ret < 0) {
		tcmu_err("handler open failed for %s\n", dev->dev_name);
		goto cleanup_aio_tracking;
	}

	return 0;

cleanup_aio_tracking:
	cleanup_aio_tracking(dev);
cleanup_io_work_queue:
	cleanup_io_work_queue(dev, true);
cleanup_lock:
	pthread_spin_destroy(&dev->lock);
err_munmap:
	munmap(dev->map, dev->map_len);
err_fd_close:
	close(dev->fd);
err_free:
	free(dev);

	return -ENOENT;
}

static void close_devices(struct tcmulib_context *ctx)
{
	struct tcmu_device **dev_ptr;
	struct tcmu_device *dev;
	char *cfgstring = "";

	darray_foreach(dev_ptr, ctx->devices) {
		dev = *dev_ptr;
		remove_device(ctx, dev->dev_name, cfgstring);
	}
}

static void remove_device(struct tcmulib_context *ctx,
			  char *dev_name, char *cfgstring)
{
	struct tcmu_device **dev_ptr;
	struct tcmu_device *dev;
	int i = 0, ret;
	bool found = false;
	struct tcmu_io_queue *io_wq;

	darray_foreach(dev_ptr, ctx->devices) {
		dev = *dev_ptr;
		size_t len = strnlen(dev->dev_name, sizeof(dev->dev_name));
		if (strncmp(dev->dev_name, dev_name, len)) {
			i++;
		} else {
			found = true;
			break;
		}
	}

	if (!found) {
		tcmu_err("could not remove device %s: not found\n", dev_name);
		return;
	}

	darray_remove(ctx->devices, i);

	io_wq = &dev->work_queue;

	/*
	 * The order of cleaning up worker threads and calling ->removed()
	 * is important: for sync handlers, the worker thread needs to be
	 * terminated before removing the handler (i.e., calling handlers
	 * ->close() callout) in order to ensure that no store callouts
	 * are getting invoked when shutting down the handler.
	 */
	cancel_thread(io_wq->io_wq_thread);
	dev->handler->removed(dev);
	cleanup_io_work_queue(dev, false);
	cleanup_aio_tracking(dev);

	ret = close(dev->fd);
	if (ret != 0) {
		tcmu_err("could not close device fd %s: %d\n", dev_name, errno);
	}
	ret = munmap(dev->map, dev->map_len);
	if (ret != 0) {
		tcmu_err("could not unmap device %s: %d\n", dev_name, errno);
	}

	ret = pthread_spin_destroy(&dev->lock);
	if (ret < 0) {
		tcmu_err("could not cleanup mailbox lock %s: %d\n", dev_name, errno);
	}
}

static int is_uio(const struct dirent *dirent)
{
	int fd;
	char tmp_path[64];
	char buf[256];
	ssize_t ret;

	if (strncmp(dirent->d_name, "uio", 3))
		return 0;

	snprintf(tmp_path, sizeof(tmp_path), "/sys/class/uio/%s/name", dirent->d_name);

	fd = open(tmp_path, O_RDONLY);
	if (fd == -1)
		return 0;

	ret = read(fd, buf, sizeof(buf));
	if (ret <= 0 || ret >= sizeof(buf))
		goto not_uio;

	buf[ret-1] = '\0'; /* null-terminate and chop off the \n */

	/* we only want uio devices whose name is a format we expect */
	if (strncmp(buf, "tcm-user", 8))
		goto not_uio;

	close(fd);
	return 1;
not_uio:
	close(fd);
	return 0;
}

static int open_devices(struct tcmulib_context *ctx)
{
	struct dirent **dirent_list;
	int num_devs;
	int num_good_devs = 0;
	int i;

	num_devs = scandir("/dev", &dirent_list, is_uio, alphasort);

	if (num_devs == -1)
		return -1;

	for (i = 0; i < num_devs; i++) {
		char tmp_path[64];
		char buf[256];
		int fd;
		int ret;

		snprintf(tmp_path, sizeof(tmp_path), "/sys/class/uio/%s/name",
			 dirent_list[i]->d_name);

		fd = open(tmp_path, O_RDONLY);
		if (fd == -1) {
			tcmu_err("could not open %s!\n", tmp_path);
			continue;
		}

		ret = read(fd, buf, sizeof(buf));
		close(fd);
		if (ret <= 0 || ret >= sizeof(buf)) {
			tcmu_err("read of %s had issues\n", tmp_path);
			continue;
		}
		buf[ret-1] = '\0'; /* null-terminate and chop off the \n */

		ret = add_device(ctx, dirent_list[i]->d_name, buf);
		if (ret < 0)
			continue;

		num_good_devs++;
	}

	for (i = 0; i < num_devs; i++)
		free(dirent_list[i]);
	free(dirent_list);

	return num_good_devs;
}

struct tcmulib_context *tcmulib_initialize(
	struct tcmulib_handler *handlers,
	size_t handler_count)
{
	struct tcmulib_context *ctx;
	int ret;
	int i;

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx)
		return NULL;

	ctx->nl_sock = setup_netlink(ctx);
	if (!ctx->nl_sock) {
		free(ctx);
		return NULL;
	}

	darray_init(ctx->handlers);
	darray_init(ctx->devices);

	for (i = 0; i < handler_count; i++) {
		struct tcmulib_handler handler = handlers[i];
		handler.ctx = ctx;
		darray_append(ctx->handlers, handler);
	}

	ret = open_devices(ctx);
	if (ret < 0) {
		teardown_netlink(ctx->nl_sock);
		darray_free(ctx->handlers);
		darray_free(ctx->devices);
		return NULL;
	}

	return ctx;
}

void tcmulib_close(struct tcmulib_context *ctx)
{
	int ret;
	close_devices(ctx);
	teardown_netlink(ctx->nl_sock);
	darray_free(ctx->handlers);
	darray_free(ctx->devices);
	ret = genl_unregister_family(&tcmu_ops);
	if (ret != 0) {
		tcmu_err("genl_unregister_family failed, %d\n", ret);
	}
	free(ctx);
}

int tcmulib_get_master_fd(struct tcmulib_context *ctx)
{
	return nl_socket_get_fd(ctx->nl_sock);
}

int tcmulib_master_fd_ready(struct tcmulib_context *ctx)
{
	return nl_recvmsgs_default(ctx->nl_sock);
}

void *tcmu_get_dev_private(struct tcmu_device *dev)
{
	return dev->hm_private;
}

void tcmu_set_dev_private(struct tcmu_device *dev, void *private)
{
	dev->hm_private = private;
}

void tcmu_set_dev_num_lbas(struct tcmu_device *dev, uint64_t num_lbas)
{
	dev->num_lbas = num_lbas;
}

uint64_t tcmu_get_dev_num_lbas(struct tcmu_device *dev)
{
	return dev->num_lbas;
}

void tcmu_set_dev_block_size(struct tcmu_device *dev, uint32_t block_size)
{
	dev->block_size = block_size;
}

uint32_t tcmu_get_dev_block_size(struct tcmu_device *dev)
{
	return dev->block_size;
}

int tcmu_get_dev_fd(struct tcmu_device *dev)
{
	return dev->fd;
}

char *tcmu_get_dev_cfgstring(struct tcmu_device *dev)
{
	return dev->cfgstring;
}

struct tcmulib_handler *tcmu_get_dev_handler(struct tcmu_device *dev)
{
	return dev->handler;
}

static inline struct tcmu_cmd_entry *
device_cmd_head(struct tcmu_device *dev)
{
	struct tcmu_mailbox *mb = dev->map;

	return (struct tcmu_cmd_entry *) ((char *) mb + mb->cmdr_off + mb->cmd_head);
}

static inline struct tcmu_cmd_entry *
device_cmd_tail(struct tcmu_device *dev)
{
	struct tcmu_mailbox *mb = dev->map;

	return (struct tcmu_cmd_entry *) ((char *) mb + mb->cmdr_off + dev->cmd_tail);
}

/* update the tcmu_device's tail */
#define TCMU_UPDATE_DEV_TAIL(dev, mb, ent) \
do { \
	dev->cmd_tail = (dev->cmd_tail + tcmu_hdr_get_len((ent)->hdr.len_op)) % mb->cmdr_size; \
} while (0);

struct tcmulib_cmd *tcmulib_get_next_command(struct tcmu_device *dev)
{
	struct tcmu_mailbox *mb = dev->map;
	struct tcmu_cmd_entry *ent;

	while ((ent = device_cmd_tail(dev)) != device_cmd_head(dev)) {

		switch (tcmu_hdr_get_op(ent->hdr.len_op)) {
		case TCMU_OP_PAD:
			/* do nothing */
			break;
		case TCMU_OP_CMD: {
			int i;
			struct tcmulib_cmd *cmd;
			uint8_t *cdb = (uint8_t *) mb + ent->req.cdb_off;
			unsigned cdb_len = tcmu_get_cdb_length(cdb);

			/* Alloc memory for cmd itself, iovec and cdb */
			cmd = malloc(sizeof(*cmd) + sizeof(*cmd->iovec) * ent->req.iov_cnt + cdb_len);
			if (!cmd)
				return NULL;
			cmd->cmd_id = ent->hdr.cmd_id;

			/* Convert iovec addrs in-place to not be offsets */
			cmd->iov_cnt = ent->req.iov_cnt;
			cmd->iovec = (struct iovec *) (cmd + 1);
			for (i = 0; i < ent->req.iov_cnt; i++) {
				cmd->iovec[i].iov_base = (void *) mb +
					(size_t) ent->req.iov[i].iov_base;
				cmd->iovec[i].iov_len = ent->req.iov[i].iov_len;
			}

			/* Copy cdb that currently points to the command ring */
			cmd->cdb = (uint8_t *) (cmd->iovec + cmd->iov_cnt);
			memcpy(cmd->cdb, (void *) mb + ent->req.cdb_off, cdb_len);

			TCMU_UPDATE_DEV_TAIL(dev, mb, ent);
			return cmd;
		}
		default:
			/* We don't even know how to handle this TCMU opcode. */
			ent->hdr.uflags |= TCMU_UFLAG_UNKNOWN_OP;
		}

		TCMU_UPDATE_DEV_TAIL(dev, mb, ent);
	}

	return NULL;
}

/* update the ring buffer's tail */
#define TCMU_UPDATE_RB_TAIL(mb, ent) \
do { \
	mb->cmd_tail = (mb->cmd_tail + tcmu_hdr_get_len((ent)->hdr.len_op)) % mb->cmdr_size; \
} while (0);

void tcmulib_command_complete(
	struct tcmu_device *dev,
	struct tcmulib_cmd *cmd,
	int result)
{
	struct tcmu_mailbox *mb = dev->map;

	pthread_cleanup_push(_cleanup_spin_lock, (void *)&dev->lock);
	pthread_spin_lock(&dev->lock);
	struct tcmu_cmd_entry *ent = (void *) mb + mb->cmdr_off + mb->cmd_tail;

	/* current command could be PAD in async case */
	while (ent != (void *) mb + mb->cmdr_off + mb->cmd_head) {
		if (tcmu_hdr_get_op(ent->hdr.len_op) == TCMU_OP_CMD)
			break;
		TCMU_UPDATE_RB_TAIL(mb, ent);
		ent = (void *) mb + mb->cmdr_off + mb->cmd_tail;
	}

	/* cmd_id could be different in async case */
	if (cmd->cmd_id != ent->hdr.cmd_id) {
		ent->hdr.cmd_id = cmd->cmd_id;
	}

	if (result == TCMU_NOT_HANDLED) {
		/* Tell the kernel we didn't handle it */
		char *buf = ent->rsp.sense_buffer;

		ent->rsp.scsi_status = SAM_STAT_CHECK_CONDITION;

		buf[0] = 0x70;	/* fixed, current */
		buf[2] = 0x5;	/* illegal request */
		buf[7] = 0xa;
		buf[12] = 0x20; /* ASC: invalid command operation code */
		buf[13] = 0x0;	/* ASCQ: (none) */
	} else {
		if (result != SAM_STAT_GOOD) {
			memcpy(ent->rsp.sense_buffer, cmd->sense_buf,
			       TCMU_SENSE_BUFFERSIZE);
		}
		ent->rsp.scsi_status = result;
	}

	TCMU_UPDATE_RB_TAIL(mb, ent);
	pthread_spin_unlock(&dev->lock);
	pthread_cleanup_pop(0);

	free(cmd);
}

void tcmulib_processing_start(struct tcmu_device *dev)
{
	int r;
	uint32_t buf;

	/* Clear the event on the fd */
	do {
		r = read(dev->fd, &buf, 4);
	} while (r == -1 && errno == EINTR);
	if (r == -1 && errno != EAGAIN)
		perror("read");
}

void tcmulib_processing_complete(struct tcmu_device *dev)
{
	int r;
	uint32_t buf = 0;

	/* Tell the kernel there are completed commands */
	do {
		r = write(dev->fd, &buf, 4);
	} while (r == -1 && errno == EINTR);
	if (r == -1 && errno != EAGAIN)
		perror("write");
}

int tcmulib_start_cmdproc_thread(struct tcmu_device *dev)
{
	int ret;
	struct tcmu_thread thread;

	thread.dev = dev;

	ret = pthread_create(&thread.thread_id, NULL, tcmu_cmdproc_thread, dev);
	if (ret) {
		return -1;
	}

	darray_append(g_threads, thread);
	return 0;
}

void tcmulib_cleanup_cmdproc_thread(struct tcmu_device *dev)
{
	struct tcmu_thread *thread;
	int i = 0;
	bool found = false;

	darray_foreach(thread, g_threads) {
		if (thread->dev == dev) {
			found = true;
			break;
		} else {
			i++;
		}
	}

	if (!found) {
		tcmu_err("could not remove a device: not found\n");
		return;
	}

	cancel_thread(thread->thread_id);
	darray_remove(g_threads, i);
}

void tcmulib_cleanup_all_cmdproc_threads()
{
	struct tcmu_thread *thread;
	darray_foreach(thread, g_threads) {
		cancel_thread(thread->thread_id);
	}
}
