/*
 * Copyright 2014, Red Hat, Inc.
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

/*
 * A daemon that supports a simplified interface for writing TCMU
 * handlers.
 */

#define _GNU_SOURCE
#define _BITS_UIO_H
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <assert.h>
#include <dlfcn.h>
#include <pthread.h>
#include <signal.h>
#include <glib.h>
#include <gio/gio.h>
#include <getopt.h>
#include <poll.h>
#include <scsi/scsi.h>

#include <libkmod.h>
#include <linux/target_core_user.h>
#include "darray.h"
#include "tcmu-runner.h"
#include "libtcmu.h"
#include "tcmuhandler-generated.h"
#include "version.h"
#include "libtcmu_config.h"
#include "libtcmu_log.h"

#define ARRAY_SIZE(X) (sizeof(X) / sizeof((X)[0]))

static char *handler_path = DEFAULT_HANDLER_PATH;

darray(struct tcmur_handler *) g_runner_handlers = darray_new();

static struct tcmur_handler *find_handler_by_subtype(gchar *subtype)
{
	struct tcmur_handler **handler;

	darray_foreach(handler, g_runner_handlers) {
		if (strcmp((*handler)->subtype, subtype) == 0)
			return *handler;
	}
	return NULL;
}

int tcmur_register_handler(struct tcmur_handler *handler)
{
	darray_append(g_runner_handlers, handler);
	return 0;
}

bool tcmur_unregister_handler(struct tcmur_handler *handler)
{
	int i;
	for (i = 0; i < darray_size(g_runner_handlers); i++) {
		if (darray_item(g_runner_handlers, i) == handler) {
			darray_remove(g_runner_handlers, i);
			return true;
		}
	}
	return false;
}

static int is_handler(const struct dirent *dirent)
{
	if (strncmp(dirent->d_name, "handler_", 8))
		return 0;

	return 1;
}

static int open_handlers(void)
{
	struct dirent **dirent_list;
	int num_handlers;
	int num_good = 0;
	int i;

	num_handlers = scandir(handler_path, &dirent_list, is_handler, alphasort);

	if (num_handlers == -1)
		return -1;

	for (i = 0; i < num_handlers; i++) {
		char *path;
		void *handle;
		int (*handler_init)(void);
		int ret;

		ret = asprintf(&path, "%s/%s", handler_path, dirent_list[i]->d_name);
		if (ret == -1) {
			tcmu_err("ENOMEM\n");
			continue;
		}

		handle = dlopen(path, RTLD_NOW|RTLD_LOCAL);
		if (!handle) {
			tcmu_err("Could not open handler at %s: %s\n", path, dlerror());
			free(path);
			continue;
		}

		handler_init = dlsym(handle, "handler_init");
		if (!handler_init) {
			tcmu_err("dlsym failure on %s\n", path);
			free(path);
			continue;
		}

		ret = handler_init();

		free(path);

		if (ret == 0)
			num_good++;
	}

	for (i = 0; i < num_handlers; i++)
		free(dirent_list[i]);
	free(dirent_list);

	return num_good;
}

static void sighandler(int signal)
{
	tcmulib_cleanup_all_cmdproc_threads();
	tcmu_cancel_log_thread();
	exit(1);
}

static struct sigaction tcmu_sigaction = {
	.sa_handler = sighandler,
};

gboolean tcmulib_callback(GIOChannel *source,
			  GIOCondition condition,
			  gpointer data)
{
	struct tcmulib_context *ctx = data;

	tcmulib_master_fd_ready(ctx);

	return TRUE;
}

static GDBusObjectManagerServer *manager = NULL;

static gboolean
on_check_config(TCMUService1 *interface,
		GDBusMethodInvocation *invocation,
		gchar *cfgstring,
		gpointer user_data)
{
	struct tcmur_handler *handler = user_data;
	char *reason = NULL;
	bool str_ok = true;

	if (handler->check_config)
		str_ok = handler->check_config(cfgstring, &reason);

	if (str_ok)
		reason = "success";

	g_dbus_method_invocation_return_value(invocation,
		    g_variant_new("(bs)", str_ok, reason ? : "unknown"));

	if (!str_ok)
		free(reason);

	return TRUE;
}

static void
dbus_export_handler(struct tcmur_handler *handler, GCallback check_config)
{
	GDBusObjectSkeleton *object;
	char obj_name[128];
	TCMUService1 *interface;

	snprintf(obj_name, sizeof(obj_name), "/org/kernel/TCMUService1/%s",
		 handler->subtype);
	object = g_dbus_object_skeleton_new(obj_name);
	interface = tcmuservice1_skeleton_new();
	g_dbus_object_skeleton_add_interface(object, G_DBUS_INTERFACE_SKELETON(interface));
	g_signal_connect(interface,
			 "handle-check-config",
			 check_config,
			 handler); /* user_data */
	tcmuservice1_set_config_desc(interface, handler->cfg_desc);
	g_dbus_object_manager_server_export(manager, G_DBUS_OBJECT_SKELETON(object));
	g_object_unref(object);
}

static bool
dbus_unexport_handler(struct tcmur_handler *handler)
{
	char obj_name[128];

	snprintf(obj_name, sizeof(obj_name), "/org/kernel/TCMUService1/%s",
		 handler->subtype);
	return g_dbus_object_manager_server_unexport(manager, obj_name) == TRUE;
}

struct dbus_info {
	guint watcher_id;
	/* The RegisterHandler invocation on
	 * org.kernel.TCMUService1.HandlerManager1 interface. */
	GDBusMethodInvocation *register_invocation;
	/* Connection to the handler's bus_name. */
	GDBusConnection *connection;
};

static int dbus_handler_open(struct tcmu_device *dev)
{
	return -1;
}

static void dbus_handler_close(struct tcmu_device *dev)
{
	/* nop */
}

static int dbus_handler_handle_cmd(struct tcmu_device *dev,
				   struct tcmulib_cmd *cmd)
{
	abort();
}

static gboolean
on_dbus_check_config(TCMUService1 *interface,
		     GDBusMethodInvocation *invocation,
		     gchar *cfgstring,
		     gpointer user_data)
{
	char *bus_name, *obj_name;
	struct tcmur_handler *handler = user_data;
	GDBusConnection *connection;
	GError *error = NULL;
	GVariant *result;

	bus_name = g_strdup_printf("org.kernel.TCMUService1.HandlerManager1.%s",
				   handler->subtype);
	obj_name = g_strdup_printf("/org/kernel/TCMUService1/HandlerManager1/%s",
				   handler->subtype);
	connection = g_dbus_method_invocation_get_connection(invocation);
	result = g_dbus_connection_call_sync(connection,
					     bus_name,
					     obj_name,
					     "org.kernel.TCMUService1",
					     "CheckConfig",
					     g_variant_new("(s)", cfgstring),
					     NULL, G_DBUS_CALL_FLAGS_NONE, -1,
					     NULL, &error);
	if (result)
		g_dbus_method_invocation_return_value(invocation, result);
	else
		g_dbus_method_invocation_return_value(invocation,
			g_variant_new("(bs)", FALSE, error->message));
	g_free(bus_name);
	g_free(obj_name);
	return TRUE;
}

static void
on_handler_appeared(GDBusConnection *connection,
		    const gchar     *name,
		    const gchar     *name_owner,
		    gpointer         user_data)
{
	struct tcmur_handler *handler = user_data;
	struct dbus_info *info = handler->opaque;

	if (info->register_invocation) {
		info->connection = connection;
		tcmur_register_handler(handler);
		dbus_export_handler(handler, G_CALLBACK(on_dbus_check_config));
		g_dbus_method_invocation_return_value(info->register_invocation,
			    g_variant_new("(bs)", TRUE, "succeeded"));
		info->register_invocation = NULL;
	}
}

static void
on_handler_vanished(GDBusConnection *connection,
		    const gchar     *name,
		    gpointer         user_data)
{
	struct tcmur_handler *handler = user_data;
	struct dbus_info *info = handler->opaque;

	if (info->register_invocation) {
		char *reason;
		reason = g_strdup_printf("Cannot find handler bus name: "
				"org.kernel.TCMUService1.HandlerManager1.%s",
				handler->subtype);
		g_dbus_method_invocation_return_value(info->register_invocation,
			    g_variant_new("(bs)", FALSE, reason));
		g_free(reason);
	}
	tcmur_unregister_handler(handler);
	dbus_unexport_handler(handler);
}

static gboolean
on_register_handler(TCMUService1HandlerManager1 *interface,
		    GDBusMethodInvocation *invocation,
		    gchar *subtype,
		    gchar *cfg_desc,
		    gpointer user_data)
{
	struct tcmur_handler *handler;
	struct dbus_info *info;
	char *bus_name;

	bus_name = g_strdup_printf("org.kernel.TCMUService1.HandlerManager1.%s",
				   subtype);

	handler               = g_new0(struct tcmur_handler, 1);
	handler->subtype      = g_strdup(subtype);
	handler->cfg_desc     = g_strdup(cfg_desc);
	handler->open         = dbus_handler_open;
	handler->close        = dbus_handler_close;
	handler->handle_cmd   = dbus_handler_handle_cmd;

	info = g_new0(struct dbus_info, 1);
	info->register_invocation = invocation;
	info->watcher_id = g_bus_watch_name(G_BUS_TYPE_SYSTEM,
					    bus_name,
					    G_BUS_NAME_WATCHER_FLAGS_NONE,
					    on_handler_appeared,
					    on_handler_vanished,
					    handler,
					    NULL);
	g_free(bus_name);
	handler->opaque = info;
	return TRUE;
}

static gboolean
on_unregister_handler(TCMUService1HandlerManager1 *interface,
		      GDBusMethodInvocation *invocation,
		      gchar *subtype,
		      gpointer user_data)
{
	struct tcmur_handler *handler = find_handler_by_subtype(subtype);
	struct dbus_info *info = handler->opaque;

	if (!handler) {
		g_dbus_method_invocation_return_value(invocation,
			g_variant_new("(bs)", FALSE,
				      "unknown subtype"));
		return TRUE;
	}
	dbus_unexport_handler(handler);
	tcmur_unregister_handler(handler);
	g_bus_unwatch_name(info->watcher_id);
	g_free(info);
	g_free(handler);
	g_dbus_method_invocation_return_value(invocation,
		g_variant_new("(bs)", TRUE, "succeeded"));
	return TRUE;
}

void dbus_handler_manager1_init(GDBusConnection *connection)
{
	GError *error = NULL;
	TCMUService1HandlerManager1 *interface;
	gboolean ret;

	interface = tcmuservice1_handler_manager1_skeleton_new();
	ret = g_dbus_interface_skeleton_export(
			G_DBUS_INTERFACE_SKELETON(interface),
			connection,
			"/org/kernel/TCMUService1/HandlerManager1",
			&error);
	g_signal_connect(interface,
			 "handle-register-handler",
			 G_CALLBACK (on_register_handler),
			 NULL);
	g_signal_connect(interface,
			 "handle-unregister-handler",
			 G_CALLBACK (on_unregister_handler),
			 NULL);
	if (!ret)
		tcmu_err("Handler manager export failed: %s\n",
		     error ? error->message : "unknown error");
	if (error)
		g_error_free(error);
}

static void dbus_bus_acquired(GDBusConnection *connection,
			      const gchar *name,
			      gpointer user_data)
{
	struct tcmur_handler **handler;

	tcmu_dbg("bus %s acquired\n", name);

	manager = g_dbus_object_manager_server_new("/org/kernel/TCMUService1");

	darray_foreach(handler, g_runner_handlers) {
		dbus_export_handler(*handler, G_CALLBACK(on_check_config));
	}

	dbus_handler_manager1_init(connection);
	g_dbus_object_manager_server_set_connection(manager, connection);
}

static void dbus_name_acquired(GDBusConnection *connection,
			      const gchar *name,
			      gpointer user_data)
{
	tcmu_dbg("name %s acquired\n", name);
}

static void dbus_name_lost(GDBusConnection *connection,
			   const gchar *name,
			   gpointer user_data)
{
	tcmu_dbg("name lost\n");
}

int load_our_module(void) {
	struct kmod_list *list = NULL, *itr;
	int err;
	struct kmod_ctx *ctx;

	ctx = kmod_new(NULL, NULL);
	if (!ctx) {
		tcmu_err("kmod_new() failed\n");
		return -1;
	}

	err = kmod_module_new_from_lookup(ctx, "target_core_user", &list);
	if (err < 0)
		return err;

	kmod_list_foreach(itr, list) {
		struct kmod_module *mod = kmod_module_get_module(itr);

		err = kmod_module_probe_insert_module (
			mod, KMOD_PROBE_APPLY_BLACKLIST, 0, 0, 0, 0);

		if (err != 0) {
			tcmu_err("kmod_module_probe_insert_module() for %s failed\n",
			    kmod_module_get_name(mod));
			return -1;
		}

		tcmu_dbg("Module %s inserted (or already loaded)\n", kmod_module_get_name(mod));

		kmod_module_unref(mod);
	}

	return 0;
}

static int dev_added(struct tcmu_device *dev)
{
	struct tcmur_handler *r_handler = tcmu_get_runner_handler(dev);
	int ret;

	ret = r_handler->open(dev);
	if (ret)
		return ret;

	ret = tcmulib_start_cmdproc_thread(dev);
	if (ret < 0) {
		r_handler->close(dev);
		return ret;
	}

	return 0;
}

static void dev_removed(struct tcmu_device *dev)
{
	tcmulib_cleanup_cmdproc_thread(dev);
}

static void usage(void) {
	printf("\nusage:\n");
	printf("\ttcmu-runner [options]\n");
	printf("\noptions:\n");
	printf("\t-h, --help: print this message and exit\n");
	printf("\t-V, --version: print version and exit\n");
	printf("\t-d, --debug: enable debug messages\n");
	printf("\t--handler-path: set path to search for handler modules\n");
	printf("\t\tdefault is %s\n", DEFAULT_HANDLER_PATH);
	printf("\n");
}

static struct option long_options[] = {
	{"debug", no_argument, 0, 'd'},
	{"handler-path", required_argument, 0, 0},
	{"help", no_argument, 0, 'h'},
	{"version", no_argument, 0, 'V'},
	{0, 0, 0, 0},
};

int main(int argc, char **argv)
{
	int ret;
	GMainLoop *loop;
	GIOChannel *libtcmu_gio;
	guint reg_id;
	int c;
	struct tcmulib_context *tcmulib_context;
	darray(struct tcmulib_handler) handlers = darray_new();
	struct tcmur_handler **tmp_r_handler;
	struct tcmu_config *cfg;

	cfg = tcmu_config_new();
	tcmu_load_config(cfg, NULL);
	tcmu_set_log_level(cfg->log_level);

	while (1) {
		int option_index = 0;

		c = getopt_long(argc, argv, "dhV",
				long_options, &option_index);
		if (c == -1)
			break;

		switch (c) {
		case 0:
			if (option_index == 1)
				handler_path = strdup(optarg);
			break;
		case 'd':
			tcmu_set_log_level(TCMU_CONF_LOG_DEBUG_SCSI_CMD);
			break;
		case 'V':
			printf("tcmu-runner %s\n", TCMUR_VERSION);
			exit(1);
		default:
		case 'h':
			usage();
			exit(1);
		}
	}

	tcmu_dbg("handler path: %s\n", handler_path);

	ret = load_our_module();
	if (ret < 0) {
		tcmu_err("couldn't load module\n");
		exit(1);
	}

	ret = open_handlers();
	if (ret < 0) {
		tcmu_err("couldn't open handlers\n");
		exit(1);
	}
	tcmu_dbg("%d runner handlers found\n", ret);

	/*
	 * Convert from tcmu-runner's handler struct to libtcmu's
	 * handler struct, an array of which we pass in, below.
	 */
	darray_foreach(tmp_r_handler, g_runner_handlers) {
		struct tcmulib_handler tmp_handler;

		tmp_handler.name = (*tmp_r_handler)->name;
		tmp_handler.subtype = (*tmp_r_handler)->subtype;
		tmp_handler.cfg_desc = (*tmp_r_handler)->cfg_desc;
		tmp_handler.check_config = (*tmp_r_handler)->check_config;
		tmp_handler.added = dev_added;
		tmp_handler.removed = dev_removed;

		/*
		 * Can hand out a ref to an internal pointer to the
		 * darray b/c handlers will never be added or removed
		 * once open_handlers() is done.
		 */
		tmp_handler.hm_private = *tmp_r_handler;

		darray_append(handlers, tmp_handler);
	}

	tcmulib_context = tcmulib_initialize(handlers.item, handlers.size);
	if (!tcmulib_context) {
		tcmu_err("tcmulib_initialize failed\n");
		exit(1);
	}

	ret = sigaction(SIGINT, &tcmu_sigaction, NULL);
	if (ret) {
		tcmu_err("couldn't set sigaction\n");
		exit(1);
	}

	/* Set up event for libtcmu */
	libtcmu_gio = g_io_channel_unix_new(tcmulib_get_master_fd(tcmulib_context));
	g_io_add_watch(libtcmu_gio, G_IO_IN, tcmulib_callback, tcmulib_context);

	/* Set up DBus name, see callback */
	reg_id = g_bus_own_name(G_BUS_TYPE_SYSTEM,
				"org.kernel.TCMUService1",
				G_BUS_NAME_OWNER_FLAGS_NONE,
				dbus_bus_acquired,
				dbus_name_acquired, // name acquired
				dbus_name_lost, // name lost
				NULL, // user data
				NULL  // user date free func
		);

	loop = g_main_loop_new(NULL, FALSE);
	g_main_loop_run(loop);

	tcmu_dbg("Exiting...\n");
	g_bus_unown_name(reg_id);
	g_main_loop_unref(loop);
	tcmu_config_destroy(cfg);

	return 0;
}
