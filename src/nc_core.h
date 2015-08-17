/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _NC_CORE_H_
#define _NC_CORE_H_

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#ifdef HAVE_DEBUG_LOG
# define NC_DEBUG_LOG 1
#endif

#ifdef HAVE_ASSERT_PANIC
# define NC_ASSERT_PANIC 1
#endif

#ifdef HAVE_ASSERT_LOG
# define NC_ASSERT_LOG 1
#endif

#ifdef HAVE_STATS
# define NC_STATS 1
#else
# define NC_STATS 0
#endif

#ifdef HAVE_EPOLL
# define NC_HAVE_EPOLL 1
#elif HAVE_KQUEUE
# define NC_HAVE_KQUEUE 1
#elif HAVE_EVENT_PORTS
# define NC_HAVE_EVENT_PORTS 1
#else
# error missing scalable I/O event notification mechanism
#endif

#ifdef HAVE_LITTLE_ENDIAN
# define NC_LITTLE_ENDIAN 1
#endif

#ifdef HAVE_BACKTRACE
# define NC_HAVE_BACKTRACE 1
#endif

#if 1 //shenzheng 2015-6-6 zookeeper
#ifdef HAVE_ZOOKEEPER
# define NC_ZOOKEEPER 1
#endif
#endif //shenzheng 2015-6-6 zookeeper

#define NC_OK        0
#define NC_ERROR    -1
#define NC_EAGAIN   -2
#define NC_ENOMEM   -3

#if 1 //shenzheng 2014-12-4 common
/**
the macro defined below must be a negative number. 
*/
#define ERROR_REPLACE_SERVER_TRY_AGAIN -1
#define ERROR_REPLACE_SERVER_CONF_VERSION_CHANGE -2
#endif //shenzheng 2014-12-4 common

/* reserved fds for std streams, log, stats fd, epoll etc. */
#define RESERVED_FDS 32

typedef int rstatus_t; /* return type */
typedef int err_t;     /* error type */

struct array;
struct string;
struct context;
struct conn;
struct conn_tqh;
struct msg;
struct msg_tqh;
struct server;
struct server_pool;
struct mbuf;
struct mhdr;
struct conf;
struct stats;
struct instance;
struct event_base;

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <netinet/in.h>

#include <nc_array.h>
#include <nc_string.h>
#include <nc_queue.h>
#include <nc_rbtree.h>
#include <nc_log.h>
#include <nc_util.h>
#include <event/nc_event.h>
#include <nc_stats.h>
#include <nc_mbuf.h>
#include <nc_message.h>
#include <nc_connection.h>
#include <nc_server.h>

#if 1 //shenzheng 2015-4-28 proxy administer
#include <nc_proxy.h>
#endif //shenzheng 2015-4-28 proxy administer

#if 1 //shenzheng 2015-6-8 zookeeper
#ifdef NC_ZOOKEEPER
#include <nc_zookeeper.h>
#endif
#endif //shenzheng 2015-6-8 zookeeper

struct context {
    uint32_t           id;          /* unique context id */
    struct conf        *cf;         /* configuration */
    struct stats       *stats;      /* stats */

    struct array       pool;        /* server_pool[] */
    struct event_base  *evb;        /* event base */
    int                max_timeout; /* max timeout in msec */
    int                timeout;     /* timeout in msec */

    uint32_t           max_nfd;     /* max # files */
    uint32_t           max_ncconn;  /* max # client connections */
    uint32_t           max_nsconn;  /* max # server connections */

#if 1 //shenzheng 2015-4-27 proxy administer
	struct proxy_adm   *padm;
#endif //shenzheng 2015-4-27 proxy administer

#if 1 //shenzheng 2015-5-8 config-reload
	uint8_t			   which_pool:1;	/* 0:ctx->pool ; 1:ctx->pool_swap */
	struct conf        *cf_swap;    	/* server_pool[] */
	struct array       pool_swap;    	/* server_pool[] */
	volatile uint8_t   reload_thread:1; /* 0: proxy_adm thread's right to handle reload; 
										  * 1: mian thread's right to handle reload */
	volatile long long conf_version;										  	
	pthread_mutex_t    reload_lock;
#endif //shenzheng 2015-7-27 config-reload

#if 1 //shenzheng 2015-6-9 zookeeper
#ifdef NC_ZOOKEEPER
	void		   	   *zkhandle;
	struct string	   watch_path;
	struct string	   zk_servers;
#endif
#endif //shenzheng 2015-6-16 zookeeper
};


struct instance {
    struct context  *ctx;                        /* active context */
    int             log_level;                   /* log level */
    char            *log_filename;               /* log filename */
    char            *conf_filename;              /* configuration filename */
    uint16_t        stats_port;                  /* stats monitoring port */
    int             stats_interval;              /* stats aggregation interval */
    char            *stats_addr;                 /* stats monitoring addr */
    char            hostname[NC_MAXHOSTNAMELEN]; /* hostname */
    size_t          mbuf_chunk_size;             /* mbuf chunk size */
    pid_t           pid;                         /* process id */
    char            *pid_filename;               /* pid filename */
    unsigned        pidfile:1;                   /* pid file created? */
#if 1 //shenzheng 2015-4-28 proxy administer
	char            *proxy_adm_addr;             /* proxy administer monitoring addr */
	uint16_t        proxy_adm_port;              /* proxy administer monitoring port */
#endif //shenzheng 2015-4-28 proxy administer

#if 1 //shenzheng 2015-6-9 zookeeper
#ifdef NC_ZOOKEEPER
	uint8_t			zk_start:1;					  /* start from configuration in zookeeper */
	uint8_t			zk_keep:1;					  /* keep configuration from zookeeper */
	char			*zk_servers;				  /* zoopeeper servers' address, like 192.168.0.1:2181,192.168.0.2:2181 */
	char			*zk_path;					  /* configuration path in zookeeper */
#endif
#endif //shenzheng 2015-6-9 zookeeper
};

struct context *core_start(struct instance *nci);
void core_stop(struct context *ctx);
rstatus_t core_core(void *arg, uint32_t events);
rstatus_t core_loop(struct context *ctx);

#endif
