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

#include <stdlib.h>
#include <unistd.h>
#include <nc_core.h>
#include <nc_conf.h>
#include <nc_server.h>
#include <nc_proxy.h>

static uint32_t ctx_id; /* context generation */

static rstatus_t
core_calc_connections(struct context *ctx)
{
    int status;
    struct rlimit limit;

    status = getrlimit(RLIMIT_NOFILE, &limit);
    if (status < 0) {
        log_error("getrlimit failed: %s", strerror(errno));
        return NC_ERROR;
    }

    ctx->max_nfd = (uint32_t)limit.rlim_cur;
    ctx->max_ncconn = ctx->max_nfd - ctx->max_nsconn - RESERVED_FDS;
    log_debug(LOG_NOTICE, "max fds %"PRIu32" max client conns %"PRIu32" "
              "max server conns %"PRIu32"", ctx->max_nfd, ctx->max_ncconn,
              ctx->max_nsconn);

    return NC_OK;
}

static struct context *
core_ctx_create(struct instance *nci)
{
    rstatus_t status;
    struct context *ctx;

    ctx = nc_alloc(sizeof(*ctx));
    if (ctx == NULL) {
        return NULL;
    }
    ctx->id = ++ctx_id;
    ctx->cf = NULL;
    ctx->stats = NULL;
    ctx->evb = NULL;
    array_null(&ctx->pool);
    ctx->max_timeout = nci->stats_interval;
    ctx->timeout = ctx->max_timeout;
    ctx->max_nfd = 0;
    ctx->max_ncconn = 0;
    ctx->max_nsconn = 0;
#if 1 //shenzheng 2015-4-28 proxy administer
	ctx->padm = NULL;
#endif //shenzheng 2015-4-28 proxy administer

#if 1 //shenzheng 2015-5-8 config-reload
	ctx->which_pool = 0;
	ctx->cf_swap = NULL;
	array_null(&ctx->pool_swap);
	ctx->reload_thread = 0;
	ctx->conf_version = 0;
	pthread_mutex_init(&ctx->reload_lock, 0);
#endif //shenzheng 2015-7-27 config-reload


    /* parse and create configuration */
#if 1 //shenzheng 2015-6-9 zookeeper
#ifdef NC_ZOOKEEPER
	log_debug(LOG_DEBUG, "nci->zk_start : %d", nci->zk_start);
	log_debug(LOG_DEBUG, "nci->zk_keep : %d", nci->zk_keep);
	log_debug(LOG_DEBUG, "nci->zk_servers : %s", nci->zk_servers);
	log_debug(LOG_DEBUG, "nci->zk_path : %s", nci->zk_path);

	ctx->zkhandle = NULL;
	string_init(&ctx->watch_path);
	string_init(&ctx->zk_servers);
	if(nci->zk_start)
	{
		ctx->cf = conf_create_from_zk(ctx, nci->zk_servers, nci->zk_path);
		if (ctx->cf == NULL) {
	        nc_free(ctx);
	        return NULL;
		}

		ctx->cf->fname = nci->conf_filename;
		
		if(nci->zk_keep)
		{
			status = conf_keep_from_zk(ctx, ctx->zkhandle, nci->zk_path);
			if(status != NC_OK)
			{
				if(ctx->zkhandle != NULL)
				{
					zk_close(ctx->zkhandle);
					ctx->zkhandle = NULL;
				}
				conf_destroy(ctx->cf);
        		nc_free(ctx);
        		return NULL;
			}
		}
		else
		{
			if(ctx->zkhandle != NULL)
			{
				zk_close(ctx->zkhandle);
				ctx->zkhandle = NULL;
			}
		}
	}
	else
	{
#endif
#endif //shenzheng 2015-6-9 zookeeper
    ctx->cf = conf_create(nci->conf_filename);
    if (ctx->cf == NULL) {
        nc_free(ctx);
        return NULL;
    }
#if 1 //shenzheng 2015-6-9 zookeeper
#ifdef NC_ZOOKEEPER
		if(nci->zk_keep)
		{
			void *zkhandle = zk_init(nci->zk_servers);
			if(zkhandle == NULL)
			{
				conf_destroy(ctx->cf);
        		nc_free(ctx);
        		return NULL;
			}
			
			ctx->zkhandle = zkhandle;
			
			status = conf_keep_from_zk(ctx, zkhandle, nci->zk_path);
			if(status != NC_OK)
			{
				if(ctx->zkhandle != NULL)
				{
					zk_close(ctx->zkhandle);
					ctx->zkhandle = NULL;
				}
				conf_destroy(ctx->cf);
        		nc_free(ctx);
        		return NULL;
			}

			if(!string_empty(&ctx->zk_servers))
			{
				string_deinit(&ctx->zk_servers);
			}
			string_copy(&ctx->zk_servers, (uint8_t *)nci->zk_servers, strlen(nci->zk_servers));
		}
	}
#endif
#endif //shenzheng 2015-6-9 zookeeper

    /* initialize server pool from configuration */
    status = server_pool_init(&ctx->pool, &ctx->cf->pool, ctx);
    if (status != NC_OK) {
#if 1 //shenzheng 2015-6-11 zookeeper
#ifdef NC_ZOOKEEPER
		if(ctx->zkhandle != NULL)
		{
			zk_close(ctx->zkhandle);
			ctx->zkhandle = NULL;
		}
		if(!string_empty(&ctx->watch_path))
		{
			string_deinit(&ctx->watch_path);
		}
		if(!string_empty(&ctx->zk_servers))
		{
			string_deinit(&ctx->zk_servers);
		}
#endif
#endif //shenzheng 2015-6-11 zookeeper
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

    /*
     * Get rlimit and calculate max client connections after we have
     * calculated max server connections
     */
    status = core_calc_connections(ctx);
    if (status != NC_OK) {
#if 1 //shenzheng 2015-6-11 zookeeper
#ifdef NC_ZOOKEEPER
		if(ctx->zkhandle != NULL)
		{
			zk_close(ctx->zkhandle);
			ctx->zkhandle = NULL;
		}
		if(!string_empty(&ctx->watch_path))
		{
			string_deinit(&ctx->watch_path);
		}
		if(!string_empty(&ctx->zk_servers))
		{
			string_deinit(&ctx->zk_servers);
		}
#endif
#endif //shenzheng 2015-6-11 zookeeper
        server_pool_deinit(&ctx->pool);
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

    /* create stats per server pool */
    ctx->stats = stats_create(nci->stats_port, nci->stats_addr, nci->stats_interval,
                              nci->hostname, &ctx->pool);
    if (ctx->stats == NULL) {
#if 1 //shenzheng 2015-6-11 zookeeper
#ifdef NC_ZOOKEEPER
		if(ctx->zkhandle != NULL)
		{
			zk_close(ctx->zkhandle);
			ctx->zkhandle = NULL;
		}
		if(!string_empty(&ctx->watch_path))
		{
			string_deinit(&ctx->watch_path);
		}
		if(!string_empty(&ctx->zk_servers))
		{
			string_deinit(&ctx->zk_servers);
		}
#endif
#endif //shenzheng 2015-6-11 zookeeper
        server_pool_deinit(&ctx->pool);
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

#if 1 //shenzheng 2015-7-16 config-reload
	ctx->stats->ctx = ctx;
#endif //shenzheng 2015-7-16 config-reload

    /* initialize event handling for client, proxy and server */
    ctx->evb = event_base_create(EVENT_SIZE, &core_core);
    if (ctx->evb == NULL) {
#if 1 //shenzheng 2015-6-11 zookeeper
#ifdef NC_ZOOKEEPER
		if(ctx->zkhandle != NULL)
		{
			zk_close(ctx->zkhandle);
			ctx->zkhandle = NULL;
		}
		if(!string_empty(&ctx->watch_path))
		{
			string_deinit(&ctx->watch_path);
		}
		if(!string_empty(&ctx->zk_servers))
		{
			string_deinit(&ctx->zk_servers);
		}
#endif
#endif //shenzheng 2015-6-11 zookeeper
        stats_destroy(ctx->stats);
        server_pool_deinit(&ctx->pool);
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

    /* preconnect? servers in server pool */
    status = server_pool_preconnect(ctx);
    if (status != NC_OK) {
#if 1 //shenzheng 2015-6-11 zookeeper
#ifdef NC_ZOOKEEPER
		if(ctx->zkhandle != NULL)
		{
			zk_close(ctx->zkhandle);
			ctx->zkhandle = NULL;
		}
		if(!string_empty(&ctx->watch_path))
		{
			string_deinit(&ctx->watch_path);
		}
		if(!string_empty(&ctx->zk_servers))
		{
			string_deinit(&ctx->zk_servers);
		}
#endif
#endif //shenzheng 2015-6-11 zookeeper
        server_pool_disconnect(ctx);
        event_base_destroy(ctx->evb);
        stats_destroy(ctx->stats);
        server_pool_deinit(&ctx->pool);
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

    /* initialize proxy per server pool */
    status = proxy_init(ctx);
    if (status != NC_OK) {
#if 1 //shenzheng 2015-6-11 zookeeper
#ifdef NC_ZOOKEEPER
		if(ctx->zkhandle != NULL)
		{
			zk_close(ctx->zkhandle);
			ctx->zkhandle = NULL;
		}
		if(!string_empty(&ctx->watch_path))
		{
			string_deinit(&ctx->watch_path);
		}
		if(!string_empty(&ctx->zk_servers))
		{
			string_deinit(&ctx->zk_servers);
		}
#endif
#endif //shenzheng 2015-6-11 zookeeper
        server_pool_disconnect(ctx);
        event_base_destroy(ctx->evb);
        stats_destroy(ctx->stats);
        server_pool_deinit(&ctx->pool);
        conf_destroy(ctx->cf);
        nc_free(ctx);
        return NULL;
    }

#if 1 //shenzheng 2015-4-27 proxy administer
	if(nci->proxy_adm_port > 0)
	{
		ctx->padm = proxy_adm_create(ctx, nci->proxy_adm_addr, nci->proxy_adm_port);
		if (ctx->padm == NULL) {
#if 1 //shenzheng 2015-6-11 zookeeper
#ifdef NC_ZOOKEEPER
			if(ctx->zkhandle != NULL)
			{
				zk_close(ctx->zkhandle);
				ctx->zkhandle = NULL;
			}
			if(!string_empty(&ctx->watch_path))
			{
				string_deinit(&ctx->watch_path);
			}
			if(!string_empty(&ctx->zk_servers))
			{
				string_deinit(&ctx->zk_servers);
			}
#endif
#endif //shenzheng 2015-6-11 zookeeper
			server_pool_disconnect(ctx);
	        event_base_destroy(ctx->evb);
	        stats_destroy(ctx->stats);
			proxy_deinit(ctx);
	        server_pool_deinit(&ctx->pool);
	        conf_destroy(ctx->cf);
	        nc_free(ctx);
			return NULL;
		}
	}
#endif //shenzheng 2015-4-27 proxy administer

    log_debug(LOG_VVERB, "created ctx %p id %"PRIu32"", ctx, ctx->id);

    return ctx;
}

static void
core_ctx_destroy(struct context *ctx)
{
    log_debug(LOG_VVERB, "destroy ctx %p id %"PRIu32"", ctx, ctx->id);
    proxy_deinit(ctx);
    server_pool_disconnect(ctx);
    event_base_destroy(ctx->evb);
    stats_destroy(ctx->stats);
    server_pool_deinit(&ctx->pool);
    conf_destroy(ctx->cf);

#if 1 //shenzheng 2015-4-28 proxy administer
	proxy_adm_destroy(ctx->padm);
#endif //shenzheng 2015-4-28 proxy administer

#if 1 //shenzheng 2015-5-8 config-reload
	ctx->which_pool = 0;
	if(ctx->cf_swap != NULL)
	{
		conf_destroy(ctx->cf_swap);
	}
	server_pool_deinit(&ctx->pool_swap);
	pthread_mutex_destroy(&ctx->reload_lock);
#endif //shenzheng 2015-7-13 config-reload

#if 1 //shenzheng 2015-6-9 zookeeper
#ifdef NC_ZOOKEEPER
	if(ctx->zkhandle)
	{
		zk_close(ctx->zkhandle);
		ctx->zkhandle = NULL;
	}
	if(!string_empty(&ctx->watch_path))
	{
		string_deinit(&ctx->watch_path);
	}
	if(!string_empty(&ctx->zk_servers))
	{
		string_deinit(&ctx->zk_servers);
	}
#endif
#endif //shenzheng 2015-6-9 zookeeper

    nc_free(ctx);
}

struct context *
core_start(struct instance *nci)
{
    struct context *ctx;

    mbuf_init(nci);
    msg_init();
    conn_init();

    ctx = core_ctx_create(nci);
    if (ctx != NULL) {
        nci->ctx = ctx;
        return ctx;
    }

    conn_deinit();
    msg_deinit();
    mbuf_deinit();

    return NULL;
}

void
core_stop(struct context *ctx)
{
    conn_deinit();
    msg_deinit();
    mbuf_deinit();
    core_ctx_destroy(ctx);
}

static rstatus_t
core_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    status = conn->recv(ctx, conn);
    if (status != NC_OK) {
        log_debug(LOG_INFO, "recv on %c %d failed: %s",
                  conn->client ? 'c' : (conn->proxy ? 'p' : 's'), conn->sd,
                  strerror(errno));
    }

    return status;
}

static rstatus_t
core_send(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    status = conn->send(ctx, conn);
    if (status != NC_OK) {
        log_debug(LOG_INFO, "send on %c %d failed: status: %d errno: %d %s",
                  conn->client ? 'c' : (conn->proxy ? 'p' : 's'), conn->sd,
                  status, errno, strerror(errno));
    }

    return status;
}

static void
core_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    char type, *addrstr;

    ASSERT(conn->sd > 0);

    if (conn->client) {
        type = 'c';
        addrstr = nc_unresolve_peer_desc(conn->sd);
    } else {
        type = conn->proxy ? 'p' : 's';
        addrstr = nc_unresolve_addr(conn->addr, conn->addrlen);
    }
    log_debug(LOG_NOTICE, "close %c %d '%s' on event %04"PRIX32" eof %d done "
              "%d rb %zu sb %zu%c %s", type, conn->sd, addrstr, conn->events,
              conn->eof, conn->done, conn->recv_bytes, conn->send_bytes,
              conn->err ? ':' : ' ', conn->err ? strerror(conn->err) : "");

    status = event_del_conn(ctx->evb, conn);
    if (status < 0) {
        log_warn("event del conn %c %d failed, ignored: %s",
                 type, conn->sd, strerror(errno));
    }

    conn->close(ctx, conn);
}

static void
core_error(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    char type = conn->client ? 'c' : (conn->proxy ? 'p' : 's');

    status = nc_get_soerror(conn->sd);
    if (status < 0) {
        log_warn("get soerr on %c %d failed, ignored: %s", type, conn->sd,
                  strerror(errno));
    }
    conn->err = errno;

    core_close(ctx, conn);
}

static void
core_timeout(struct context *ctx)
{
    for (;;) {
        struct msg *msg;
        struct conn *conn;
        int64_t now, then;

        msg = msg_tmo_min();
        if (msg == NULL) {
            ctx->timeout = ctx->max_timeout;
            return;
        }

        /* skip over req that are in-error or done */

        if (msg->error || msg->done) {
            msg_tmo_delete(msg);
            continue;
        }

        /*
         * timeout expired req and all the outstanding req on the timing
         * out server
         */

        conn = msg->tmo_rbe.data;
        then = msg->tmo_rbe.key;

        now = nc_msec_now();
        if (now < then) {
            int delta = (int)(then - now);
            ctx->timeout = MIN(delta, ctx->max_timeout);
            return;
        }

        log_debug(LOG_INFO, "req %"PRIu64" on s %d timedout", msg->id, conn->sd);

        msg_tmo_delete(msg);
        conn->err = ETIMEDOUT;
		
#if 1 //shenzheng 2015-7-30 replace server
		if(conn->replace_server)
		{
			conn_close_for_replace_server(conn, conn->err);
		}
		else
		{
#endif //shenzheng 2015-7-30 replace server
        core_close(ctx, conn);
#if 1 //shenzheng 2015-7-30 replace server
		}	
#endif //shenzheng 2015-7-30 replace server

    }
}

rstatus_t
core_core(void *arg, uint32_t events)
{
    rstatus_t status;
	struct conn *conn = arg;
	struct context *ctx;    

	if (conn->owner == NULL) 
	{        
		log_warn("conn is already unrefed!");        
		return NC_OK;    
	}

#if 1 //shenzheng 2015-7-14 config-reload
	if(conn->reload_conf == 1)
	{
		struct array *pools_curr, *pools_old;
		struct conf *cf;

		log_debug(LOG_DEBUG, "this conn is for reload_conf.");
		
		ctx = conn->owner;
		ASSERT(ctx->reload_thread == 1);
		ASSERT(ctx->stats->pause);

		cf = ctx->cf;
		ctx->cf = ctx->cf_swap;
		ctx->cf_swap = cf;
		if(cf->fname != NULL)
		{
			ctx->cf->fname = ctx->cf_swap->fname;
		}

		ctx->which_pool = ctx->which_pool ? 0 : 1;

		conn->events = events;
		
		if(ctx->which_pool)
		{
			pools_curr = &ctx->pool_swap;
			pools_old = &ctx->pool;
		}
		else
		{
			pools_curr = &ctx->pool;
			pools_old = &ctx->pool_swap;
		}

		status = array_each(pools_curr, server_pool_each_proxy_conn_reset, pools_old);
		ASSERT(status == NC_OK);

		status = array_each(pools_old, server_pool_each_conn_old_close, pools_curr);
		ASSERT(status == NC_OK);
		
		status = array_each(pools_curr, server_pool_each_client_conn_reset, pools_old);
		ASSERT(status == NC_OK);
		

		status = event_del_conn(ctx->evb , conn);
		if (status < 0) {
	        log_warn("event del conn for reload %d failed, ignored: %s",
	        	conn->sd, strerror(errno));
	    }

		ctx->stats->reload_thread = 1;
		
		struct sockaddr_in s_add,c_adda;
		bzero(&s_add,sizeof(struct sockaddr_in));
		s_add.sin_family=AF_INET;	
		s_add.sin_addr.s_addr= inet_addr(ctx->stats->addr.data);	
		s_add.sin_port=htons(ctx->stats->port);

		if(-1 == connect(conn->sd, (struct sockaddr *)(&s_add), sizeof(struct sockaddr)))
		{
			log_error("error: connect to stats for reload error(%s)", strerror(errno));
		}
		

		close(conn->sd);
		conn->sd = -1;
		conn->owner = NULL;
		
		conn_put_for_reload(conn);
		
		ctx->reload_thread = 0;

		ctx->conf_version ++;
		
		return status;
	}
#endif //shenzheng 2015-7-14 config-reload

	ctx = conn_to_ctx(conn);

    log_debug(LOG_VVERB, "event %04"PRIX32" on %c %d", events,
              conn->client ? 'c' : (conn->proxy ? 'p' : 's'), conn->sd);

#if 1 //shenzheng 2015-7-28 replace server
	if(conn->replace_server)
	{
		ASSERT(conn->ctx != NULL);

		if(conn->ctx->conf_version != conn->conf_version_curr)
		{
			conn_close_for_replace_server(conn, ERROR_REPLACE_SERVER_CONF_VERSION_CHANGE);
			return NC_ERROR;
		}
		else
		{
			ASSERT(conn->ctx == ctx);			
		}
	}
#endif //shenzheng 2015-7-28 replace server

    conn->events = events;

    /* error takes precedence over read | write */
    if (events & EVENT_ERR) {
        core_error(ctx, conn);
        return NC_ERROR;
    }

    /* read takes precedence over write */
    if (events & EVENT_READ) {
        status = core_recv(ctx, conn);
        if (status != NC_OK || conn->done || conn->err) {
            core_close(ctx, conn);
            return NC_ERROR;
        }
    }

    if (events & EVENT_WRITE) {
        status = core_send(ctx, conn);
        if (status != NC_OK || conn->done || conn->err) {
            core_close(ctx, conn);
            return NC_ERROR;
        }
    }

    return NC_OK;
}

rstatus_t
core_loop(struct context *ctx)
{
    int nsd;

    nsd = event_wait(ctx->evb, ctx->timeout);
    if (nsd < 0) {
        return nsd;
    }

    core_timeout(ctx);

    stats_swap(ctx->stats);
	
    return NC_OK;
}
