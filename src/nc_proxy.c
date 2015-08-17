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

#include <sys/un.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_proxy.h>

#if 1 //shenzheng 2015-4-28 proxy administer
#include <nc_proto.h>
#include <nc_conf.h>
#endif //shenzheng 2015-4-28 proxy administer

void
proxy_ref(struct conn *conn, void *owner)
{
    struct server_pool *pool = owner;

    ASSERT(!conn->client && conn->proxy);
    ASSERT(conn->owner == NULL);

    conn->family = pool->family;
    conn->addrlen = pool->addrlen;
    conn->addr = pool->addr;

    pool->p_conn = conn;

    /* owner of the proxy connection is the server pool */
    conn->owner = owner;

    log_debug(LOG_VVERB, "ref conn %p owner %p into pool %"PRIu32"", conn,
              pool, pool->idx);
}

void
proxy_unref(struct conn *conn)
{
    struct server_pool *pool;

    ASSERT(!conn->client && conn->proxy);
    ASSERT(conn->owner != NULL);

    pool = conn->owner;
    conn->owner = NULL;

    pool->p_conn = NULL;

    log_debug(LOG_VVERB, "unref conn %p owner %p from pool %"PRIu32"", conn,
              pool, pool->idx);
}

void
proxy_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(!conn->client && conn->proxy);

    if (conn->sd < 0) {
        conn->unref(conn);
        conn_put(conn);
        return;
    }

    ASSERT(conn->rmsg == NULL);
    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("close p %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put(conn);
}

static rstatus_t
proxy_reuse(struct conn *p)
{
    rstatus_t status;
    struct sockaddr_un *un;

    switch (p->family) {
    case AF_INET:
    case AF_INET6:
        status = nc_set_reuseaddr(p->sd);
        break;

    case AF_UNIX:
        /*
         * bind() will fail if the pathname already exist. So, we call unlink()
         * to delete the pathname, in case it already exists. If it does not
         * exist, unlink() returns error, which we ignore
         */
        un = (struct sockaddr_un *) p->addr;
        unlink(un->sun_path);
        status = NC_OK;
        break;

    default:
        NOT_REACHED();
        status = NC_ERROR;
    }

    return status;
}

static rstatus_t
proxy_listen(struct context *ctx, struct conn *p)
{
    rstatus_t status;
    struct server_pool *pool = p->owner;

    ASSERT(p->proxy);

    p->sd = socket(p->family, SOCK_STREAM, 0);
    if (p->sd < 0) {
        log_error("socket failed: %s", strerror(errno));
        return NC_ERROR;
    }

    status = proxy_reuse(p);
    if (status < 0) {
        log_error("reuse of addr '%.*s' for listening on p %d failed: %s",
                  pool->addrstr.len, pool->addrstr.data, p->sd,
                  strerror(errno));
        return NC_ERROR;
    }

    status = bind(p->sd, p->addr, p->addrlen);
    if (status < 0) {
        log_error("bind on p %d to addr '%.*s' failed: %s", p->sd,
                  pool->addrstr.len, pool->addrstr.data, strerror(errno));
        return NC_ERROR;
    }

    status = listen(p->sd, pool->backlog);
    if (status < 0) {
        log_error("listen on p %d on addr '%.*s' failed: %s", p->sd,
                  pool->addrstr.len, pool->addrstr.data, strerror(errno));
        return NC_ERROR;
    }

    status = nc_set_nonblocking(p->sd);
    if (status < 0) {
        log_error("set nonblock on p %d on addr '%.*s' failed: %s", p->sd,
                  pool->addrstr.len, pool->addrstr.data, strerror(errno));
        return NC_ERROR;
    }

    status = event_add_conn(ctx->evb, p);
    if (status < 0) {
        log_error("event add conn p %d on addr '%.*s' failed: %s",
                  p->sd, pool->addrstr.len, pool->addrstr.data,
                  strerror(errno));
        return NC_ERROR;
    }

    status = event_del_out(ctx->evb, p);
    if (status < 0) {
        log_error("event del out p %d on addr '%.*s' failed: %s",
                  p->sd, pool->addrstr.len, pool->addrstr.data,
                  strerror(errno));
        return NC_ERROR;
    }

    return NC_OK;
}

rstatus_t
proxy_each_init(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *pool = elem;
    struct conn *p;

    p = conn_get_proxy(pool);
    if (p == NULL) {
        return NC_ENOMEM;
    }

    status = proxy_listen(pool->ctx, p);
    if (status != NC_OK) {
        p->close(pool->ctx, p);
        return status;
    }

    log_debug(LOG_NOTICE, "p %d listening on '%.*s' in %s pool %"PRIu32" '%.*s'"
              " with %"PRIu32" servers", p->sd, pool->addrstr.len,
              pool->addrstr.data, pool->redis ? "redis" : "memcache",
              pool->idx, pool->name.len, pool->name.data,
              array_n(&pool->server));

    return NC_OK;
}

rstatus_t
proxy_init(struct context *ctx)
{
    rstatus_t status;

    ASSERT(array_n(&ctx->pool) != 0);

    status = array_each(&ctx->pool, proxy_each_init, NULL);
    if (status != NC_OK) {
        proxy_deinit(ctx);
        return status;
    }

    log_debug(LOG_VVERB, "init proxy with %"PRIu32" pools",
              array_n(&ctx->pool));

    return NC_OK;
}

rstatus_t
proxy_each_deinit(void *elem, void *data)
{
    struct server_pool *pool = elem;
    struct conn *p;

    p = pool->p_conn;
    if (p != NULL) {
        p->close(pool->ctx, p);
    }

    return NC_OK;
}

void
proxy_deinit(struct context *ctx)
{
    rstatus_t status;

    ASSERT(array_n(&ctx->pool) != 0);

    status = array_each(&ctx->pool, proxy_each_deinit, NULL);
    if (status != NC_OK) {
        return;
    }

    log_debug(LOG_VVERB, "deinit proxy with %"PRIu32" pools",
              array_n(&ctx->pool));
}

static rstatus_t
proxy_accept(struct context *ctx, struct conn *p)
{
    rstatus_t status;
    struct conn *c;
    int sd;

    ASSERT(p->proxy && !p->client);
    ASSERT(p->sd > 0);
    ASSERT(p->recv_active && p->recv_ready);

    for (;;) {
        sd = accept(p->sd, NULL, NULL);
        if (sd < 0) {
            if (errno == EINTR) {
                log_debug(LOG_VERB, "accept on p %d not ready - eintr", p->sd);
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ECONNABORTED) {
                log_debug(LOG_VERB, "accept on p %d not ready - eagain", p->sd);
                p->recv_ready = 0;
                return NC_OK;
            }

            /* 
             * Workaround of https://github.com/twitter/twemproxy/issues/97
             *
             * We should never reach here because the check for conn_ncurr_cconn()
             * against ctx->max_ncconn should catch this earlier in the cycle.
             * If we reach here ignore EMFILE/ENFILE, return NC_OK will enable
             * the server continue to run instead of close the server socket
             *
             * The right solution however, is on EMFILE/ENFILE to mask out IN
             * event on the proxy and mask it back in when some existing
             * connections gets closed
             */
            if (errno == EMFILE || errno == ENFILE) {
                log_debug(LOG_CRIT, "accept on p %d with max fds %"PRIu32" "
                          "used connections %"PRIu32" max client connections %"PRIu32" "
                          "curr client connections %"PRIu32" failed: %s",
                          p->sd, ctx->max_nfd, conn_ncurr_conn(),
                          ctx->max_ncconn, conn_ncurr_cconn(), strerror(errno));

                p->recv_ready = 0;

                return NC_OK;
            }

            log_error("accept on p %d failed: %s", p->sd, strerror(errno));

            return NC_ERROR;
        }

        break;
    }

#if 1 //shenzheng 2015-7-9 proxy administer
	if (conn_ncurr_cconn() + conn_ncurr_cconn_proxy_adm() >= ctx->max_ncconn) {
#else //shenzheng 2015-7-9 proxy administer
    if (conn_ncurr_cconn() >= ctx->max_ncconn) {
#endif //shenzheng 2015-7-9 proxy administer
        log_debug(LOG_CRIT, "client connections %"PRIu32" exceed limit %"PRIu32,
                  conn_ncurr_cconn(), ctx->max_ncconn);
        status = close(sd);
        if (status < 0) {
            log_error("close c %d failed, ignored: %s", sd, strerror(errno));
        }
        return NC_OK;
    }

    c = conn_get(p->owner, true, p->redis);
    if (c == NULL) {
        log_error("get conn for c %d from p %d failed: %s", sd, p->sd,
                  strerror(errno));
        status = close(sd);
        if (status < 0) {
            log_error("close c %d failed, ignored: %s", sd, strerror(errno));
        }
        return NC_ENOMEM;
    }
    c->sd = sd;

    stats_pool_incr(ctx, c->owner, client_connections);

    status = nc_set_nonblocking(c->sd);
    if (status < 0) {
        log_error("set nonblock on c %d from p %d failed: %s", c->sd, p->sd,
                  strerror(errno));
        c->close(ctx, c);
        return status;
    }

    if (p->family == AF_INET || p->family == AF_INET6) {
        status = nc_set_tcpnodelay(c->sd);
        if (status < 0) {
            log_warn("set tcpnodelay on c %d from p %d failed, ignored: %s",
                     c->sd, p->sd, strerror(errno));
        }
#if 1 //shenzheng 2015-6-5 tcpkeepalive
		struct server_pool *sp = p->owner;
		ASSERT(sp != NULL);
		log_debug(LOG_DEBUG, "pool(%s) tcpkeepalive : %s", 
			sp->name.data, sp->tcpkeepalive? "true" : "false");
		log_debug(LOG_DEBUG, "pool(%s) tcpkeepidle : %d", 
			sp->name.data, sp->tcpkeepidle);
		log_debug(LOG_DEBUG, "pool(%s) tcpkeepintvl : %d", 
			sp->name.data, sp->tcpkeepintvl);
		log_debug(LOG_DEBUG, "pool(%s) tcpkeepcnt : %d", 
			sp->name.data, sp->tcpkeepcnt);
		if(sp->tcpkeepalive)
		{
			status = nc_set_tcpkeepalive(c->sd, sp->tcpkeepidle, 
				sp->tcpkeepintvl, sp->tcpkeepcnt);
			if (status != NC_OK) {
				log_warn("set tcpkeepalive on c %d from p %d failed, ignored.",
						 c->sd, p->sd);
			}
		}
#endif //shenzheng 2015-6-5 tcpkeepalive

    }

    status = event_add_conn(ctx->evb, c);
    if (status < 0) {
        log_error("event add conn from p %d failed: %s", p->sd,
                  strerror(errno));
        c->close(ctx, c);
        return status;
    }

    log_debug(LOG_NOTICE, "accepted c %d on p %d from '%s'", c->sd, p->sd,
              nc_unresolve_peer_desc(c->sd));

    return NC_OK;
}

rstatus_t
proxy_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(conn->proxy && !conn->client);
    ASSERT(conn->recv_active);

    conn->recv_ready = 1;
    do {
        status = proxy_accept(ctx, conn);
        if (status != NC_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return NC_OK;
}

#if 1 //shenzheng 2015-4-27 proxy administer
static rstatus_t
proxy_adm_listen(struct proxy_adm *padm, struct conn *p)
{
    rstatus_t status;
	ASSERT(padm != NULL);
	
    ASSERT(p->proxy);

    p->sd = socket(p->family, SOCK_STREAM, 0);
    if (p->sd < 0) {
        log_error("socket failed: %s", strerror(errno));
        return NC_ERROR;
    }
	
    status = proxy_reuse(p);
    if (status < 0) {
        log_error("reuse of addr '%.*s' for listening on p %d failed: %s",
                  padm->addrstr.len, padm->addrstr.data, p->sd,
                  strerror(errno));
        return NC_ERROR;
    }

    status = bind(p->sd, p->addr, p->addrlen);
    if (status < 0) {
        log_error("bind on p %d to addr '%.*s' failed: %s", p->sd,
                  padm->addrstr.len, padm->addrstr.data, strerror(errno));
        return NC_ERROR;
    }

    status = listen(p->sd, 30);
    if (status < 0) {
        log_error("listen on p %d on addr '%.*s' failed: %s", p->sd,
                  padm->addrstr.len, padm->addrstr.data, strerror(errno));
        return NC_ERROR;
    }

    status = nc_set_nonblocking(p->sd);
    if (status < 0) {
        log_error("set nonblock on p %d on addr '%.*s' failed: %s", p->sd,
                  padm->addrstr.len, padm->addrstr.data, strerror(errno));
        return NC_ERROR;
    }

    status = event_add_conn(padm->evb, p);
    if (status < 0) {
        log_error("event add conn p %d on addr '%.*s' failed: %s",
                  p->sd, padm->addrstr.len, padm->addrstr.data,
                  strerror(errno));
        return NC_ERROR;
    }

    status = event_del_out(padm->evb, p);
    if (status < 0) {
        log_error("event del out p %d on addr '%.*s' failed: %s",
                  p->sd, padm->addrstr.len, padm->addrstr.data,
                  strerror(errno));
        return NC_ERROR;
    }

    return NC_OK;
}

static void *
proxy_adm_start(void * arg)
{
	ASSERT(arg != NULL);

	int nsd;
	
	struct context *ctx = arg;
	struct proxy_adm *padm = ctx->padm;
	ASSERT(padm != NULL);

	log_debug(LOG_DEBUG, "proxy_adm thread(%u) starting...", padm->tid);

    for (;;) {
    	nsd = event_wait(padm->evb, ctx->timeout);
    	if (nsd < 0) {
        	return NULL;
    	}
    }
	
	return NULL;
}

struct proxy_adm *
proxy_adm_create(struct context *ctx, char *proxy_adm_ip, uint16_t proxy_adm_port)
{
	rstatus_t status;
	struct conn *p;
	struct proxy_adm *padm;

	padm = nc_alloc(sizeof(*padm));
	if (padm == NULL) {
        return NULL;
    }

	padm->tid = (pthread_t) -1;
	padm->p_conn = NULL;
	padm->evb = NULL;
	padm->nc_conn_q = 0;
    TAILQ_INIT(&padm->c_conn_q);

	string_set_raw(&padm->addrstr, proxy_adm_ip);
	padm->port = proxy_adm_port;

    status = nc_resolve(&padm->addrstr, padm->port, &padm->si);
    if (status < 0) {
        goto error;
    }

	ctx->padm = padm;

	p = conn_get_proxy_adm(ctx, -1, false);
	if (p == NULL) {
    	goto error;
    }
	
	
	padm->p_conn = p;

	padm->evb = event_base_create(100, &proxy_adm_loop);
    if(padm->evb == NULL) {
		goto error;
    }
	
	status = proxy_adm_listen(padm, p);
	if(status != NC_OK)
	{
		goto error;
	}

	
	status = pthread_create(&padm->tid, NULL, proxy_adm_start, ctx);
	if (status < 0) {
		log_error("proxy administer thread start failed: %s", strerror(status));
		goto error;
	}

	return padm;	

error:
	proxy_adm_destroy(padm);
	return NULL;
}

void
proxy_adm_destroy(struct proxy_adm *padm)
{
	ASSERT(padm != NULL);
	
    ASSERT(TAILQ_EMPTY(&padm->c_conn_q) && padm->nc_conn_q == 0);
	
	if(padm->p_conn != NULL)
	{
		ASSERT(padm->p_conn->proxy);

		padm->p_conn->close(padm->p_conn->owner, padm->p_conn);
	}

	if(padm->evb != NULL)
	{
		event_base_destroy(padm->evb);
	}
	
	nc_free(padm);
}


rstatus_t
proxy_adm_loop(void *arg, uint32_t events)
{
	rstatus_t status;
	struct conn *conn = arg;
	ASSERT(conn->owner != NULL);

    struct context *ctx = conn->owner;
	struct proxy_adm *padm = ctx->padm;
	struct array *pools_old;
	
    log_debug(LOG_VVERB, "event %04"PRIX32" on %c %d", events,
              conn->client ? 'c' : (conn->proxy ? 'p' : 's'), conn->sd);

    conn->events = events;

    /* error takes precedence over read | write */
    if (events & EVENT_ERR) {
        //core_error(ctx, conn);
        nc_get_soerror(conn->sd);
		conn->err = errno;
		event_del_conn(padm->evb, conn);
		conn->close(ctx, conn);
        return NC_ERROR;
    }

    /* read takes precedence over write */
    if (events & EVENT_READ) {
        status = conn->recv(ctx, conn);
        if (status != NC_OK || conn->done || conn->err) {
			if(!conn->done && conn->err == EINVAL)
			{
				conn->err = 0;

				ASSERT(conn->rmsg != NULL);
				conn->rmsg->done = 1;
			    conn->enqueue_outq(ctx, conn, conn->rmsg);
				conn->rmsg = NULL;
				status = event_add_out(padm->evb, conn);
    			if (status != NC_OK) {
        			event_del_conn(padm->evb, conn);
					conn->close(ctx, conn);
		       		return NC_ERROR;
    			}
			}
			else
			{
				event_del_conn(padm->evb, conn);
				conn->close(ctx, conn);
		        return NC_ERROR;
			}
        }
    }

    if (events & EVENT_WRITE) {
        status = conn->send(ctx, conn);
        if (status != NC_OK || conn->done || conn->err) {
            event_del_conn(padm->evb, conn);
			conn->close(ctx, conn);
            return NC_ERROR;
        }
    }

	return NC_OK;
}


static rstatus_t
proxy_adm_accept(struct context *ctx, struct conn *p)
{
    rstatus_t status;
    struct conn *c;
    int sd;
	struct proxy_adm *padm;

    ASSERT(p->proxy && !p->client);
    ASSERT(p->sd > 0);
    ASSERT(p->recv_active && p->recv_ready);
	padm = ctx->padm;
	ASSERT(padm != NULL);
    for (;;) {
        sd = accept(p->sd, NULL, NULL);
        if (sd < 0) {
            if (errno == EINTR) {
                log_debug(LOG_VERB, "accept on p %d not ready - eintr", p->sd);
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ECONNABORTED) {
                log_debug(LOG_VERB, "accept on p %d not ready - eagain", p->sd);
                p->recv_ready = 0;
                return NC_OK;
            }

            /* 
             * Workaround of https://github.com/twitter/twemproxy/issues/97
             *
             * We should never reach here because the check for conn_ncurr_cconn()
             * against ctx->max_ncconn should catch this earlier in the cycle.
             * If we reach here ignore EMFILE/ENFILE, return NC_OK will enable
             * the server continue to run instead of close the server socket
             *
             * The right solution however, is on EMFILE/ENFILE to mask out IN
             * event on the proxy and mask it back in when some existing
             * connections gets closed
             */
            if (errno == EMFILE || errno == ENFILE) {
                log_debug(LOG_CRIT, "accept on p %d with max fds %"PRIu32" "
                          "used connections %"PRIu32" max client connections %"PRIu32" "
                          "curr client connections %"PRIu32" failed: %s",
                          p->sd, ctx->max_nfd, conn_ncurr_conn(),
                          ctx->max_ncconn, conn_ncurr_cconn(), strerror(errno));

                p->recv_ready = 0;

                return NC_OK;
            }

            log_error("accept on p %d failed: %s", p->sd, strerror(errno));

            return NC_ERROR;
        }

        break;
    }

    if (conn_ncurr_cconn_proxy_adm() + conn_ncurr_cconn() >= ctx->max_ncconn) {
        log_debug(LOG_CRIT, "client connections %"PRIu32" exceed limit %"PRIu32,
                  conn_ncurr_cconn(), ctx->max_ncconn);
        status = close(sd);
        if (status < 0) {
            log_error("close c %d failed, ignored: %s", sd, strerror(errno));
        }
        return NC_OK;
    }

    c = conn_get_proxy_adm(ctx, sd, true);
	
    if (c == NULL) {
        log_error("get conn for c %d from p %d failed: %s", sd, p->sd,
                  strerror(errno));
        status = close(sd);
        if (status < 0) {
            log_error("close c %d failed, ignored: %s", sd, strerror(errno));
        }
        return NC_ENOMEM;
    }
    

    //stats_pool_incr(ctx, c->owner, client_connections);

    status = nc_set_nonblocking(c->sd);
    if (status < 0) {
        log_error("set nonblock on c %d from p %d failed: %s", c->sd, p->sd,
                  strerror(errno));
        c->close(ctx, c);
        return status;
    }

    if (p->family == AF_INET || p->family == AF_INET6) {
        status = nc_set_tcpnodelay(c->sd);
        if (status < 0) {
            log_warn("set tcpnodelay on c %d from p %d failed, ignored: %s",
                     c->sd, p->sd, strerror(errno));
        }
    }

    status = event_add_conn(padm->evb, c);
    if (status < 0) {
        log_error("event add conn from p %d failed: %s", p->sd,
                  strerror(errno));
        c->close(ctx, c);
        return status;
    }

    log_debug(LOG_NOTICE, "accepted c %d on p %d from '%s'", c->sd, p->sd,
              nc_unresolve_peer_desc(c->sd));

    return NC_OK;
}


rstatus_t
proxy_adm_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(conn->proxy && !conn->client);
    ASSERT(conn->recv_active);

    conn->recv_ready = 1;
    do {
        status = proxy_adm_accept(ctx, conn);
        if (status != NC_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return NC_OK;
}

void
proxy_adm_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(!conn->client && conn->proxy);

    if (conn->sd < 0) {
        conn->unref(conn);
        conn_put_proxy_adm(conn);
        return;
    }

    ASSERT(conn->rmsg == NULL);
    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("close p %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put_proxy_adm(conn);
}


void
proxy_adm_ref(struct conn *conn, void *owner)
{
    struct context *ctx = owner;
	struct proxy_adm *padm;
    ASSERT(!conn->client && conn->proxy);
    ASSERT(conn->owner == NULL);

	padm = ctx->padm;
	ASSERT(padm != NULL);
	
    conn->family = padm->si.family;
    conn->addrlen = padm->si.addrlen;
    conn->addr = (struct sockaddr *)&padm->si.addr;

    padm->p_conn = conn;

    /* owner of the proxy connection is the server pool */
    conn->owner = owner;

    log_debug(LOG_VVERB, "ref conn %p owner %p into context %"PRIu32"", conn,
              ctx, ctx->id);
}

void
proxy_adm_unref(struct conn *conn)
{
    struct context *ctx;
	struct proxy_adm *padm;
    ASSERT(!conn->client && conn->proxy);
    ASSERT(conn->owner != NULL);

	ctx = conn->owner;

	padm = ctx->padm;
	ASSERT(padm != NULL);
    
    conn->owner = NULL;

    padm->p_conn = NULL;

    log_debug(LOG_VVERB, "unref conn %p owner %p from context %"PRIu32"", conn,
              ctx, ctx->id);
}

bool
proxy_adm_client_active(struct conn *conn)
{
    ASSERT(conn->client && !conn->proxy);

    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        log_debug(LOG_VVERB, "c %d is active", conn->sd);
        return true;
    }

    if (conn->rmsg != NULL) {
        log_debug(LOG_VVERB, "c %d is active", conn->sd);
        return true;
    }

    if (conn->smsg != NULL) {
        log_debug(LOG_VVERB, "c %d is active", conn->sd);
        return true;
    }

    log_debug(LOG_VVERB, "c %d is inactive", conn->sd);

    return false;
}

void
proxy_adm_client_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */

    ASSERT(conn->client && !conn->proxy);

    //client_close_stats(ctx, conn->owner, conn->err, conn->eof);

    if (conn->sd < 0) {
        conn->unref(conn);
        conn_put_proxy_adm(conn);
        return;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        conn->rmsg = NULL;

        ASSERT(msg->peer == NULL);
        ASSERT(msg->request && !msg->done);

        log_debug(LOG_INFO, "close c %d discarding pending req %"PRIu64" len "
                  "%"PRIu32" type %d", conn->sd, msg->id, msg->mlen,
                  msg->type);

        req_put(msg);
    }

    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, c_tqe);

        /* dequeue the message (request) from client outq */
        conn->dequeue_outq(ctx, conn, msg);

        if (msg->done) {
            log_debug(LOG_INFO, "close c %d discarding %s req %"PRIu64" len "
                      "%"PRIu32" type %d", conn->sd,
                      msg->error ? "error": "completed", msg->id, msg->mlen,
                      msg->type);
            req_put(msg);
        } else {
            msg->swallow = 1;

            ASSERT(msg->request);

            ASSERT(msg->peer == NULL);

            log_debug(LOG_INFO, "close c %d schedule swallow of req %"PRIu64" "
                      "len %"PRIu32" type %d", conn->sd, msg->id, msg->mlen,
                      msg->type);
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("close c %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put_proxy_adm(conn);
}

void
proxy_adm_client_ref(struct conn *conn, void *owner)
{
    struct context *ctx = owner;
	struct proxy_adm *padm;
    ASSERT(conn->client && !conn->proxy);
    ASSERT(conn->owner == NULL);

	padm = ctx->padm;
	ASSERT(padm != NULL);
    /*
     * We use null pointer as the sockaddr argument in the accept() call as
     * we are not interested in the address of the peer for the accepted
     * connection
     */
    conn->family = 0;
    conn->addrlen = 0;
    conn->addr = NULL;

    padm->nc_conn_q++;
    TAILQ_INSERT_TAIL(&padm->c_conn_q, conn, conn_tqe);

    /* owner of the client connection is the server pool */
    conn->owner = owner;

    log_debug(LOG_VVERB, "ref conn %p owner %p into context", conn, ctx);
}

void
proxy_adm_client_unref(struct conn *conn)
{
    struct context *ctx = conn->owner;
	struct proxy_adm *padm;

    ASSERT(conn->client && !conn->proxy);
    ASSERT(conn->owner != NULL);

	padm = ctx->padm;
	ASSERT(padm != NULL);
    conn->owner = NULL;

    ASSERT(padm->nc_conn_q != 0);
    padm->nc_conn_q--;
    TAILQ_REMOVE(&padm->c_conn_q, conn, conn_tqe);

    log_debug(LOG_VVERB, "unref conn %p owner %p from context", conn, ctx);
}

static bool
proxy_adm_arg0(struct msg *r)
{
	switch (r->type) {
	case MSG_REQ_PROXY_ADM_SHOW_CONF:
	case MSG_REQ_PROXY_ADM_SHOW_OCONF:
	case MSG_REQ_PROXY_ADM_RELOAD_CONF:
	case MSG_REQ_PROXY_ADM_SHOW_POOLS:
	return true;

    default:
        break;
	}
	return false;
}

static bool
proxy_adm_arg1(struct msg *r)
{
	switch (r->type) {
	case MSG_REQ_PROXY_ADM_SHOW_POOL:
	case MSG_REQ_PROXY_ADM_SHOW_SERVERS:
#if 1 //shenzheng 2015-6-15 zookeeper
#ifdef NC_ZOOKEEPER
	case MSG_REQ_PROXY_ADM_DEL_WATCH:
	case MSG_REQ_PROXY_ADM_SHOW_WATCH:
#endif
#endif //shenzheng 2015-6-15 zookeeper
	return true;

    default:
        break;
	}
	return false;
}

static bool
proxy_adm_arg2(struct msg *r)
{
	switch (r->type) {
	case MSG_REQ_PROXY_ADM_FIND_KEY:
#if 0 //shenzheng 2015-6-15 zookeeper
#ifdef NC_ZOOKEEPER
	case MSG_REQ_PROXY_ADM_SET_WATCH:
	case MSG_REQ_PROXY_ADM_RESET_WATCH:
#endif
#endif //shenzheng 2015-6-15 zookeeper
	return true;

    default:
        break;
	}
	return false;
}

static bool
proxy_adm_arg2or3(struct msg *r)
{
	switch (r->type) {
#if 1 //shenzheng 2015-6-15 zookeeper
#ifdef NC_ZOOKEEPER
	case MSG_REQ_PROXY_ADM_SET_WATCH:
	case MSG_REQ_PROXY_ADM_RESET_WATCH:
#endif
#endif //shenzheng 2015-6-15 zookeeper
	return true;

    default:
        break;
	}
	return false;
}


static bool
proxy_adm_arg2ormore(struct msg *r)
{
	switch (r->type) {
	case MSG_REQ_PROXY_ADM_FIND_KEYS:
	return true;

    default:
        break;
	}
	return false;
}


void
proxy_adm_parse_req(struct msg *r)
{
    struct mbuf *b;
    uint8_t *p, *m;
    uint8_t ch;
    enum {
        SW_START,
        SW_REQ_TYPE,
        SW_SPACES_BEFORE_KEY,
        SW_KEY,
        SW_SPACES_BEFORE_KEYS,
        SW_CRLF,
        SW_ALMOST_DONE,
        SW_SENTINEL
    } state;

    state = r->state;
    b = STAILQ_LAST(&r->mhdr, mbuf, next);

    ASSERT(r->request);
    ASSERT(!r->redis);
    ASSERT(state >= SW_START && state < SW_SENTINEL);
    ASSERT(b != NULL);
    ASSERT(b->pos <= b->last);

    /* validate the parsing maker */
    ASSERT(r->pos != NULL);
    ASSERT(r->pos >= b->pos && r->pos <= b->last);

    for (p = r->pos; p < b->last; p++) {
        ch = *p;

        switch (state) {

        case SW_START:
            if (ch == ' ') {
                break;
            }

            if (!islower(ch)) {
                goto error;
            }

            /* req_start <- p; type_start <- p */
            r->token = p;
            state = SW_REQ_TYPE;

            break;

        case SW_REQ_TYPE:
            if (ch == ' ' || ch == CR) {
                /* type_end = p - 1 */
                m = r->token;
                r->token = NULL;
                r->type = MSG_UNKNOWN;
                r->narg++;

                switch (p - m) {
				case 4:
					if (str4cmp(m, 'q', 'u', 'i', 't')) {
                        r->type = MSG_REQ_PROXY_ADM_QUIT;
						r->quit = 1;
                        break;
                    }

					if (str4cmp(m, 'h', 'e', 'l', 'p')) {
                        r->type = MSG_REQ_PROXY_ADM_HELP;
						break;
                    }

					break;
					
                case 8:

					if (str8cmp(m, 'f', 'i', 'n', 'd', '_', 'k', 'e', 'y')) {
                        r->type = MSG_REQ_PROXY_ADM_FIND_KEY;
                        break;
                    }
					
                    break;

                case 9:
					if (str9cmp(m, 's', 'h', 'o', 'w', '_', 'c', 'o', 'n', 'f')) {
                        r->type = MSG_REQ_PROXY_ADM_SHOW_CONF;
                        break;
                    }
					
                    if (str9cmp(m, 's', 'h', 'o', 'w', '_', 'p', 'o', 'o', 'l')) {
                        r->type = MSG_REQ_PROXY_ADM_SHOW_POOL;
                        break;
                    }

					if (str9cmp(m, 'f', 'i', 'n', 'd', '_', 'k', 'e', 'y', 's')) {
                        r->type = MSG_REQ_PROXY_ADM_FIND_KEYS;
                        break;
                    }

#if 1 //shenzheng 2015-6-15 zookeeper
#ifdef NC_ZOOKEEPER
					if (str9cmp(m, 's', 'e', 't', '_', 'w', 'a', 't', 'c', 'h')) {
                        r->type = MSG_REQ_PROXY_ADM_SET_WATCH;
                        break;
                    }

					if (str9cmp(m, 'd', 'e', 'l', '_', 'w', 'a', 't', 'c', 'h')) {
                        r->type = MSG_REQ_PROXY_ADM_DEL_WATCH;
                        break;
                    }
#endif
#endif //shenzheng 2015-6-15 zookeeper

					break;

                case 10:
					if (str10cmp(m, 's', 'h', 'o', 'w', '_', 'o', 'c', 'o', 'n', 'f')) {
                        r->type = MSG_REQ_PROXY_ADM_SHOW_OCONF;
                        break;
                    }
					
                    if (str10cmp(m, 's', 'h', 'o', 'w', '_', 'p', 'o', 'o', 'l', 's')) {
                        r->type = MSG_REQ_PROXY_ADM_SHOW_POOLS;
                        break;
                    }
					
#if 1 //shenzheng 2015-6-16 zookeeper
#ifdef NC_ZOOKEEPER
					if (str10cmp(m, 's', 'h', 'o', 'w', '_', 'w', 'a', 't', 'c', 'h')) {
                        r->type = MSG_REQ_PROXY_ADM_SHOW_WATCH;
                        break;
                    }
#endif
#endif //shenzheng 2015-6-16 zookeeper

                    break;

                case 11:
                    if (str11cmp(m, 'r', 'e', 'l', 'o', 'a', 'd', '_', 'c', 'o', 'n', 'f')) {
                        r->type = MSG_REQ_PROXY_ADM_RELOAD_CONF;
                        break;
                    }

#if 1 //shenzheng 2015-6-15 zookeeper
#ifdef NC_ZOOKEEPER
					if (str11cmp(m, 'r', 'e', 's', 'e', 't', '_', 'w', 'a', 't', 'c', 'h')) {
                        r->type = MSG_REQ_PROXY_ADM_RESET_WATCH;
                        break;
                    }
#endif
#endif //shenzheng 2015-6-15 zookeeper

                    break;

				case 12:
                    if (str12cmp(m, 's', 'h', 'o', 'w', '_', 's', 'e', 'r', 'v', 'e', 'r', 's')) {
                        r->type = MSG_REQ_PROXY_ADM_SHOW_SERVERS;
                        break;
                    }
					
                    break;

                }

                switch (r->type) {
                case MSG_REQ_PROXY_ADM_FIND_KEY:
				case MSG_REQ_PROXY_ADM_SHOW_POOL:
				case MSG_REQ_PROXY_ADM_FIND_KEYS:
				case MSG_REQ_PROXY_ADM_SHOW_SERVERS:
#if 1 //shenzheng 2015-6-15 zookeeper
#ifdef NC_ZOOKEEPER
				case MSG_REQ_PROXY_ADM_SET_WATCH:
				case MSG_REQ_PROXY_ADM_DEL_WATCH:
				case MSG_REQ_PROXY_ADM_RESET_WATCH:
				case MSG_REQ_PROXY_ADM_SHOW_WATCH:
#endif
#endif //shenzheng 2015-6-15 zookeeper
                    if (ch == CR) {
                        goto error;
                    }
                    state = SW_SPACES_BEFORE_KEY;
                    break;

				case MSG_REQ_PROXY_ADM_QUIT:
				case MSG_REQ_PROXY_ADM_HELP:					
				case MSG_REQ_PROXY_ADM_SHOW_POOLS:
                case MSG_REQ_PROXY_ADM_SHOW_CONF:
				case MSG_REQ_PROXY_ADM_SHOW_OCONF:
				case MSG_REQ_PROXY_ADM_RELOAD_CONF:
                    p = p - 1; /* go back by 1 byte */
                    state = SW_CRLF;
                    break;

                case MSG_UNKNOWN:
                    goto error;

                default:
                    NOT_REACHED();
                }

            }

            break;

        case SW_SPACES_BEFORE_KEY:
			if (ch == CR)
			{
				goto error;
			}
            if (ch != ' ') {
                p = p - 1; /* go back by 1 byte */
                r->token = NULL;
                state = SW_KEY;
            }
            break;

        case SW_KEY:
            if (r->token == NULL) {
                r->token = p;
            }
            if (ch == ' ' || ch == CR) {
                struct keypos *kpos;

                if ((p - r->token) > mbuf_data_size()) {
                    log_error("parsed bad req %"PRIu64" of type %d with key "
                              "prefix '%.*s...' and length %d that exceeds "
                              "maximum key length", r->id, r->type, 16,
                              r->token, p - r->token);
                    goto error;
                }

                kpos = array_push(r->keys);
                if (kpos == NULL) {
                    goto enomem;
                }
                kpos->start = r->token;
                kpos->end = p;
				if(kpos->start == kpos->end)
				{
					goto error;
				}

                r->narg++;
                r->token = NULL;

                /* get next state */
                if (proxy_adm_arg1(r)) {
                    state = SW_CRLF;
                } else if (proxy_adm_arg2(r)) {
                	ASSERT(array_n(r->keys) > 0);
                	if(array_n(r->keys) == 1)
                	{
                		if(ch == CR)
                		{
							goto error;
						}
                    	else
						{
							state = SW_SPACES_BEFORE_KEY;
						}
                	}
					else if(array_n(r->keys) == 2)
					{
						state = SW_CRLF;
					}
					else
					{
						goto error;
					}
                } 
				else if (proxy_adm_arg2or3(r)) {
                	ASSERT(array_n(r->keys) > 0);
                	if(array_n(r->keys) == 1)
                	{
                		if(ch == CR)
                		{
							goto error;
						}
                    	else
						{
							state = SW_SPACES_BEFORE_KEY;
						}
                	}
					else if(array_n(r->keys) == 2){
						if(ch == CR)
                		{
							state = SW_CRLF;
						}
                    	else
						{
							state = SW_SPACES_BEFORE_KEYS;
						}			
					}
					else if(array_n(r->keys) == 3)
					{
						state = SW_CRLF;
					}
					else
					{
						goto error;
					}
                }
				else if (proxy_adm_arg2ormore(r)) {

					ASSERT(array_n(r->keys) > 0);
                	if(array_n(r->keys) == 1)
                	{
                		if(ch == CR)
                		{
							goto error;
						}
                    	else
						{
							state = SW_SPACES_BEFORE_KEY;
						}
                	}
					else
					{
						state = SW_SPACES_BEFORE_KEYS;
					}
                }else {
                    goto error;
                }

				if(ch == CR)
				{
					p = p - 1;
				}
            }

            break;

        case SW_SPACES_BEFORE_KEYS:
            ASSERT(proxy_adm_arg2ormore(r) || proxy_adm_arg2or3(r));
            switch (ch) {
            case ' ':
                break;

            case CR:
                state = SW_ALMOST_DONE;
                break;

            default:
                r->token = NULL;
                p = p - 1; /* go back by 1 byte */
                state = SW_KEY;
            }

            break;
    
        case SW_CRLF:
            switch (ch) {
            case ' ':
                break;

            case CR:
                state = SW_ALMOST_DONE;
                break;

            default:
                goto error;
            }

            break;

        case SW_ALMOST_DONE:
            switch (ch) {
            case LF:
                /* req_end <- p */
                goto done;

            default:
                goto error;
            }

            break;

        case SW_SENTINEL:
        default:
            NOT_REACHED();
            break;

        }
    }

    /*
     * At this point, buffer from b->pos to b->last has been parsed completely
     * but we haven't been able to reach to any conclusion. Normally, this
     * means that we have to parse again starting from the state we are in
     * after more data has been read. The newly read data is either read into
     * a new mbuf, if existing mbuf is full (b->last == b->end) or into the
     * existing mbuf.
     *
     * The only exception to this is when the existing mbuf is full (b->last
     * is at b->end) and token marker is set, which means that we have to
     * copy the partial token into a new mbuf and parse again with more data
     * read into new mbuf.
     */
    ASSERT(p == b->last);
    r->pos = p;
    r->state = state;

    if (b->last == b->end && r->token != NULL) {
        r->pos = r->token;
        r->token = NULL;
        r->result = MSG_PARSE_REPAIR;
    } else {
        r->result = MSG_PARSE_AGAIN;
    }

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed req %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

done:
    ASSERT(r->type > MSG_UNKNOWN && r->type < MSG_SENTINEL);
    r->pos = p + 1;
    ASSERT(r->pos <= b->last);
    r->state = SW_START;
    r->result = MSG_PARSE_OK;

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed req %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

enomem:
    r->result = MSG_PARSE_ERROR;
    r->state = state;

    log_hexdump(LOG_INFO, b->pos, mbuf_length(b), "out of memory on parse req %"PRIu64" "
                "res %d type %d state %d", r->id, r->result, r->type, r->state);

    return;

error:
    r->result = MSG_PARSE_ERROR;
    r->state = state;

	struct msg *resp;
	resp = msg_get_error_proxy_adm(EINVAL);
	if(resp == NULL)
	{
		return;
	}
	resp->request = 0;
	resp->peer = r;
	r->peer = resp;
	
	errno = EINVAL;

	
	
    log_hexdump(LOG_INFO, b->pos, mbuf_length(b), "parsed bad req %"PRIu64" "
                "res %d type %d state %d", r->id, r->result, r->type,
                r->state);
}

void
proxy_adm_parse_rsp(struct msg *r)
{
    struct mbuf *b;
    uint8_t *p, *m;
    uint8_t ch;
    enum {
        SW_START,
        SW_RSP_NUM,
        SW_RSP_STR,
        SW_SPACES_BEFORE_KEY,
        SW_KEY,
        SW_SPACES_BEFORE_FLAGS,     /* 5 */
        SW_FLAGS,
        SW_SPACES_BEFORE_VLEN,
        SW_VLEN,
        SW_RUNTO_VAL,
        SW_VAL,                     /* 10 */
        SW_VAL_LF,
        SW_END,
        SW_RUNTO_CRLF,
        SW_CRLF,
        SW_ALMOST_DONE,             /* 15 */
        SW_SENTINEL
    } state;

    state = r->state;
    b = STAILQ_LAST(&r->mhdr, mbuf, next);

    ASSERT(!r->request);
    ASSERT(!r->redis);
    ASSERT(state >= SW_START && state < SW_SENTINEL);
    ASSERT(b != NULL);
    ASSERT(b->pos <= b->last);

    /* validate the parsing marker */
    ASSERT(r->pos != NULL);
    ASSERT(r->pos >= b->pos && r->pos <= b->last);

    for (p = r->pos; p < b->last; p++) {
        ch = *p;

        switch (state) {
        case SW_START:
            if (isdigit(ch)) {
                state = SW_RSP_NUM;
            } else {
                state = SW_RSP_STR;
            }
            p = p - 1; /* go back by 1 byte */

            break;

        case SW_RSP_NUM:
            if (r->token == NULL) {
                /* rsp_start <- p; type_start <- p */
                r->token = p;
            }

            if (isdigit(ch)) {
                /* num <- num * 10 + (ch - '0') */
                ;
            } else if (ch == ' ' || ch == CR) {
                /* type_end <- p - 1 */
                r->token = NULL;
                r->type = MSG_RSP_MC_NUM;
                p = p - 1; /* go back by 1 byte */
                state = SW_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RSP_STR:
            if (r->token == NULL) {
                /* rsp_start <- p; type_start <- p */
                r->token = p;
            }

            if (ch == ' ' || ch == CR) {
                /* type_end <- p - 1 */
                m = r->token;
                /* r->token = NULL; */
                r->type = MSG_UNKNOWN;

                switch (p - m) {
                case 3:
                    if (str4cmp(m, 'E', 'N', 'D', '\r')) {
                        r->type = MSG_RSP_MC_END;
                        /* end_start <- m; end_end <- p - 1 */
                        r->end = m;
                        break;
                    }

                    break;

                case 5:
                    if (str5cmp(m, 'V', 'A', 'L', 'U', 'E')) {
                        /*
                         * Encompasses responses for 'get', 'gets' and
                         * 'cas' command.
                         */
                        r->type = MSG_RSP_MC_VALUE;
                        break;
                    }

                    if (str5cmp(m, 'E', 'R', 'R', 'O', 'R')) {
                        r->type = MSG_RSP_MC_ERROR;
                        break;
                    }

                    break;

                case 6:
                    if (str6cmp(m, 'S', 'T', 'O', 'R', 'E', 'D')) {
                        r->type = MSG_RSP_MC_STORED;
                        break;
                    }

                    if (str6cmp(m, 'E', 'X', 'I', 'S', 'T', 'S')) {
                        r->type = MSG_RSP_MC_EXISTS;
                        break;
                    }

                    break;

                case 7:
                    if (str7cmp(m, 'D', 'E', 'L', 'E', 'T', 'E', 'D')) {
                        r->type = MSG_RSP_MC_DELETED;
                        break;
                    }

                    break;

                case 9:
                    if (str9cmp(m, 'N', 'O', 'T', '_', 'F', 'O', 'U', 'N', 'D')) {
                        r->type = MSG_RSP_MC_NOT_FOUND;
                        break;
                    }

                    break;

                case 10:
                    if (str10cmp(m, 'N', 'O', 'T', '_', 'S', 'T', 'O', 'R', 'E', 'D')) {
                        r->type = MSG_RSP_MC_NOT_STORED;
                        break;
                    }

                    break;

                case 12:
                    if (str12cmp(m, 'C', 'L', 'I', 'E', 'N', 'T', '_', 'E', 'R', 'R', 'O', 'R')) {
                        r->type = MSG_RSP_MC_CLIENT_ERROR;
                        break;
                    }

                    if (str12cmp(m, 'S', 'E', 'R', 'V', 'E', 'R', '_', 'E', 'R', 'R', 'O', 'R')) {
                        r->type = MSG_RSP_MC_SERVER_ERROR;
                        break;
                    }

                    break;
                }

                switch (r->type) {
                case MSG_UNKNOWN:
                    goto error;

                case MSG_RSP_MC_STORED:
                case MSG_RSP_MC_NOT_STORED:
                case MSG_RSP_MC_EXISTS:
                case MSG_RSP_MC_NOT_FOUND:
                case MSG_RSP_MC_DELETED:
                    state = SW_CRLF;
                    break;

                case MSG_RSP_MC_END:
                    state = SW_CRLF;
                    break;

                case MSG_RSP_MC_VALUE:
                    state = SW_SPACES_BEFORE_KEY;
                    break;

                case MSG_RSP_MC_ERROR:
                    state = SW_CRLF;
                    break;

                case MSG_RSP_MC_CLIENT_ERROR:
                case MSG_RSP_MC_SERVER_ERROR:
                    state = SW_RUNTO_CRLF;
                    break;

                default:
                    NOT_REACHED();
                }

                p = p - 1; /* go back by 1 byte */
            }

            break;

        case SW_SPACES_BEFORE_KEY:
            if (ch != ' ') {
                state = SW_KEY;
                p = p - 1; /* go back by 1 byte */
#if 1 //shenzheng 2015-1-20 replication pool
				//token = NULL;
				r->res_key_token = NULL;
#endif //shenzheng 2015-4-3 replication pool
            }

            break;

        case SW_KEY:
#if 1 //shenzheng 2015-1-20 replication pool
			/*if(token == NULL)
			{
				token = p;
			}
			*/
			if(r->res_key_token == NULL)
			{
				r->res_key_token = p;
			}
#endif //shenzheng 2015-4-3 replication pool
            if (ch == ' ') {
                /* r->token = NULL; */
                state = SW_SPACES_BEFORE_FLAGS;
#if 1 //shenzheng 2015-1-20 replication pool
				struct keypos *kpos;

				kpos = array_push(r->keys);
				if (kpos == NULL) {
					goto error;
				}
				//ASSERT(token != NULL);
				//ASSERT(token >= b->pos && token < b->last);
				//kpos->start = token;

				ASSERT(r->res_key_token != NULL);
				ASSERT(r->res_key_token >= b->pos && r->res_key_token < b->last);
				kpos->start = r->res_key_token;
				
				kpos->end = p;

				ASSERT(kpos->start >= b->pos && kpos->start < b->last);
				ASSERT(kpos->end >= b->pos && kpos->end < b->last);
				//kpos->end_mbuf = b;
#endif //shenzheng 2015-4-3 replication pool
            }

            break;

        case SW_SPACES_BEFORE_FLAGS:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                state = SW_FLAGS;
                p = p - 1; /* go back by 1 byte */
            }

            break;

        case SW_FLAGS:
            if (r->token == NULL) {
                /* flags_start <- p */
                /* r->token = p; */
            }

            if (isdigit(ch)) {
                /* flags <- flags * 10 + (ch - '0') */
                ;
            } else if (ch == ' ') {
                /* flags_end <- p - 1 */
                /* r->token = NULL; */
                state = SW_SPACES_BEFORE_VLEN;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_VLEN:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1; /* go back by 1 byte */
                state = SW_VLEN;
                r->vlen = 0;
            }

            break;

        case SW_VLEN:
            if (isdigit(ch)) {
                r->vlen = r->vlen * 10 + (uint32_t)(ch - '0');
            } else if (ch == ' ' || ch == CR) {
                /* vlen_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                /* r->token = NULL; */
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RUNTO_VAL:
            switch (ch) {
            case LF:
                /* val_start <- p + 1 */
                state = SW_VAL;
                r->token = NULL;
#if 1 //shenzheng 2015-3-31 replication pool
				//token = NULL;
				r->res_key_token = NULL;
#endif //shenzheng 2015-4-3 replication pool
                break;

            default:
                goto error;
            }

            break;

        case SW_VAL:
            m = p + r->vlen;
            if (m >= b->last) {
                ASSERT(r->vlen >= (uint32_t)(b->last - p));
                r->vlen -= (uint32_t)(b->last - p);
                m = b->last - 1;
                p = m; /* move forward by vlen bytes */
                break;
            }
            switch (*m) {
            case CR:
                /* val_end <- p - 1 */
                p = m; /* move forward by vlen bytes */
                state = SW_VAL_LF;
                break;

            default:
                goto error;
            }

            break;

        case SW_VAL_LF:
            switch (ch) {
            case LF:
                /* state = SW_END; */
                state = SW_RSP_STR;
                break;

            default:
                goto error;
            }

            break;

        case SW_END:
            if (r->token == NULL) {
                if (ch != 'E') {
                    goto error;
                }
                /* end_start <- p */
                r->token = p;
            } else if (ch == CR) {
                /* end_end <- p */
                m = r->token;
                r->token = NULL;

                switch (p - m) {
                case 3:
                    if (str4cmp(m, 'E', 'N', 'D', '\r')) {
                        r->end = m;
                        state = SW_ALMOST_DONE;
                    }
                    break;

                default:
                    goto error;
                }
            }

            break;

        case SW_RUNTO_CRLF:
            switch (ch) {
            case CR:
                if (r->type == MSG_RSP_MC_VALUE) {
                    state = SW_RUNTO_VAL;
                } else {
                    state = SW_ALMOST_DONE;
                }

                break;

            default:
                break;
            }

            break;

        case SW_CRLF:
            switch (ch) {
            case ' ':
                break;

            case CR:
                state = SW_ALMOST_DONE;
                break;

            default:
                goto error;
            }

            break;

        case SW_ALMOST_DONE:
            switch (ch) {
            case LF:
                /* rsp_end <- p */
                goto done;

            default:
                goto error;
            }

            break;

        case SW_SENTINEL:
        default:
            NOT_REACHED();
            break;

        }
    }

    ASSERT(p == b->last);
    r->pos = p;
    r->state = state;

    if (b->last == b->end && r->token != NULL) {
        if (state <= SW_RUNTO_VAL || state == SW_CRLF || state == SW_ALMOST_DONE) {
            r->state = SW_START;

#if 1 //shenzheng 2015-2-4 replication pool
			//if (token != NULL)
			if(r->res_key_token != NULL)
			{
				ASSERT(array_n(r->keys) > 0);
				struct keypos *kpos;
				kpos = array_pop(r->keys);
				ASSERT(kpos != NULL);
				r->res_key_token = NULL;
				//nc_free(kpos);
				//kpos = NULL;
			}
#endif //shenzheng 2015-4-3 replication pool
        }

        r->pos = r->token;
        r->token = NULL;

        r->result = MSG_PARSE_REPAIR;
    } else {
        r->result = MSG_PARSE_AGAIN;
    }

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed rsp %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

done:
    ASSERT(r->type > MSG_UNKNOWN && r->type < MSG_SENTINEL);
    r->pos = p + 1;
    ASSERT(r->pos <= b->last);
    r->state = SW_START;
    r->token = NULL;
    r->result = MSG_PARSE_OK;

#if 1 //shenzheng 2015-4-3 replication pool
	r->res_key_token = NULL;
#endif //shenzheng 2015-4-3 replication pool

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed rsp %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

error:
    r->result = MSG_PARSE_ERROR;
    r->state = state;
    errno = EINVAL;

    log_hexdump(LOG_INFO, b->pos, mbuf_length(b), "parsed bad rsp %"PRIu64" "
                "res %d type %d state %d", r->id, r->result, r->type,
                r->state);
}

static struct array *
get_server_pools(struct context *ctx, bool old)
{
	if((old && ctx->which_pool) || (!old && !ctx->which_pool))
	{
		return &ctx->pool;
	}
	else
	{
		return &ctx->pool_swap;
	}

	return NULL;
}


static struct server_pool *
proxy_adm_find_server_pool(struct array *sps, 
	struct keypos *kp, struct msg *msg, struct conn *conn)
{
	rstatus_t status;
	uint32_t i, nserver_pools;
	struct server_pool *sp;
	struct string pool_name;
	char *contents;

	ASSERT(sps != NULL);
	ASSERT(kp != NULL);
	ASSERT(conn->client && !conn->proxy);
	ASSERT(msg != NULL && !msg->request);

	pool_name.data = kp->start;
	pool_name.len = (uint32_t)(kp->end - kp->start);
	
	nserver_pools = array_n(sps);
	for(i = 0; i < nserver_pools; i ++)
	{
		sp = array_get(sps, i);

		if(0 == string_compare(&sp->name, &pool_name))
		{
			break;
		}
	}

	if(i >= nserver_pools)
	{
		contents = "ERR: pool doesn't exist!";
		status = msg_append_proxy_adm(msg, (uint8_t *)contents, strlen(contents));
	    if (status != NC_OK) {
			conn->err = ENOMEM;
	        return NULL;
	    }

		status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			conn->err = ENOMEM;
			return NULL;
	    }
		return NULL;
	}
	
	return sp;
}

static rstatus_t
proxy_adm_command_help(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg)
{
	rstatus_t status;
	uint32_t nkeys;
	char *contents;
	char *line = "**********************************\x0d\x0a";
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);

	nkeys = array_n(msg->keys);
	ASSERT(nkeys == 0);


	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	contents = " COMMAND  : show_conf\x0d\x0a DESCRIBE : display the conf file\x0d\x0a USAGE    : no args\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }

	contents = " COMMAND  : show_oconf\x0d\x0a DESCRIBE : display the old conf file if conf reloaded\x0d\x0a USAGE    : no args\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }

	contents = " COMMAND  : show_pools\x0d\x0a DESCRIBE : display all pools name\x0d\x0a USAGE    : no args\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }

	contents = " COMMAND  : show_pool\x0d\x0a DESCRIBE : display one pool's info\x0d\x0a USAGE    : show_pool poolname\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }

	contents = " COMMAND  : show_servers\x0d\x0a DESCRIBE : display one pool's servers info\x0d\x0a USAGE    : show_servers poolname\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }

	contents = " COMMAND  : find_key\x0d\x0a DESCRIBE : display a server which the key is on\x0d\x0a USAGE    : find_key poolname key\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }

	contents = " COMMAND  : find_keys\x0d\x0a DESCRIBE : display servers which the keys are on\x0d\x0a USAGE    : find_key poolname key1 key2 ...\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }

	contents = " COMMAND  : reload_conf\x0d\x0a DESCRIBE : reload the nutcracker conf file\x0d\x0a USAGE    : no args\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }

#if 1 //shenzheng 2015-6-15 zookeeper
#ifdef NC_ZOOKEEPER
	contents = " COMMAND  : set_watch\x0d\x0a DESCRIBE : set watch in zookeeper\x0d\x0a USAGE    : set_watch watch_name watch_path [zk_servers]\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }

	contents = " COMMAND  : del_watch\x0d\x0a DESCRIBE : delete watch from zookeeper\x0d\x0a USAGE    : del_watch watch_name\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }

	contents = " COMMAND  : reset_watch\x0d\x0a DESCRIBE : reset watch in zookeeper\x0d\x0a USAGE    : reset_watch watch_name watch_path [zk_servers]\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }

	contents = " COMMAND  : show_watch\x0d\x0a DESCRIBE : show watch in zookeeper\x0d\x0a USAGE    : show_watch watch_name\x0d\x0a";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
	status = msg_append_proxy_adm(pmsg, (uint8_t *)line, strlen(line));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		return status;
    }
#endif
#endif //shenzheng 2015-6-15 zookeeper

	return NC_OK;
}


static rstatus_t
proxy_adm_command_show_conf(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg, bool old)
{
	rstatus_t status;
	uint32_t i, nkeys, nserver_pools;
	struct server_pool *sp;
	struct array *pools;
	char *contents;
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);

	nkeys = array_n(msg->keys);
	ASSERT(nkeys == 0);

	pools = get_server_pools(ctx, old);

	ASSERT(pools != NULL);

	log_debug(LOG_DEBUG, "old:%d which_pool:%d", old, ctx->which_pool);

	if(pools->nelem == 0)
	{
		contents = "pools is NULL!";
		status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	    if (status != NC_OK) {
			conn->err = ENOMEM;
	        return status;
	    }
	
		status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			conn->err = ENOMEM;
	        return status;
	    }
		return NC_OK;
	}
	
	nserver_pools = array_n(pools);
	for(i = 0; i < nserver_pools; i ++)
	{
		sp = array_get(pools, i);

		status = msg_append_server_pool_info(sp, pmsg);
	    if (status != NC_OK) {
			conn->err = status;
	        return status;
	    }

		status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			conn->err = ENOMEM;
	        return status;
	    }
	}

	return NC_OK;
}

static rstatus_t
proxy_adm_command_show_pools(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg)
{
	rstatus_t status;
	uint32_t i, nkeys, nserver_pools;
	struct server_pool *sp;
	struct array *pools;
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);

	nkeys = array_n(msg->keys);
	ASSERT(nkeys == 0);

	pools = get_server_pools(ctx, false);

	nserver_pools = array_n(pools);
	for(i = 0; i < nserver_pools; i ++)
	{
		sp = array_get(pools, i);

		status = msg_append_proxy_adm(pmsg, sp->name.data, sp->name.len);
	    if (status != NC_OK) {
	        conn->err = ENOMEM;
	        return status;
	    }

		status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			conn->err = ENOMEM;
	        return status;
	    }
	}

	return NC_OK;
}

static rstatus_t
proxy_adm_command_show_pool(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg)
{
	rstatus_t status;
	uint32_t nkeys;
	struct server_pool *sp;
	struct array *pools;
	struct keypos *kp;
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);

	nkeys = array_n(msg->keys);
	ASSERT(nkeys == 1);

	pools = get_server_pools(ctx, false);

	kp = array_get(msg->keys, 0);

	sp = proxy_adm_find_server_pool(pools, kp, pmsg, conn);
	if(sp == NULL)
	{
		if(conn->err)
		{
			return NC_ERROR;
		}
		else
		{
			return NC_OK;
		}
	}

	status = msg_append_server_pool_info(sp, pmsg);
    if (status != NC_OK) {
		conn->err = ENOMEM;
        return status;
    }

	return NC_OK;
}

static rstatus_t
proxy_adm_command_show_servers(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg)
{
	rstatus_t status;
	uint32_t nkeys;
	struct server_pool *sp;
	struct array *pools;
	struct keypos *kp;
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);

	nkeys = array_n(msg->keys);
	ASSERT(nkeys == 1);

	pools = get_server_pools(ctx, false);

	kp = array_get(msg->keys, 0);

	sp = proxy_adm_find_server_pool(pools, kp, pmsg, conn);
	if(sp == NULL)
	{
		if(conn->err)
		{
			return NC_ERROR;
		}
		else
		{
			return NC_OK;
		}
	}

	status = msg_append_servers_info(sp, pmsg);
    if (status != NC_OK) {
		conn->err = ENOMEM;
        return status;
    }

	return NC_OK;
}

static rstatus_t
proxy_adm_command_find_key(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg)
{
	rstatus_t status;
	uint32_t nkeys;
	struct server_pool *sp;
	struct array *pools;
	struct server *server;
	struct keypos *kp;
	uint32_t idx;
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);

	nkeys = array_n(msg->keys);
	ASSERT(nkeys == 2);

	pools = get_server_pools(ctx, false);

	kp = array_get(msg->keys, 0);

	sp = proxy_adm_find_server_pool(pools, kp, pmsg, conn);
	if(sp == NULL)
	{
		if(conn->err)
		{
			return NC_ERROR;
		}
		else
		{
			return NC_OK;
		}
	}

	kp = array_get(msg->keys, 1);

	idx = server_pool_idx(sp, kp->start, (uint32_t)(kp->end - kp->start));
    server = array_get(&sp->server, idx);

	status = msg_append_proxy_adm(pmsg, server->pname.data, server->pname.len);
    if (status != NC_OK) {
        conn->err = ENOMEM;
        return status;
    }

	status = msg_append_proxy_adm(pmsg, (uint8_t *)" ", 1);
    if (status != NC_OK) {
		return status;
    }

	status = msg_append_proxy_adm(pmsg, server->name.data, server->name.len);
    if (status != NC_OK) {
		return status;
    }

	status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		conn->err = ENOMEM;
        return status;
    }

	return NC_OK;
}

static rstatus_t
proxy_adm_command_find_keys(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg)
{
	rstatus_t status;
	uint32_t i, nkeys;
	struct server_pool *sp;
	struct array *pools;
	struct server *server;
	struct keypos *kp;
	uint32_t idx;
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);

	nkeys = array_n(msg->keys);
	ASSERT(nkeys >= 2);

	pools = get_server_pools(ctx, false);

	kp = array_get(msg->keys, 0);
	
	sp = proxy_adm_find_server_pool(pools, kp, pmsg, conn);
	if(sp == NULL)
	{
		if(conn->err)
		{
			return NC_ERROR;
		}
		else
		{
			return NC_OK;
		}
	}

	for(i = 1; i < nkeys; i ++)
	{
		kp = array_get(msg->keys, i);

		idx = server_pool_idx(sp, kp->start, (uint32_t)(kp->end - kp->start));
		server = array_get(&sp->server, idx);

		status = msg_append_proxy_adm(pmsg, server->pname.data, server->pname.len);
		if (status != NC_OK) {
			conn->err = ENOMEM;
			return status;
		}

		status = msg_append_proxy_adm(pmsg, (uint8_t *)" ", 1);
		if (status != NC_OK) {
			return status;
		}

		status = msg_append_proxy_adm(pmsg, server->name.data, server->name.len);
		if (status != NC_OK) {
			return status;
		}

		status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
		if (status != NC_OK) {
			conn->err = ENOMEM;
			return status;
		}
	}

	return NC_OK;
}

static rstatus_t
proxy_adm_command_reload_conf(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg)
{
	rstatus_t status;
	uint32_t nkeys;
	uint32_t msg_len1, msg_len2;
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);
	
	nkeys = array_n(msg->keys);
	ASSERT(nkeys == 0);
	
	msg_len1 = pmsg->mlen;
	
	status = conf_reload(ctx, conn, CONF_PARSE_FILE, NULL, pmsg);
	
	msg_len2 = pmsg->mlen;
	if(msg_len1 == msg_len2)
	{
		status = NC_ENOMEM;
	}
	else
	{
		status = NC_OK;
	}

	return status;
}

#if 1 //shenzheng 2015-6-15 zookeeper
#ifdef NC_ZOOKEEPER
static rstatus_t
proxy_adm_command_set_watch(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg)
{
	rstatus_t status;
	uint32_t nkeys;
	char *contents;
	void *zkhandle = NULL;
	struct server_pool *sp;
	struct array *pools;
	struct keypos *kp;
	struct string watch_name, watch_path, zk_servers, str_tmp;
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);

	nkeys = array_n(msg->keys);
	ASSERT(nkeys == 2 || nkeys == 3);

	kp = array_get(msg->keys, 0);
	watch_name.data = kp->start;
	watch_name.len = (uint32_t)(kp->end - kp->start);

	kp = array_get(msg->keys, 1);
	string_init(&watch_path);
	status = string_copy(&watch_path, kp->start, (uint32_t)(kp->end - kp->start));
	if(status != NC_OK)
	{
		goto error;
	}

	string_init(&zk_servers);
	if(nkeys > 2)
	{
		kp = array_get(msg->keys, 2);
		status = string_copy(&zk_servers, kp->start, (uint32_t)(kp->end - kp->start));
		if(status != NC_OK)
		{
			goto error;
		}
	}

	str_tmp.data = "conf";
	str_tmp.len = 4;

	if(string_compare(&watch_name, &str_tmp) != 0)
	{
		contents = "ERR: by now watch_name must be conf.";
		goto done;
	}

	if(nkeys > 2)
	{
		if(ctx->zkhandle != NULL && string_compare(&zk_servers, &ctx->zk_servers) == 0)
		{
			zkhandle = ctx->zkhandle;
		}
		else
		{
			zkhandle = zk_init(zk_servers.data);
		}
	}
	else if(ctx->zkhandle == NULL)
	{
		if(string_empty(&ctx->zk_servers))
		{
			contents = "ERR: need argument zk_servers.";
    		goto done;
		}
		else
		{
			zkhandle = zk_init(ctx->zk_servers.data);
		}
	}
	else
	{
		zkhandle = ctx->zkhandle;
	}

	if(zkhandle == NULL)
	{
		contents = "ERR: init zkhandle error.";
		goto done;
	}

	status = conf_keep_from_zk(ctx, zkhandle, watch_path.data);
	if(status != NC_OK)
	{
		if(zkhandle != NULL)
		{
			zk_close(zkhandle);
			if(ctx->zkhandle == zkhandle)
			{
				ctx->zkhandle = NULL;
			}
			zkhandle = NULL;
		}

		contents = "ERR: conf keep error.";
		goto done;
	}

	if(ctx->zkhandle != zkhandle)
	{
		if(ctx->zkhandle != NULL)
		{
			zk_close(ctx->zkhandle);
		}
		ctx->zkhandle = zkhandle;

		if(nkeys > 2)
		{
			if(!string_empty(&ctx->zk_servers))
			{
				string_deinit(&ctx->zk_servers);
			}
			status = string_copy(&ctx->zk_servers, zk_servers.data, zk_servers.len);
			if(status != NC_OK)
			{
				goto error;
			}
		}
	}
	
	contents = "set watch success.";
	
done:
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
    if (status != NC_OK) {
		conn->err = ENOMEM;
        goto error;
    }

	status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}

	string_deinit(&watch_path);
	string_deinit(&zk_servers);
	return NC_OK;

error:

	string_deinit(&watch_path);
	string_deinit(&zk_servers);
	return status;
}

static rstatus_t
proxy_adm_command_del_watch(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg)
{
	rstatus_t status;
	uint32_t nkeys;
	char *contents;
	struct server_pool *sp;
	struct array *pools;
	struct keypos *kp;
	struct string watch_name, str_tmp;
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);

	nkeys = array_n(msg->keys);
	ASSERT(nkeys == 1);

	kp = array_get(msg->keys, 0);
	watch_name.data = kp->start;
	watch_name.len = (uint32_t)(kp->end - kp->start);

	str_tmp.data = "conf";
	str_tmp.len = 4;	
	if(string_compare(&watch_name, &str_tmp) != 0)
	{
		contents = "ERR: by now watch_name must be conf.";
		goto done;
	}

	if(ctx->zkhandle == NULL)
	{
		contents = "conf watch didn't exist.";
		goto done;
	}
	else
	{
		zk_close(ctx->zkhandle);
		ctx->zkhandle = NULL;

		contents = "conf watch delete success.";
	}
	
done:
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}

	status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}

	return NC_OK;

error:

	return status;

}

static rstatus_t
proxy_adm_command_reset_watch(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg)
{
	rstatus_t status;
	uint32_t nkeys;
	char *contents;
	void *zkhandle = NULL;
	struct server_pool *sp;
	struct array *pools;
	struct keypos *kp;
	struct string watch_name, watch_path, zk_servers, str_tmp;
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);

	nkeys = array_n(msg->keys);
	ASSERT(nkeys == 2 || nkeys == 3);
	log_debug(LOG_DEBUG, "nkeys : %d", nkeys);
	if(nkeys > 2)
	{
		kp = array_get(msg->keys, 2);
		log_debug(LOG_DEBUG, "last kp : %s", kp->start);
	}

	kp = array_get(msg->keys, 0);
	watch_name.data = kp->start;
	watch_name.len = (uint32_t)(kp->end - kp->start);
	log_debug(LOG_DEBUG, "first kp : %s", kp->start);

	kp = array_get(msg->keys, 1);
	log_debug(LOG_DEBUG, "second kp : %s", kp->start);
	string_init(&watch_path);
	status = string_copy(&watch_path, kp->start, (uint32_t)(kp->end - kp->start));
	if(status != NC_OK)
	{
		goto error;
	}

	string_init(&zk_servers);
	if(nkeys > 2)
	{
		kp = array_get(msg->keys, 2);
		status = string_copy(&zk_servers, kp->start, (uint32_t)(kp->end - kp->start));
		if(status != NC_OK)
		{
			goto error;
		}
	}

	str_tmp.data = "conf";
	str_tmp.len = 4;
	if(string_compare(&watch_name, &str_tmp) != 0)
	{
		contents = "ERR: by now watch_name must be conf.";
		goto done;
	}

	if(nkeys > 2)
	{
		zkhandle = zk_init(zk_servers.data);
	}
	else
	{
		if(string_empty(&ctx->zk_servers))
		{
			contents = "ERR: need argument zk_servers.";
			goto done;
		}
		zkhandle = zk_init(ctx->zk_servers.data);
	}
	
	if(zkhandle == NULL)
	{
		contents = "ERR: init zkhandle error.";
		goto done;
	}

	status = conf_keep_from_zk(ctx, zkhandle, watch_path.data);
	if(status != NC_OK)
	{
		if(zkhandle != NULL)
		{
			zk_close(zkhandle);
			zkhandle = NULL;
		}

		contents = "ERR: conf keep error.";
		goto done;
	}

	if(ctx->zkhandle != NULL)
	{
		zk_close(ctx->zkhandle);
		ctx->zkhandle = NULL;
	}

	ctx->zkhandle = zkhandle;

	if(nkeys > 2)
	{
		if(!string_empty(&ctx->zk_servers))
		{
			string_deinit(&ctx->zk_servers);
		}
		status = string_copy(&ctx->zk_servers, zk_servers.data, zk_servers.len);
		if(status != NC_OK)
		{
			goto error;
		}
	}
	
	contents = "reset watch success.";
done:
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
    if (status != NC_OK) {
		conn->err = ENOMEM;
        goto error;
    }

	status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}

	string_deinit(&watch_path);
	string_deinit(&zk_servers);
	return NC_OK;

error:

	string_deinit(&watch_path);
	string_deinit(&zk_servers);
	return status;
}

static rstatus_t
proxy_adm_command_show_watch(struct context *ctx, 
	struct conn *conn, struct msg * msg, struct msg * pmsg)
{
	rstatus_t status;
	uint32_t nkeys;
	char *key, *contents;
	struct server_pool *sp;
	struct array *pools;
	struct keypos *kp;
	struct string watch_name, str_tmp;
	
	ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
	ASSERT(pmsg != NULL && !pmsg->request);
    ASSERT(msg->owner == conn);
	ASSERT(conn->owner == ctx);

	nkeys = array_n(msg->keys);
	ASSERT(nkeys == 1);

	kp = array_get(msg->keys, 0);
	watch_name.data = kp->start;
	watch_name.len = (uint32_t)(kp->end - kp->start);

	str_tmp.data = "conf";
	str_tmp.len = 4;
	if(string_compare(&watch_name, &str_tmp) != 0)
	{
		contents = "ERR: by now watch_name must be conf.";
		goto command_error;
	}

	key = "zk_servers  : ";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}
	if(ctx->zk_servers.len > 0)
	{
		status = msg_append_proxy_adm(pmsg, ctx->zk_servers.data, ctx->zk_servers.len);
		if (status != NC_OK) {
			conn->err = ENOMEM;
			goto error;
		}
	}
	status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}

	key = "watch_path  : ";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}
	if(ctx->watch_path.len > 0)
	{
		status = msg_append_proxy_adm(pmsg, ctx->watch_path.data, ctx->watch_path.len);
		if (status != NC_OK) {
			conn->err = ENOMEM;
			goto error;
		}
	}
	status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}

	key = "watch_state : ";
	status = msg_append_proxy_adm(pmsg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}
	if(ctx->zkhandle == NULL)
	{
		contents = "no";
	}
	else
	{
		contents = "yes";
	}
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}
	status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}

	return NC_OK;
	
command_error:
	status = msg_append_proxy_adm(pmsg, (uint8_t *)contents, strlen(contents));
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}

	status = msg_append_proxy_adm(pmsg, (uint8_t *)CRLF, CRLF_LEN);
	if (status != NC_OK) {
		conn->err = ENOMEM;
		goto error;
	}

	return NC_OK;

error:

	return status;

}

#endif
#endif //shenzheng 2015-6-15 zookeeper

struct msg *
proxy_adm_make_response(struct context *ctx, struct conn *conn, struct msg * req)
{
	rstatus_t status;
	struct msg * res;

	ASSERT(conn->client && !conn->proxy);
    ASSERT(req->request);
    ASSERT(req->owner == conn);
	ASSERT(conn->owner == ctx);
	
	res = msg_get_proxy_adm(conn, false);
	if(res == NULL)
	{
		conn->err = errno;
		return NULL;
	}

	log_debug(LOG_DEBUG, "req->type : %d", req->type);

	switch(req->type)
	{
	case MSG_REQ_PROXY_ADM_HELP:
		status = proxy_adm_command_help(ctx, conn, req, res);
		break;
		
	case MSG_REQ_PROXY_ADM_SHOW_CONF:
		pthread_mutex_lock(&ctx->reload_lock);
		status = proxy_adm_command_show_conf(ctx, conn, req, res, false);
		pthread_mutex_unlock(&ctx->reload_lock);
		break;
		
	case MSG_REQ_PROXY_ADM_SHOW_OCONF:
		pthread_mutex_lock(&ctx->reload_lock);
		status = proxy_adm_command_show_conf(ctx, conn, req, res, true);
		pthread_mutex_unlock(&ctx->reload_lock);
		break;
		
	case MSG_REQ_PROXY_ADM_SHOW_POOLS:
		pthread_mutex_lock(&ctx->reload_lock);
		status = proxy_adm_command_show_pools(ctx, conn, req, res);
		pthread_mutex_unlock(&ctx->reload_lock);
		break;
		
	case MSG_REQ_PROXY_ADM_SHOW_POOL:
		pthread_mutex_lock(&ctx->reload_lock);
		status = proxy_adm_command_show_pool(ctx, conn, req, res);
		pthread_mutex_unlock(&ctx->reload_lock);
		break;
		
	case MSG_REQ_PROXY_ADM_SHOW_SERVERS:
		pthread_mutex_lock(&ctx->reload_lock);
		status = proxy_adm_command_show_servers(ctx, conn, req, res);
		pthread_mutex_unlock(&ctx->reload_lock);
		break;
		
	case MSG_REQ_PROXY_ADM_FIND_KEY:
		pthread_mutex_lock(&ctx->reload_lock);
		status = proxy_adm_command_find_key(ctx, conn, req, res);
		pthread_mutex_unlock(&ctx->reload_lock);
		break;
		
	case MSG_REQ_PROXY_ADM_FIND_KEYS:
		pthread_mutex_lock(&ctx->reload_lock);
		status = proxy_adm_command_find_keys(ctx, conn, req, res);
		pthread_mutex_unlock(&ctx->reload_lock);
		break;
		
	case MSG_REQ_PROXY_ADM_RELOAD_CONF:
		status = proxy_adm_command_reload_conf(ctx, conn, req, res);
		break;
		
#if 1 //shenzheng 2015-6-15 zookeeper
#ifdef NC_ZOOKEEPER
	case MSG_REQ_PROXY_ADM_SET_WATCH:
		status = proxy_adm_command_set_watch(ctx, conn, req, res);
		break;
	case MSG_REQ_PROXY_ADM_DEL_WATCH:
		status = proxy_adm_command_del_watch(ctx, conn, req, res);
		break;
	case MSG_REQ_PROXY_ADM_RESET_WATCH:
		status = proxy_adm_command_reset_watch(ctx, conn, req, res);
		break;
	case MSG_REQ_PROXY_ADM_SHOW_WATCH:
		status = proxy_adm_command_show_watch(ctx, conn, req, res);
		break;
#endif
#endif //shenzheng 2015-6-15 zookeeper

	default:
		msg_put_proxy_adm(res);
		conn->err = ENOENT;
		return NULL;
		
		break;
	}

	if(status != NC_OK)
	{
		msg_put_proxy_adm(res);
		return NULL;
	}

	return res;
}

#endif //shenzheng 2015-4-27 proxy administer

#if 1 //shenzheng 2015-7-9 config-reload
static void
proxy_close_for_reload(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(!conn->client && conn->proxy);

    if (conn->sd < 0) {
        conn->unref(conn);
	    conn_put_for_reload(conn);
        return;
    }

    ASSERT(conn->rmsg == NULL);
    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("close p %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put_for_reload(conn);
}

rstatus_t
proxy_init_for_reload(struct server_pool *pool)
{
    rstatus_t status;
    struct conn *p;

    p = conn_get_proxy_for_reload(pool);
    if (p == NULL) {
        return NC_ENOMEM;
    }

	/* to avoid multi-thread core dump */
	p->recv_active = 1;

    status = proxy_listen(pool->ctx, p);
    if (status != NC_OK) {
        //p->close(pool->ctx, p);
		proxy_close_for_reload(pool->ctx, p);
        return status;
    }

    log_debug(LOG_NOTICE, "p %d listening on '%.*s' in %s pool %"PRIu32" '%.*s'"
              " with %"PRIu32" servers", p->sd, pool->addrstr.len,
              pool->addrstr.data, pool->redis ? "redis" : "memcache",
              pool->idx, pool->name.len, pool->name.data,
              array_n(&pool->server));

    return NC_OK;
}

rstatus_t
proxy_each_deinit_for_reload(void *elem, void *data)
{
    struct server_pool *pool = elem;
    struct conn *p;

    p = pool->p_conn;
    if (p != NULL) {
        //p->close(pool->ctx, p);
		proxy_close_for_reload(pool->ctx, p);
    }

    return NC_OK;
}

#endif //shenzheng 2015-7-9 config-reload

