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
#include <nc_server.h>
#include <nc_conf.h>

void
server_ref(struct conn *conn, void *owner)
{
    struct server *server = owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->owner == NULL);

    conn->family = server->family;
    conn->addrlen = server->addrlen;
    conn->addr = server->addr;

    server->ns_conn_q++;
    TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

    conn->owner = owner;

    log_debug(LOG_VVERB, "ref conn %p owner %p into '%.*s", conn, server,
              server->pname.len, server->pname.data);
}

void
server_unref(struct conn *conn)
{
    struct server *server;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->owner != NULL);

    server = conn->owner;
    conn->owner = NULL;

    ASSERT(server->ns_conn_q != 0);
    server->ns_conn_q--;
    TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);

    log_debug(LOG_VVERB, "unref conn %p owner %p from '%.*s'", conn, server,
              server->pname.len, server->pname.data);
}

int
server_timeout(struct conn *conn)
{
    struct server *server;
    struct server_pool *pool;

    ASSERT(!conn->client && !conn->proxy);

    server = conn->owner;
    pool = server->owner;

    return pool->timeout;
}

bool
server_active(struct conn *conn)
{
    ASSERT(!conn->client && !conn->proxy);

    if (!TAILQ_EMPTY(&conn->imsg_q)) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (conn->rmsg != NULL) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (conn->smsg != NULL) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    log_debug(LOG_VVERB, "s %d is inactive", conn->sd);

    return false;
}

static rstatus_t
server_each_set_owner(void *elem, void *data)
{
    struct server *s = elem;
    struct server_pool *sp = data;

    s->owner = sp;

    return NC_OK;
}

rstatus_t
server_init(struct array *server, struct array *conf_server,
            struct server_pool *sp)
{
    rstatus_t status;
    uint32_t nserver;

    nserver = array_n(conf_server);
    ASSERT(nserver != 0);
    ASSERT(array_n(server) == 0);

    status = array_init(server, nserver, sizeof(struct server));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf server to server */
    status = array_each(conf_server, conf_server_each_transform, server);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }
    ASSERT(array_n(server) == nserver);

    /* set server owner */
    status = array_each(server, server_each_set_owner, sp);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }

    log_debug(LOG_DEBUG, "init %"PRIu32" servers in pool %"PRIu32" '%.*s'",
              nserver, sp->idx, sp->name.len, sp->name.data);

    return NC_OK;
}

void
server_deinit(struct array *server)
{
    uint32_t i, nserver;

    for (i = 0, nserver = array_n(server); i < nserver; i++) {
        struct server *s;

        s = array_pop(server);
        ASSERT(TAILQ_EMPTY(&s->s_conn_q) && s->ns_conn_q == 0);
    }
    array_deinit(server);
}

struct conn *
server_conn(struct server *server)
{
    struct server_pool *pool;
    struct conn *conn;

    pool = server->owner;

    /*
     * FIXME: handle multiple server connections per server and do load
     * balancing on it. Support multiple algorithms for
     * 'server_connections:' > 0 key
     */

    if (server->ns_conn_q < pool->server_connections) {
        return conn_get(server, false, pool->redis);
    }
    ASSERT(server->ns_conn_q == pool->server_connections);

    /*
     * Pick a server connection from the head of the queue and insert
     * it back into the tail of queue to maintain the lru order
     */
    conn = TAILQ_FIRST(&server->s_conn_q);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);
    TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

    return conn;
}

static rstatus_t
server_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server *server;
    struct server_pool *pool;
    struct conn *conn;

    server = elem;
    pool = server->owner;

    conn = server_conn(server);
    if (conn == NULL) {
        return NC_ENOMEM;
    }

    status = server_connect(pool->ctx, server, conn);
    if (status != NC_OK) {
        log_warn("connect to server '%.*s' failed, ignored: %s",
                 server->pname.len, server->pname.data, strerror(errno));
        server_close(pool->ctx, conn);
    }

    return NC_OK;
}

static rstatus_t
server_each_disconnect(void *elem, void *data)
{
    struct server *server;
    struct server_pool *pool;

    server = elem;
    pool = server->owner;

    while (!TAILQ_EMPTY(&server->s_conn_q)) {
        struct conn *conn;

        ASSERT(server->ns_conn_q > 0);

        conn = TAILQ_FIRST(&server->s_conn_q);
        conn->close(pool->ctx, conn);
    }

    return NC_OK;
}

static void
server_failure(struct context *ctx, struct server *server)
{
    struct server_pool *pool = server->owner;
    int64_t now, next;
    rstatus_t status;

    if (!pool->auto_eject_hosts) {
        return;
    }

    server->failure_count++;

    log_debug(LOG_VERB, "server '%.*s' failure count %"PRIu32" limit %"PRIu32,
              server->pname.len, server->pname.data, server->failure_count,
              pool->server_failure_limit);

    if (server->failure_count < pool->server_failure_limit) {
        return;
    }

    now = nc_usec_now();
    if (now < 0) {
        return;
    }

    stats_server_set_ts(ctx, server, server_ejected_at, now);

    next = now + pool->server_retry_timeout;

    log_debug(LOG_INFO, "update pool %"PRIu32" '%.*s' to delete server '%.*s' "
              "for next %"PRIu32" secs", pool->idx, pool->name.len,
              pool->name.data, server->pname.len, server->pname.data,
              pool->server_retry_timeout / 1000 / 1000);

    stats_pool_incr(ctx, pool, server_ejects);

    server->failure_count = 0;
    server->next_retry = next;

    status = server_pool_run(pool);
    if (status != NC_OK) {
        log_error("updating pool %"PRIu32" '%.*s' failed: %s", pool->idx,
                  pool->name.len, pool->name.data, strerror(errno));
    }
}

static void
server_close_stats(struct context *ctx, struct server *server, err_t err,
                   unsigned eof, unsigned connected)
{
    if (connected) {
        stats_server_decr(ctx, server, server_connections);
    }

    if (eof) {
        stats_server_incr(ctx, server, server_eof);
        return;
    }

    switch (err) {
    case ETIMEDOUT:
        stats_server_incr(ctx, server, server_timedout);
        break;
    case EPIPE:
    case ECONNRESET:
    case ECONNABORTED:
    case ECONNREFUSED:
    case ENOTCONN:
    case ENETDOWN:
    case ENETUNREACH:
    case EHOSTDOWN:
    case EHOSTUNREACH:
    default:
        stats_server_incr(ctx, server, server_err);
        break;
    }
}

void
server_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */
    struct conn *c_conn;    /* peer client connection */

    ASSERT(!conn->client && !conn->proxy);
	
    server_close_stats(ctx, conn->owner, conn->err, conn->eof,
                       conn->connected);

    if (conn->sd < 0) {		
        server_failure(ctx, conn->owner);
        conn->unref(conn);
        conn_put(conn);
        return;
    }

    for (msg = TAILQ_FIRST(&conn->imsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from server inq */
        conn->dequeue_inq(ctx, conn, msg);

        /*
         * Don't send any error response, if
         * 1. request is tagged as noreply or,
         * 2. client has already closed its connection
         */
        if (msg->swallow || msg->noreply) {
            log_debug(LOG_INFO, "close s %d swallow req %"PRIu64" len %"PRIu32
                      " type %d", conn->sd, msg->id, msg->mlen, msg->type);

			req_put(msg);
        } else {

#if 1 //shenzheng 2015-4-20 replication pool		
#else //shenzheng 2015-4-20 replication pool
            c_conn = msg->owner;
            ASSERT(c_conn->client && !c_conn->proxy);
#endif //shenzheng 2015-4-20 replication pool
			msg->error = 1;
            msg->err = conn->err;
#if 1 //shenzheng 2015-1-16 replication pool
			if(msg->frag_id)
			{
				if(req_need_penetrate(msg, msg->peer))
				{
					req_penetrate(ctx, conn, msg, msg->peer);
					continue;
				}
				else
				{
#endif //shenzheng 2015-1-16 replication pool
            msg->done = 1;
#if 1 //shenzheng 2015-1-16 replication pool
				}
			}
			else 
			{
				if(msg->server_pool_id == -1)
				{
					msg->self_done = 1;
					if(msg_pass(msg))
					{
						msg->done = 1;
						msg->handle_result(ctx, conn, msg);
					}
				}
				else if(msg->server_pool_id == -2)
				{
					msg->self_done = 1;
					msg->handle_result(ctx, conn, msg);
					req_put(msg);
					continue;
				}
				else
				{
					msg->self_done = 1;
					struct msg *master_msg = msg->master_msg;
					ASSERT(master_msg->nreplication_msgs > 0);
					ASSERT(master_msg->replication_msgs != NULL);
					if(msg_pass(master_msg))
					{
						master_msg->done = 1;
						master_msg->handle_result(ctx, conn, master_msg);
						if(master_msg->master_send)
						{
							req_put(master_msg);
							continue;
						}
					}
				}
			}
#endif //shenzheng 2015-3-10 replication pool
            

            if (msg->frag_owner != NULL) {
                msg->frag_owner->nfrag_done++;
            }
			
#if 1 //shenzheng 2015-4-20 replication pool
			c_conn = msg->owner;
			ASSERT(c_conn->client && !c_conn->proxy);
#endif //shenzheng 2015-4-20 replication pool

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {

                event_add_out(ctx->evb, msg->owner);
            }

            log_debug(LOG_INFO, "close s %d schedule error for req %"PRIu64" "
                      "len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
                      msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
                      conn->err ? strerror(conn->err): " ");
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from server outq */
        conn->dequeue_outq(ctx, conn, msg);

        if (msg->swallow) {
            log_debug(LOG_INFO, "close s %d swallow req %"PRIu64" len %"PRIu32
                      " type %d", conn->sd, msg->id, msg->mlen, msg->type);
			req_put(msg);
        } else {
#if 1 //shenzheng 2015-4-20 replication pool		
#else //shenzheng 2015-4-20 replication pool
            c_conn = msg->owner;
            ASSERT(c_conn->client && !c_conn->proxy);
#endif //shenzheng 2015-4-20 replication pool

			msg->error = 1;
            msg->err = conn->err;
#if 1 //shenzheng 2015-1-16 replication pool
			if(msg->frag_id)
			{
			
				if(req_need_penetrate(msg, msg->peer))
				{
					req_penetrate(ctx, conn, msg, msg->peer);
					continue;
				}
				else
				{
#endif //shenzheng 2015-4-8 replication pool
			msg->done = 1;
#if 1 //shenzheng 2015-4-8 replication pool
				}
			}
			else 
			{
				if(msg->server_pool_id == -1)
				{
					msg->self_done = 1;
					if(msg_pass(msg))
					{
						msg->done = 1;
						msg->handle_result(ctx, conn, msg);
					}
				}
				else if(msg->server_pool_id == -2)
				{
					msg->self_done = 1;
					msg->handle_result(ctx, conn, msg);
					req_put(msg);
					continue;
				}
				else
				{
					msg->self_done = 1;
					struct msg *master_msg = msg->master_msg;
					ASSERT(master_msg->nreplication_msgs > 0);
					ASSERT(master_msg->replication_msgs != NULL);
					if(msg_pass(master_msg))
					{
						master_msg->done = 1;
						master_msg->handle_result(ctx, conn, master_msg);
						if(master_msg->master_send)
						{
							req_put(master_msg);
							continue;
						}
					}
				}
			}
#endif //shenzheng 2015-3-10 replication pool
			
            if (msg->frag_owner != NULL) {
                msg->frag_owner->nfrag_done++;
            }

#if 1 //shenzheng 2015-4-20 replication pool
			c_conn = msg->owner;
			ASSERT(c_conn->client && !c_conn->proxy);
#endif //shenzheng 2015-4-20 replication pool

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {

                event_add_out(ctx->evb, msg->owner);
            }

            log_debug(LOG_INFO, "close s %d schedule error for req %"PRIu64" "
                      "len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
                      msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
                      conn->err ? strerror(conn->err): " ");
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));


    msg = conn->rmsg;
    if (msg != NULL) {
        conn->rmsg = NULL;

        ASSERT(!msg->request);
        ASSERT(msg->peer == NULL);

        rsp_put(msg);

        log_debug(LOG_INFO, "close s %d discarding rsp %"PRIu64" len %"PRIu32" "
                  "in error", conn->sd, msg->id, msg->mlen);
    }

    ASSERT(conn->smsg == NULL);

    server_failure(ctx, conn->owner);

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("close s %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put(conn);
}

rstatus_t
server_connect(struct context *ctx, struct server *server, struct conn *conn)
{
    rstatus_t status;

    ASSERT(!conn->client && !conn->proxy);

    if (conn->sd > 0) {
        /* already connected on server connection */
        return NC_OK;
    }

    log_debug(LOG_VVERB, "connect to server '%.*s'", server->pname.len,
              server->pname.data);

    conn->sd = socket(conn->family, SOCK_STREAM, 0);
    if (conn->sd < 0) {
        log_error("socket for server '%.*s' failed: %s", server->pname.len,
                  server->pname.data, strerror(errno));
        status = NC_ERROR;
        goto error;
    }

    status = nc_set_nonblocking(conn->sd);
    if (status != NC_OK) {
        log_error("set nonblock on s %d for server '%.*s' failed: %s",
                  conn->sd, server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    if (server->pname.data[0] != '/') {
        status = nc_set_tcpnodelay(conn->sd);
        if (status != NC_OK) {
            log_warn("set tcpnodelay on s %d for server '%.*s' failed, ignored: %s",
                     conn->sd, server->pname.len, server->pname.data,
                     strerror(errno));
        }
    }

    status = event_add_conn(ctx->evb, conn);
    if (status != NC_OK) {
        log_error("event add conn s %d for server '%.*s' failed: %s",
                  conn->sd, server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    ASSERT(!conn->connecting && !conn->connected);

    status = connect(conn->sd, conn->addr, conn->addrlen);
    if (status != NC_OK) {
        if (errno == EINPROGRESS) {
            conn->connecting = 1;
            log_debug(LOG_DEBUG, "connecting on s %d to server '%.*s'",
                      conn->sd, server->pname.len, server->pname.data);
            return NC_OK;
        }

        log_error("connect on s %d to server '%.*s' failed: %s", conn->sd,
                  server->pname.len, server->pname.data, strerror(errno));

        goto error;
    }

    ASSERT(!conn->connecting);
    conn->connected = 1;
    log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
              server->pname.len, server->pname.data);

    return NC_OK;

error:
    conn->err = errno;
    return status;
}

void
server_connected(struct context *ctx, struct conn *conn)
{
    struct server *server = conn->owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->connecting && !conn->connected);

    stats_server_incr(ctx, server, server_connections);

    conn->connecting = 0;
    conn->connected = 1;

    log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
              server->pname.len, server->pname.data);
}

void
server_ok(struct context *ctx, struct conn *conn)
{
    struct server *server = conn->owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->connected);

    if (server->failure_count != 0) {
        log_debug(LOG_VERB, "reset server '%.*s' failure count from %"PRIu32
                  " to 0", server->pname.len, server->pname.data,
                  server->failure_count);
        server->failure_count = 0;
        server->next_retry = 0LL;
    }
}

static rstatus_t
server_pool_update(struct server_pool *pool)
{
    rstatus_t status;
    int64_t now;
    uint32_t pnlive_server; /* prev # live server */

    if (!pool->auto_eject_hosts) {
        return NC_OK;
    }

    if (pool->next_rebuild == 0LL) {
        return NC_OK;
    }

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    if (now <= pool->next_rebuild) {
        if (pool->nlive_server == 0) {
            errno = ECONNREFUSED;
            return NC_ERROR;
        }
        return NC_OK;
    }

    pnlive_server = pool->nlive_server;

    status = server_pool_run(pool);
    if (status != NC_OK) {
        log_error("updating pool %"PRIu32" with dist %d failed: %s", pool->idx,
                  pool->dist_type, strerror(errno));
        return status;
    }

    log_debug(LOG_INFO, "update pool %"PRIu32" '%.*s' to add %"PRIu32" servers",
              pool->idx, pool->name.len, pool->name.data,
              pool->nlive_server - pnlive_server);


    return NC_OK;
}

static uint32_t
server_pool_hash(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    ASSERT(array_n(&pool->server) != 0);
    ASSERT(key != NULL);

    if (array_n(&pool->server) == 1) {
        return 0;
    }

    if (keylen == 0) {
        return 0;
    }

    return pool->key_hash((char *)key, keylen);
}

uint32_t
server_pool_idx(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    uint32_t hash, idx;

    ASSERT(array_n(&pool->server) != 0);
    ASSERT(key != NULL);

    /*
     * If hash_tag: is configured for this server pool, we use the part of
     * the key within the hash tag as an input to the distributor. Otherwise
     * we use the full key
     */
    if (!string_empty(&pool->hash_tag)) {
        struct string *tag = &pool->hash_tag;
        uint8_t *tag_start, *tag_end;

        tag_start = nc_strchr(key, key + keylen, tag->data[0]);
        if (tag_start != NULL) {
            tag_end = nc_strchr(tag_start + 1, key + keylen, tag->data[1]);
            if ((tag_end != NULL) && (tag_end - tag_start > 1)) {
                key = tag_start + 1;
                keylen = (uint32_t)(tag_end - key);
            }
        }
    }

    switch (pool->dist_type) {
    case DIST_KETAMA:
        hash = server_pool_hash(pool, key, keylen);
        idx = ketama_dispatch(pool->continuum, pool->ncontinuum, hash);
        break;

    case DIST_MODULA:
        hash = server_pool_hash(pool, key, keylen);
        idx = modula_dispatch(pool->continuum, pool->ncontinuum, hash);
        break;

    case DIST_RANDOM:
        idx = random_dispatch(pool->continuum, pool->ncontinuum, 0);
        break;

    default:
        NOT_REACHED();
        return 0;
    }
    ASSERT(idx < array_n(&pool->server));
    return idx;
}

static struct server *
server_pool_server(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    struct server *server;
    uint32_t idx;

    idx = server_pool_idx(pool, key, keylen);
    server = array_get(&pool->server, idx);

    log_debug(LOG_VERB, "key '%.*s' on dist %d maps to server '%.*s'", keylen,
              key, pool->dist_type, server->pname.len, server->pname.data);

    return server;
}

struct conn *
server_pool_conn(struct context *ctx, struct server_pool *pool, uint8_t *key,
                 uint32_t keylen)
{
    rstatus_t status;
    struct server *server;
    struct conn *conn;

    status = server_pool_update(pool);
    if (status != NC_OK) {
        return NULL;
    }

    /* from a given {key, keylen} pick a server from pool */
    server = server_pool_server(pool, key, keylen);

    if (server == NULL) {
        return NULL;
    }

    /* pick a connection to a given server */
    conn = server_conn(server);
    if (conn == NULL) {
        return NULL;
    }

    status = server_connect(ctx, server, conn);
    if (status != NC_OK) {
        server_close(ctx, conn);
        return NULL;
    }

    return conn;
}

#if 1 //shenzheng 2015-6-25 replace server
struct conn *
server_pool_conn_for_replace(struct context *ctx, struct server_pool *pool, struct msg *msg)
{
    rstatus_t status;
	struct sockinfo sock;
    struct server *curr_ser, *new_ser;
    struct conn *conn;
	struct keypos *kp;
	struct string new_ser_addr;
	struct string new_ser_host;
	struct string new_ser_port_str;
	struct string weight_str;
	int new_ser_port;
	uint8_t *p, *q, *last;
	uint32_t i;

	curr_ser = array_get(&pool->server, msg->mser_idx);

	log_debug(LOG_DEBUG, "curr_ser->name : %s", curr_ser->name.data);
	
	new_ser = nc_alloc(sizeof(struct server));

	if (new_ser == NULL) {
        return NULL;
    }

	string_init(&new_ser->pname);
	string_init(&new_ser->name);
	string_init(&weight_str);
	string_init(&new_ser_host);

	kp = array_get(msg->keys, 1);
	new_ser_addr.data = kp->start;
	new_ser_addr.len = (uint32_t)(kp->end - kp->start);
	log_debug(LOG_DEBUG, "new_ser_addr.len : %d", new_ser_addr.len);
	log_debug(LOG_DEBUG, "new_ser_addr.data : %.*s", new_ser_addr.len, new_ser_addr.data);
	
	ASSERT(new_ser_addr.len > 0);

	p = new_ser_addr.data;
	last = new_ser_addr.data + new_ser_addr.len;

	q = nc_strchr(p, last, ':');
	if(q == NULL || q >= last || q <= new_ser_addr.data)
	{
		goto error;
	}

	status = string_copy(&new_ser_host, new_ser_addr.data, (uint32_t)(q - new_ser_addr.data));
	if(status != NC_OK)
	{
		goto error;
	}

	new_ser_port_str.data = q + 1;
	new_ser_port_str.len = (uint32_t)(last - new_ser_port_str.data);

    new_ser_port = nc_atoi(new_ser_port_str.data, new_ser_port_str.len);

	log_debug(LOG_DEBUG, "new_ser_host: %.*s", new_ser_host.len, new_ser_host.data);
	log_debug(LOG_DEBUG, "new_ser_port_str: %.*s", new_ser_port_str.len, new_ser_port_str.data);
	log_debug(LOG_DEBUG, "new_ser_port: %d", new_ser_port);

	new_ser->idx = curr_ser->idx;
	new_ser->owner = curr_ser->owner;

	nc_itos(&weight_str, curr_ser->weight);
	
	new_ser->pname.len = new_ser_addr.len + 1 + weight_str.len;
	new_ser->pname.data = nc_alloc((new_ser->pname.len + 1)*sizeof(uint8_t *));
	p = new_ser->pname.data;
	nc_memcpy(p, new_ser_addr.data, new_ser_addr.len);
	p += new_ser_addr.len;
	nc_memcpy(p, ":", 1);
	p += 1;
	nc_memcpy(p, weight_str.data, weight_str.len);
	p += weight_str.len;
	nc_memcpy(p, "\0", 1);
	log_debug(LOG_DEBUG, "new_ser->pname.len: %d", new_ser->pname.len);
	log_debug(LOG_DEBUG, "new_ser->pname.data: %s", new_ser->pname.data);

	if(curr_ser->name_null)
	{
		status = string_copy(&new_ser->name, new_ser_addr.data, new_ser_addr.len);
		if(status != NC_OK)
		{
			goto error;
		}
	}
	else
	{
		new_ser->name = curr_ser->name;
	}
	new_ser->port = (uint16_t)new_ser_port;
	new_ser->weight = curr_ser->weight;

	status = nc_resolve(&new_ser_host, new_ser_port, &sock);

	new_ser->addrlen = sock.addrlen;

	new_ser->addr = nc_alloc(sizeof(struct sockaddr));
	
	new_ser->addr->sa_family = ((struct sockaddr*)&sock.addr)->sa_family;

	new_ser->family = new_ser->addr->sa_family;
	
	for(i = 0; i < 14; i++)
	{
		new_ser->addr->sa_data[i] = (((struct sockaddr*)&sock.addr)->sa_data)[i];
	}
	
	new_ser->ns_conn_q = 0;
    TAILQ_INIT(&new_ser->s_conn_q);
	new_ser->next_retry = 0LL;
	new_ser->failure_count = 0;
	new_ser->name_null = curr_ser->name_null;	

    /* pick a connection to a given server */
    conn = server_conn(new_ser);
    if (conn == NULL) {
        return NULL;
    }

    status = server_connect(ctx, new_ser, conn);
    if (status != NC_OK) {
        server_close(ctx, conn);
        return NULL;
    }


	if(!string_empty(&weight_str))
	{
		string_deinit(&weight_str);
	}

	if(!string_empty(&new_ser_host))
	{
		string_deinit(&new_ser_host);
	}

	msg->server = new_ser;

	conn->replace_server = 1;
	conn->conf_version_curr = ctx->conf_version;
	conn->ctx = ctx;
	
    return conn;

error:
	if(!string_empty(&new_ser->pname))
	{
		string_deinit(&new_ser->pname);
	}

	if(!string_empty(&new_ser->name) && curr_ser->name_null)
	{
		string_deinit(&new_ser->name);
	}

	if(!string_empty(&weight_str))
	{
		string_deinit(&weight_str);
	}

	if(!string_empty(&new_ser_host))
	{
		string_deinit(&new_ser_host);
	}

	nc_free(new_ser);
	new_ser = NULL;
	
	return NULL;
}

void
server_close_for_replace_server(struct server *server)
{
	if(server == NULL)
	{
		return;
	}
	
	ASSERT(server->ns_conn_q == 0);
	
	string_deinit(&server->pname);
	if(server->name_null)
	{
		string_deinit(&server->name);
	}
	nc_free(server->addr);
	nc_free(server);
}

#endif //shenzheng 2015-6-25 replace server

static rstatus_t
server_pool_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    if (!sp->preconnect) {
        return NC_OK;
    }

    status = array_each(&sp->server, server_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

rstatus_t
server_pool_preconnect(struct context *ctx)
{
    rstatus_t status;

    status = array_each(&ctx->pool, server_pool_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

static rstatus_t
server_pool_each_disconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    status = array_each(&sp->server, server_each_disconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

void
server_pool_disconnect(struct context *ctx)
{
    array_each(&ctx->pool, server_pool_each_disconnect, NULL);
}

static rstatus_t
server_pool_each_set_owner(void *elem, void *data)
{
    struct server_pool *sp = elem;
    struct context *ctx = data;

    sp->ctx = ctx;

    return NC_OK;
}

static rstatus_t
server_pool_each_calc_connections(void *elem, void *data)
{
    struct server_pool *sp = elem;
    struct context *ctx = data;

    ctx->max_nsconn += sp->server_connections * array_n(&sp->server);
    ctx->max_nsconn += 1; /* pool listening socket */

    return NC_OK;
}

rstatus_t
server_pool_run(struct server_pool *pool)
{
    ASSERT(array_n(&pool->server) != 0);

    switch (pool->dist_type) {
    case DIST_KETAMA:
        return ketama_update(pool);

    case DIST_MODULA:
        return modula_update(pool);

    case DIST_RANDOM:
        return random_update(pool);

    default:
        NOT_REACHED();
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
server_pool_each_run(void *elem, void *data)
{
    return server_pool_run(elem);
}

#if 1 //shenzheng 2014-12-23 replication pool
static rstatus_t 
server_pool_each_set_replication(void *elem, void *data)
{
	rstatus_t status;
	uint32_t i, nelem;
	struct server_pool *sp = elem;
	struct array *sps = data;
	struct server_pool *sp_master;
	for (i = 0, nelem = array_n(sps); i < nelem; i++) 
	{
        sp_master = array_get(sps, i);
		if(0 == string_compare(&sp->replication_from, &sp_master->name))
		{
			log_debug(LOG_DEBUG, "server_pool %s replication from %s", sp->name.data, sp_master->name.data);

			//now we just allow one replication pool from another pool
			if(array_n(&sp_master->replication_pools) > 0)
			{
				log_error("error: %s pool has more than one replication pools(now we just allow one replication pool from another pool)!!", sp_master->name.data);
				return NC_ERROR;
			}
			//now we do not allow multi-level replication
			if(!string_empty(&sp_master->replication_from))
			{
				log_error("error: %s pool can not replication from %s pool(now we do not allow multi-level replication)!!", 
					sp->name.data, sp_master->name.data);
				return NC_ERROR;
			}
			//now we do not allow multi-level replication
			if(array_n(&sp->replication_pools) > 0)
			{
				log_error("error: %s pool can not replication from %s pool(now we do not allow multi-level replication)!!", 
					sp->name.data, sp_master->name.data);
				return NC_ERROR;
			}
			//now we do not support replication pool for redis
			if(sp->redis || sp_master->redis)
			{
				log_error("error: %s pool can not replication from %s pool(now we do not support replication pool for redis)!!", 
					sp->name.data, sp_master->name.data);
				return NC_ERROR;
			}
			
			if(0 == sp_master->replication_pools.nalloc)
			{
				array_init(&sp_master->replication_pools, 1, sizeof(struct server_pool **));
			}
			struct server_pool **sp_address = array_push(&sp_master->replication_pools);
			*sp_address = sp;
			sp->sp_master = sp_master;
			if(sp_master->inconsistent_log == NULL)
			{
				status = inconsistent_log_init(sp_master);
				if(status != NC_OK)
				{
					return status;
				}
			}
			break;
		}
	}
	return NC_OK;
}
#endif //shenzheng 2015-4-20 replication pool

rstatus_t
server_pool_init(struct array *server_pool, struct array *conf_pool,
                 struct context *ctx)
{
    rstatus_t status;
    uint32_t npool;

    npool = array_n(conf_pool);
    ASSERT(npool != 0);
    ASSERT(array_n(server_pool) == 0);

    status = array_init(server_pool, npool, sizeof(struct server_pool));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf pool to server pool */
    status = array_each(conf_pool, conf_pool_each_transform, server_pool);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }
    ASSERT(array_n(server_pool) == npool);

    /* set ctx as the server pool owner */
    status = array_each(server_pool, server_pool_each_set_owner, ctx);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    /* compute max server connections */
    ctx->max_nsconn = 0;
    status = array_each(server_pool, server_pool_each_calc_connections, ctx);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    /* update server pool continuum */
    status = array_each(server_pool, server_pool_each_run, NULL);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

#if 1 //shenzheng 2014-12-23 replication pool
	/* build the replication relationship */
	status = array_each(server_pool, server_pool_each_set_replication, server_pool);
	if (status != NC_OK){
		server_pool_deinit(server_pool);
		return status;
	}

	uint32_t i, nelem;
	for(i = 0, nelem = array_n(server_pool); i < nelem; i ++)
	{
		struct server_pool * sp = array_get(server_pool, i);
		log_debug(LOG_DEBUG, "server_pool %s replication_mode : %d", sp->name.data, 
			sp->replication_mode);
		log_debug(LOG_DEBUG, "server_pool %s penetrate_mode : %d", sp->name.data, 
			sp->penetrate_mode);
		log_debug(LOG_DEBUG, "server_pool %s write_back_mode : %d", sp->name.data, 
			sp->write_back_mode);
		log_debug(LOG_DEBUG, "server_pool %s replication_from : %s", sp->name.data, 
			sp->replication_from.len?sp->replication_from.data:"NULL");
		log_debug(LOG_DEBUG, "server_pool %s sp_master : %d", sp->name.data, 
			sp->sp_master?sp->sp_master->idx:"NULL");
		log_debug(LOG_DEBUG, "server_pool %s replication_pools size : %d", sp->name.data, 
			sp->replication_pools.nelem);
		uint32_t j, nelem_replication;
		for(j = 0, nelem_replication= array_n(&sp->replication_pools); j < nelem_replication; j ++)
		{
			struct server_pool ** sp_tmp = array_get(&sp->replication_pools, j);
			log_debug(LOG_DEBUG, "server_pool %s replication_pools[%d] : %s", sp->name.data, 
			j, (*sp_tmp)->name.len?(*sp_tmp)->name.data:"NULL");
		}
	}
#endif //shenzheng 2015-3-9 replication pool

    log_debug(LOG_DEBUG, "init %"PRIu32" pools", npool);

    return NC_OK;
}

void
server_pool_deinit(struct array *server_pool)
{
    uint32_t i, npool;

    for (i = 0, npool = array_n(server_pool); i < npool; i++) {
        struct server_pool *sp;

        sp = array_pop(server_pool);
        ASSERT(sp->p_conn == NULL);
        ASSERT(TAILQ_EMPTY(&sp->c_conn_q) && sp->nc_conn_q == 0);

        if (sp->continuum != NULL) {
            nc_free(sp->continuum);
            sp->ncontinuum = 0;
            sp->nserver_continuum = 0;
            sp->nlive_server = 0;
        }

        server_deinit(&sp->server);
#if 1 //shenzheng 2014-12-23 replication pool
		uint32_t nreplication_pool;
		nreplication_pool = array_n(&sp->replication_pools);
		log_debug(LOG_DEBUG, "nreplication_pool : %d", nreplication_pool);
		while(array_n(&sp->replication_pools) > 0) 
		{
        	struct server_pool *sp_tmp;
			sp_tmp = array_pop(&sp->replication_pools);
    	}
    	log_debug(LOG_DEBUG, "deinit replication_pools");
    	array_deinit(&sp->replication_pools);

		if(sp->inconsistent_log != NULL)
		{
			inconsistent_log_deinit(sp);
		}
#endif //shenzheng 2015-4-20 replication pool
        log_debug(LOG_DEBUG, "deinit pool %"PRIu32" '%.*s'", sp->idx,
                  sp->name.len, sp->name.data);
    }

    array_deinit(server_pool);

    log_debug(LOG_DEBUG, "deinit %"PRIu32" pools", npool);
}

#if 1 //shenzheng 2015-5-8 config-reload
static rstatus_t 
server_pool_each_reset_old(void *elem, void *data)
{
	uint32_t idx;
	struct server_pool *sp, *sp_new;
	struct context *ctx = data;
	
	sp = elem;
	ASSERT(sp != NULL);

	idx = sp->idx;

	if(!ctx->which_pool)
	{
		if(array_n(&ctx->pool_swap) < idx+1)
		{
			return NC_OK;
		}
		sp_new = array_get(&ctx->pool_swap, idx);
	}
	else
	{
		if(array_n(&ctx->pool) < idx+1)
		{
			return NC_OK;
		}
		sp_new = array_get(&ctx->pool, idx);
	}
	ASSERT(sp_new != NULL);

	ASSERT(sp->p_conn->owner == sp);
	
	sp->p_conn->owner = sp_new;
	
	return NC_OK;
}

static rstatus_t 
server_each_reset_old(void *elem, void *data)
{
	rstatus_t status;
	uint32_t i, nelem, ns_conn_q_temp;
	struct conn *conn;
	struct server *server, *server_new = elem;
	struct array *servers = data;

	for (i = 0, nelem = array_n(servers); i < nelem; i++) 
	{
		server = array_get(servers, i);
		if(0 == string_compare(&server->pname, &server_new->pname))
		{
			
			log_debug(LOG_DEBUG, "server->pname : %s", server->pname.data);
			
			ASSERT(server_new->ns_conn_q == 0 && TAILQ_EMPTY(&server_new->s_conn_q));

			break;
		}
	}
	
	return NC_OK;
}

rstatus_t 
server_pool_each_proxy_conn_new(void *elem, void *data)
{
	rstatus_t status;
	uint32_t i, nelem;
	struct conn *pconn;
	struct server_pool *sp, *sp_new = elem;
	struct array *sps = data;
	log_debug(LOG_DEBUG, "sp_new->addrstr : %s", sp_new->addrstr.data);
	for (i = 0, nelem = array_n(sps); i < nelem; i++) 
	{
        sp = array_get(sps, i);
		if(0 == string_compare(&sp->addrstr, &sp_new->addrstr))
		{
			return NC_OK;
		}
	}
	status = proxy_init_for_reload(elem);
	if(status != NC_OK)
	{
		return status;
	}
	return NC_OK;
}

rstatus_t 
server_pool_each_conn_old_close(void *elem, void *data)
{
	rstatus_t status;
	uint32_t i, nelem;
	struct context *ctx;
	struct conn *p_conn, *c_conn, *s_conn;
	struct server_pool *sp, *sp_old = elem;
	struct server *server;
	struct array *sps = data;
	
	log_debug(LOG_DEBUG, "sp_old->addrstr : %s", sp_old->addrstr.data);

	ctx = sp_old->ctx;
	ASSERT(ctx != NULL);
	
	for (i = 0, nelem = array_n(sps); i < nelem; i++) 
	{
        sp = array_get(sps, i);
		if(0 == string_compare(&sp->addrstr, &sp_old->addrstr))
		{
			//return NC_OK;
			goto server_close;
		}
	}

	p_conn = sp_old->p_conn;
	if(p_conn != NULL)
	{
		status = event_del_conn(ctx->evb, p_conn);
	    if (status < 0) {
	        log_warn("event del conn client %d failed, ignored: %s",
	                 p_conn->sd, strerror(errno));
	    }
	}
	proxy_each_deinit(sp_old, NULL);

	while (!TAILQ_EMPTY(&sp_old->c_conn_q)) {			

		ASSERT(sp_old->nc_conn_q > 0);
		c_conn = TAILQ_FIRST(&sp_old->c_conn_q);

		ASSERT(c_conn->client && !c_conn->proxy);
		ASSERT(c_conn->addr == NULL);

		status = event_del_conn(ctx->evb, c_conn);
	    if (status < 0) {
	        log_warn("event del conn client %d failed, ignored: %s",
	                 c_conn->sd, strerror(errno));
	    }

		c_conn->close(ctx, c_conn);

		log_debug(LOG_INFO, "c_conn(%d) removed", c_conn->sd);
	}

	ASSERT(sp_old->nc_conn_q == 0);
	ASSERT(TAILQ_EMPTY(&sp_old->c_conn_q));	

server_close:
	for(i = 0, nelem = array_n(&sp_old->server); i < nelem; i++)
	{
		server = array_get(&sp_old->server, i);
		ASSERT(server != NULL);
		
		while (!TAILQ_EMPTY(&server->s_conn_q)) {

	        ASSERT(server->ns_conn_q > 0);

	        s_conn = TAILQ_FIRST(&server->s_conn_q);

			status = event_del_conn(ctx->evb, s_conn);
		    if (status < 0) {
		        log_warn("event del conn client %d failed, ignored: %s",
		                 s_conn->sd, strerror(errno));
		    }

	        s_conn->close(ctx, s_conn);
			
			log_debug(LOG_INFO, "s_conn(%d) removed", s_conn->sd);
	    }

		ASSERT(server->ns_conn_q == 0);
		ASSERT(TAILQ_EMPTY(&server->s_conn_q));	
	}
	
	
	return NC_OK;
}

rstatus_t 
server_pool_each_proxy_conn_reset(void *elem, void *data)
{
	rstatus_t status;
	uint32_t i, nelem;
	struct conn *pconn;
	struct server_pool *sp, *sp_new = elem;
	struct array *sps = data;
	log_debug(LOG_DEBUG, "sp_new->addrstr : %s", sp_new->addrstr.data);
	for (i = 0, nelem = array_n(sps); i < nelem; i++) 
	{
        sp = array_get(sps, i);
		if(0 == string_compare(&sp->addrstr, &sp_new->addrstr))
		{
			pconn = sp->p_conn;
			
			ASSERT(pconn != NULL);
			
			pconn->owner = sp_new;
			sp_new->p_conn = pconn;
			pconn->addr = sp_new->addr;
			sp->p_conn = NULL;
			log_debug(LOG_INFO, "p_conn(%d) moved", pconn->sd);
			return NC_OK;
		}
	}
	return NC_OK;
}


rstatus_t 
server_pool_each_client_conn_reset(void *elem, void *data)
{
	uint32_t i, nelem;
	struct conn *conn;
	struct server_pool *sp, *sp_new = elem;
	struct array *sps = data;
	log_debug(LOG_DEBUG, "sp_new->addrstr : %s", sp_new->addrstr.data);
	
	for (i = 0, nelem = array_n(sps); i < nelem; i++) 
	{
        sp = array_get(sps, i);
		if(0 == string_compare(&sp->addrstr, &sp_new->addrstr))
		{			
			if(TAILQ_EMPTY(&sp->c_conn_q))
			{
				ASSERT(sp->nc_conn_q == 0);
				return NC_OK;
			}
		
			while (!TAILQ_EMPTY(&sp->c_conn_q)) {
			
				ASSERT(sp->nc_conn_q > 0);
				conn = TAILQ_FIRST(&sp->c_conn_q);

				ASSERT(conn->client && !conn->proxy);
				ASSERT(conn->addr == NULL);
				
				sp->nc_conn_q--;
				TAILQ_REMOVE(&sp->c_conn_q, conn, conn_tqe);

				sp_new->nc_conn_q++;
				TAILQ_INSERT_TAIL(&sp_new->c_conn_q, conn, conn_tqe);
				conn->owner = sp_new;
				
#if 1 //shenzheng 2015-7-7 replication pool
				conn->nreplication_request = array_n(&sp_new->replication_pools);
#endif //shenzheng 2015-7-7 replication pool
				
				log_debug(LOG_INFO, "c_conn(%d) moved", conn->sd);
			}
			
			ASSERT(sp->nc_conn_q == 0);
			ASSERT(TAILQ_EMPTY(&sp->c_conn_q));			
			
			break;
		}
	}
	return NC_OK;
}


rstatus_t 
server_pool_old_deinit(struct array *sps, struct context *ctx)
{
	rstatus_t status;
	bool flag;
	uint32_t i, nelem, i_p, nelem_p;
	struct conn *conn, *pconn;
	struct server *server;
	struct server_pool *sp;
	
	ASSERT(sps != NULL);
	flag = false;
	for(i_p = 0, nelem_p = array_n(sps); i_p < nelem_p; i_p++)
	{
		sp = array_get(sps, i_p);
		log_debug(LOG_DEBUG, "sp->addrstr : %s", sp->addrstr.data);
		pconn = sp->p_conn;
		if(pconn != NULL)
		{
			status = event_del_conn(ctx->evb, pconn);
		    if (status < 0) {
		        log_warn("event del conn client %d failed, ignored: %s",
		                 pconn->sd, strerror(errno));
		    }
			pconn->close(ctx, pconn);
		}		

		for (i = 0, nelem = array_n(&sp->server); i < nelem; i++) 
		{
			server = array_get(&sp->server, i);

			for (conn = TAILQ_LAST(&server->s_conn_q, conn_tqh); conn != NULL;
			conn = pconn) {
				pconn = TAILQ_PREV(conn, conn_tqh, conn_tqe);

				if(!conn->active(conn))
				{
					status = event_del_conn(ctx->evb, conn);
				    if (status < 0) {
				        log_warn("event del conn client %d failed, ignored: %s",
				                 conn->sd, strerror(errno));
				    }
				
					log_debug(LOG_INFO, "s_conn(%d) closed in old pool", conn->sd);
					conn->close(ctx, conn);
				}
				else
				{
					flag |= true;
				}
			}
		}
	}
	
	if(flag)
	{
		return NC_EAGAIN;
	}
	
	return NC_OK;
}


#endif //shenzheng 2015-5-8 config-reload

