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

#include <sys/uio.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_client.h>
#include <nc_proxy.h>
#include <proto/nc_proto.h>

/*
 *                   nc_connection.[ch]
 *                Connection (struct conn)
 *                 +         +          +
 *                 |         |          |
 *                 |       Proxy        |
 *                 |     nc_proxy.[ch]  |
 *                 /                    \
 *              Client                Server
 *           nc_client.[ch]         nc_server.[ch]
 *
 * Nutcracker essentially multiplexes m client connections over n server
 * connections. Usually m >> n, so that nutcracker can pipeline requests
 * from several clients over a server connection and hence use the connection
 * bandwidth to the server efficiently
 *
 * Client and server connection maintain two fifo queues for requests:
 *
 * 1). in_q (imsg_q):  queue of incoming requests
 * 2). out_q (omsg_q): queue of outstanding (outgoing) requests
 *
 * Request received over the client connection are forwarded to the server by
 * enqueuing the request in the chosen server's in_q. From the client's
 * perspective once the request is forwarded, it is outstanding and is tracked
 * in the client's out_q (unless the request was tagged as noreply). The server
 * in turn picks up requests from its own in_q in fifo order and puts them on
 * the wire. Once the request is outstanding on the wire, and a response is
 * expected for it, the server keeps track of outstanding requests it in its
 * own out_q.
 *
 * The server's out_q enables us to pair a request with a response while the
 * client's out_q enables us to pair request and response in the order in
 * which they are received from the client.
 *
 *
 *      Clients                             Servers
 *                                    .
 *    in_q: <empty>                   .
 *    out_q: req11 -> req12           .   in_q:  req22
 *    (client1)                       .   out_q: req11 -> req21 -> req12
 *                                    .   (server1)
 *    in_q: <empty>                   .
 *    out_q: req21 -> req22 -> req23  .
 *    (client2)                       .
 *                                    .   in_q:  req23
 *                                    .   out_q: <empty>
 *                                    .   (server2)
 *
 * In the above example, client1 has two pipelined requests req11 and req12
 * both of which are outstanding on the server connection server1. On the
 * other hand, client2 has three requests req21, req22 and req23, of which
 * only req21 is outstanding on the server connection while req22 and
 * req23 are still waiting to be put on the wire. The fifo of client's
 * out_q ensures that we always send back the response of request at the head
 * of the queue, before sending out responses of other completed requests in
 * the queue.
 */

static uint32_t nfree_connq;       /* # free conn q */
static struct conn_tqh free_connq; /* free conn q */
static uint64_t ntotal_conn;       /* total # connections counter from start */
static uint32_t ncurr_conn;        /* current # connections */
static uint32_t ncurr_cconn;       /* current # client connections */

#if 1 //shenzheng 2015-7-8 proxy administer
static uint32_t nfree_connq_proxy_adm;       /* # free conn q for proxy administer  */
static struct conn_tqh free_connq_proxy_adm; /* free conn q for proxy administer  */
static uint64_t ntotal_conn_proxy_adm;       /* total # connections counter from start for proxy administer  */
static uint32_t ncurr_conn_proxy_adm;        /* current # connections for proxy administer  */
static uint32_t ncurr_cconn_proxy_adm;       /* current # client connections for proxy administer  */
#endif //shenzheng 2015-7-8 proxy administer

/*
 * Return the context associated with this connection.
 */
struct context *
conn_to_ctx(struct conn *conn)
{
    struct server_pool *pool;

    if (conn->proxy || conn->client) {
        pool = conn->owner;
    } else {
        struct server *server = conn->owner;
        pool = server->owner;
    }

    return pool->ctx;
}

static struct conn *
_conn_get(void)
{
    struct conn *conn;

    if (!TAILQ_EMPTY(&free_connq)) {
        ASSERT(nfree_connq > 0);

        conn = TAILQ_FIRST(&free_connq);
        nfree_connq--;
        TAILQ_REMOVE(&free_connq, conn, conn_tqe);
    } else {
        conn = nc_alloc(sizeof(*conn));
        if (conn == NULL) {
            return NULL;
        }
    }

    conn->owner = NULL;

    conn->sd = -1;
    /* {family, addrlen, addr} are initialized in enqueue handler */

    TAILQ_INIT(&conn->imsg_q);
    TAILQ_INIT(&conn->omsg_q);
    conn->rmsg = NULL;
    conn->smsg = NULL;

    /*
     * Callbacks {recv, recv_next, recv_done}, {send, send_next, send_done},
     * {close, active}, parse, {ref, unref}, {enqueue_inq, dequeue_inq} and
     * {enqueue_outq, dequeue_outq} are initialized by the wrapper.
     */

    conn->send_bytes = 0;
    conn->recv_bytes = 0;

    conn->events = 0;
    conn->err = 0;
    conn->recv_active = 0;
    conn->recv_ready = 0;
    conn->send_active = 0;
    conn->send_ready = 0;

    conn->client = 0;
    conn->proxy = 0;
    conn->connecting = 0;
    conn->connected = 0;
    conn->eof = 0;
    conn->done = 0;
    conn->redis = 0;
    conn->need_auth = 0;
	
#if 1 //shenzheng 2015-7-14 config-reload
	conn->reload_conf = 0;
#endif //shenzheng 2015-7-14 config-reload

#if 1 //shenzheng 2015-7-28 replace server
	conn->replace_server = 0;
	conn->conf_version_curr = -1;
	conn->ctx = NULL;
#endif //shenzheng 2015-7-28 replace server

    ntotal_conn++;
    ncurr_conn++;

    return conn;
}

static bool
conn_need_auth(void *owner, bool redis) {
    struct server_pool *pool = (struct server_pool *)(owner);

    if (redis && pool->redis_auth.len > 0) {
        return true;
    }

    return false;
}

struct conn *
conn_get(void *owner, bool client, bool redis)
{
    struct conn *conn;

    conn = _conn_get();
    if (conn == NULL) {
        return NULL;
    }

    /* connection either handles redis or memcache messages */
    conn->redis = redis ? 1 : 0;

    conn->client = client ? 1 : 0;

    if (conn->client) {
        /*
         * client receives a request, possibly parsing it, and sends a
         * response downstream.
         */
        conn->recv = msg_recv;
        conn->recv_next = req_recv_next;
        conn->recv_done = req_recv_done;

        conn->send = msg_send;
        conn->send_next = rsp_send_next;
        conn->send_done = rsp_send_done;

        conn->close = client_close;
        conn->active = client_active;

        conn->ref = client_ref;
        conn->unref = client_unref;
        conn->need_auth = conn_need_auth(owner, redis);

        conn->enqueue_inq = NULL;
        conn->dequeue_inq = NULL;
        conn->enqueue_outq = req_client_enqueue_omsgq;
        conn->dequeue_outq = req_client_dequeue_omsgq;

        ncurr_cconn++;
    } else {
        /*
         * server receives a response, possibly parsing it, and sends a
         * request upstream.
         */
        struct server *server = (struct server *)owner;

        conn->recv = msg_recv;
        conn->recv_next = rsp_recv_next;
        conn->recv_done = rsp_recv_done;

        conn->send = msg_send;
        conn->send_next = req_send_next;
        conn->send_done = req_send_done;

        conn->close = server_close;
        conn->active = server_active;

        conn->ref = server_ref;
        conn->unref = server_unref;

        conn->need_auth = conn_need_auth(server->owner, redis);

        conn->enqueue_inq = req_server_enqueue_imsgq;
        conn->dequeue_inq = req_server_dequeue_imsgq;
        conn->enqueue_outq = req_server_enqueue_omsgq;
        conn->dequeue_outq = req_server_dequeue_omsgq;
    }

    conn->ref(conn, owner);
    log_debug(LOG_VVERB, "get conn %p client %d", conn, conn->client);

    return conn;
}

struct conn *
conn_get_proxy(void *owner)
{
    struct server_pool *pool = owner;
    struct conn *conn;

    conn = _conn_get();
    if (conn == NULL) {
        return NULL;
    }

    conn->redis = pool->redis;

    conn->proxy = 1;

    conn->recv = proxy_recv;
    conn->recv_next = NULL;
    conn->recv_done = NULL;

    conn->send = NULL;
    conn->send_next = NULL;
    conn->send_done = NULL;

    conn->close = proxy_close;
    conn->active = NULL;

    conn->ref = proxy_ref;
    conn->unref = proxy_unref;

    conn->enqueue_inq = NULL;
    conn->dequeue_inq = NULL;
    conn->enqueue_outq = NULL;
    conn->dequeue_outq = NULL;

    conn->ref(conn, owner);

    log_debug(LOG_VVERB, "get conn %p proxy %d", conn, conn->proxy);

    return conn;
}

static void
conn_free(struct conn *conn)
{
    log_debug(LOG_VVERB, "free conn %p", conn);
    nc_free(conn);
}

void
conn_put(struct conn *conn)
{
    ASSERT(conn->sd < 0);
    ASSERT(conn->owner == NULL);

    log_debug(LOG_VVERB, "put conn %p", conn);

    nfree_connq++;
    TAILQ_INSERT_HEAD(&free_connq, conn, conn_tqe);

    if (conn->client) {
        ncurr_cconn--;
    }
    ncurr_conn--;
}

void
conn_init(void)
{
    log_debug(LOG_DEBUG, "conn size %d", sizeof(struct conn));
    nfree_connq = 0;
    TAILQ_INIT(&free_connq);
}

void
conn_deinit(void)
{
    struct conn *conn, *nconn; /* current and next connection */

    for (conn = TAILQ_FIRST(&free_connq); conn != NULL;
         conn = nconn, nfree_connq--) {
        ASSERT(nfree_connq > 0);
        nconn = TAILQ_NEXT(conn, conn_tqe);
        conn_free(conn);
    }
    ASSERT(nfree_connq == 0);
}

ssize_t
conn_recv(struct conn *conn, void *buf, size_t size)
{
    ssize_t n;

    ASSERT(buf != NULL);
    ASSERT(size > 0);
    ASSERT(conn->recv_ready);

    for (;;) {
        n = nc_read(conn->sd, buf, size);

        log_debug(LOG_VERB, "recv on sd %d %zd of %zu", conn->sd, n, size);

        if (n > 0) {
            if (n < (ssize_t) size) {
                conn->recv_ready = 0;
            }
            conn->recv_bytes += (size_t)n;
            return n;
        }

        if (n == 0) {
            conn->recv_ready = 0;
            conn->eof = 1;
            log_debug(LOG_INFO, "recv on sd %d eof rb %zu sb %zu", conn->sd,
                      conn->recv_bytes, conn->send_bytes);
            return n;
        }

        if (errno == EINTR) {
            log_debug(LOG_VERB, "recv on sd %d not ready - eintr", conn->sd);
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            conn->recv_ready = 0;
            log_debug(LOG_VERB, "recv on sd %d not ready - eagain", conn->sd);
            return NC_EAGAIN;
        } else {
            conn->recv_ready = 0;
            conn->err = errno;
            log_error("recv on sd %d failed: %s", conn->sd, strerror(errno));
            return NC_ERROR;
        }
    }

    NOT_REACHED();

    return NC_ERROR;
}

ssize_t
conn_sendv(struct conn *conn, struct array *sendv, size_t nsend)
{
    ssize_t n;

    ASSERT(array_n(sendv) > 0);
    ASSERT(nsend != 0);
    ASSERT(conn->send_ready);

    for (;;) {
        n = nc_writev(conn->sd, sendv->elem, sendv->nelem);

        log_debug(LOG_VERB, "sendv on sd %d %zd of %zu in %"PRIu32" buffers",
                  conn->sd, n, nsend, sendv->nelem);

        if (n > 0) {
            if (n < (ssize_t) nsend) {
                conn->send_ready = 0;
            }
            conn->send_bytes += (size_t)n;
            return n;
        }

        if (n == 0) {
            log_warn("sendv on sd %d returned zero", conn->sd);
            conn->send_ready = 0;
            return 0;
        }

        if (errno == EINTR) {
            log_debug(LOG_VERB, "sendv on sd %d not ready - eintr", conn->sd);
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            conn->send_ready = 0;
            log_debug(LOG_VERB, "sendv on sd %d not ready - eagain", conn->sd);
            return NC_EAGAIN;
        } else {
            conn->send_ready = 0;
            conn->err = errno;
            log_error("sendv on sd %d failed: %s", conn->sd, strerror(errno));
            return NC_ERROR;
        }
    }

    NOT_REACHED();

    return NC_ERROR;
}

uint32_t
conn_ncurr_conn(void)
{
    return ncurr_conn;
}

uint64_t
conn_ntotal_conn(void)
{
    return ntotal_conn;
}

uint32_t
conn_ncurr_cconn(void)
{
    return ncurr_cconn;
}

#if 1 //shenzheng 2015-4-27 proxy administer
uint32_t
conn_ncurr_conn_proxy_adm(void)
{
    return ncurr_conn_proxy_adm;
}

uint64_t
conn_ntotal_conn_proxy_adm(void)
{
    return ntotal_conn_proxy_adm;
}

uint32_t
conn_ncurr_cconn_proxy_adm(void)
{
    return ncurr_cconn_proxy_adm;
}

static struct conn *
_conn_get_proxy_adm(void)
{
    struct conn *conn;

    if (!TAILQ_EMPTY(&free_connq_proxy_adm)) {
        ASSERT(nfree_connq_proxy_adm > 0);

        conn = TAILQ_FIRST(&free_connq_proxy_adm);
        nfree_connq_proxy_adm--;
        TAILQ_REMOVE(&free_connq_proxy_adm, conn, conn_tqe);
    } else {
        conn = nc_alloc(sizeof(*conn));
        if (conn == NULL) {
            return NULL;
        }
    }

    conn->owner = NULL;

    conn->sd = -1;
    /* {family, addrlen, addr} are initialized in enqueue handler */

    TAILQ_INIT(&conn->imsg_q);
    TAILQ_INIT(&conn->omsg_q);
    conn->rmsg = NULL;
    conn->smsg = NULL;

    /*
     * Callbacks {recv, recv_next, recv_done}, {send, send_next, send_done},
     * {close, active}, parse, {ref, unref}, {enqueue_inq, dequeue_inq} and
     * {enqueue_outq, dequeue_outq} are initialized by the wrapper.
     */

    conn->send_bytes = 0;
    conn->recv_bytes = 0;

    conn->events = 0;
    conn->err = 0;
    conn->recv_active = 0;
    conn->recv_ready = 0;
    conn->send_active = 0;
    conn->send_ready = 0;

    conn->client = 0;
    conn->proxy = 0;
    conn->connecting = 0;
    conn->connected = 0;
    conn->eof = 0;
    conn->done = 0;
    conn->redis = 0;
    conn->need_auth = 0;
	
#if 1 //shenzheng 2015-7-14 config-reload
	conn->reload_conf = 0;
#endif //shenzheng 2015-7-14 config-reload

#if 1 //shenzheng 2015-7-28 replace server
	conn->replace_server = 0;
	conn->conf_version_curr = -1;
	conn->ctx = NULL;
#endif //shenzheng 2015-7-28 replace server

    ntotal_conn_proxy_adm++;
    ncurr_conn_proxy_adm++;

    return conn;
}

void
conn_put_proxy_adm(struct conn *conn)
{
    ASSERT(conn->sd < 0);
    ASSERT(conn->owner == NULL);

    log_debug(LOG_VVERB, "put conn %p", conn);

    nfree_connq_proxy_adm++;
    TAILQ_INSERT_HEAD(&free_connq_proxy_adm, conn, conn_tqe);

    if (conn->client) {
        ncurr_cconn_proxy_adm--;
    }
    ncurr_conn_proxy_adm--;
}

struct conn *
conn_get_proxy_adm(void *owner, int sd, bool client)
{
	
	struct conn *conn;
	
	conn = _conn_get_proxy_adm();
	if (conn == NULL) {
		return NULL;
	}

	conn->sd = sd;
	if(client)
	{
		conn->client = 1;
		conn->recv = msg_recv_proxy_adm;
		conn->recv_next = req_recv_next_proxy_adm;
		conn->recv_done = req_recv_done_proxy_adm;

		conn->send = msg_send_proxy_adm;
		conn->send_next = rsp_send_next_proxy_adm;
		conn->send_done = rsp_send_done_proxy_adm;

		conn->close = proxy_adm_client_close;
		conn->active = proxy_adm_client_active;

		conn->ref = proxy_adm_client_ref;
		conn->unref = proxy_adm_client_unref;
		conn->need_auth = NULL;
		
		conn->enqueue_inq = NULL;
		conn->dequeue_inq = NULL;
		conn->enqueue_outq = req_client_enqueue_omsgq;
		conn->dequeue_outq = req_client_dequeue_omsgq;		
		
		ncurr_cconn_proxy_adm++;
	}
	else
	{
		conn->proxy = 1;
		conn->recv = proxy_adm_recv;
	    conn->recv_next = NULL;
	    conn->recv_done = NULL;

	    conn->send = NULL;
	    conn->send_next = NULL;
	    conn->send_done = NULL;

	    conn->close = proxy_adm_close;
	    conn->active = NULL;

	    conn->ref = proxy_adm_ref;
	    conn->unref = proxy_adm_unref;

	    conn->enqueue_inq = NULL;
	    conn->dequeue_inq = NULL;
	    conn->enqueue_outq = NULL;
	    conn->dequeue_outq = NULL;

    }
	

	conn->ref(conn, owner);
    log_debug(LOG_VVERB, "get conn %p client %d", conn, conn->client);

	return conn;
}
#endif //shenzheng 2015-7-9 proxy administer

#if 1 //shenzheng 2015-7-9 config-reload
static struct conn *
_conn_get_for_reload(void)
{
    struct conn *conn;
	
    conn = nc_alloc(sizeof(*conn));
    if (conn == NULL) {
        return NULL;
    }
    
    conn->owner = NULL;

    conn->sd = -1;
    /* {family, addrlen, addr} are initialized in enqueue handler */

    TAILQ_INIT(&conn->imsg_q);
    TAILQ_INIT(&conn->omsg_q);
    conn->rmsg = NULL;
    conn->smsg = NULL;

    /*
     * Callbacks {recv, recv_next, recv_done}, {send, send_next, send_done},
     * {close, active}, parse, {ref, unref}, {enqueue_inq, dequeue_inq} and
     * {enqueue_outq, dequeue_outq} are initialized by the wrapper.
     */

    conn->send_bytes = 0;
    conn->recv_bytes = 0;

    conn->events = 0;
    conn->err = 0;
    conn->recv_active = 0;
    conn->recv_ready = 0;
    conn->send_active = 0;
    conn->send_ready = 0;

    conn->client = 0;
    conn->proxy = 0;
    conn->connecting = 0;
    conn->connected = 0;
    conn->eof = 0;
    conn->done = 0;
    conn->redis = 0;
    conn->need_auth = 0;
	conn->reload_conf = 0;

#if 1 //shenzheng 2015-7-28 replace server
	conn->replace_server = 0;
	conn->conf_version_curr = -1;
	conn->ctx = NULL;
#endif //shenzheng 2015-7-28 replace server

    ntotal_conn++;
    ncurr_conn++;
	
    return conn;
}

struct conn *
conn_get_proxy_for_reload(void *owner)
{
    struct server_pool *pool = owner;
    struct conn *conn;

    conn = _conn_get_for_reload();
    if (conn == NULL) {
        return NULL;
    }

    conn->redis = pool->redis;

    conn->proxy = 1;

    conn->recv = proxy_recv;
    conn->recv_next = NULL;
    conn->recv_done = NULL;

    conn->send = NULL;
    conn->send_next = NULL;
    conn->send_done = NULL;

    conn->close = proxy_close;
    conn->active = NULL;

    conn->ref = proxy_ref;
    conn->unref = proxy_unref;

    conn->enqueue_inq = NULL;
    conn->dequeue_inq = NULL;
    conn->enqueue_outq = NULL;
    conn->dequeue_outq = NULL;

    conn->ref(conn, owner);

    log_debug(LOG_VVERB, "get conn %p proxy %d", conn, conn->proxy);

    return conn;
}

struct conn *
conn_get_for_reload(void *owner)
{
    struct conn *conn;

    conn = _conn_get_for_reload();
    if (conn == NULL) {
        return NULL;
    }

    conn->redis = 0;

    conn->proxy = 0;

    conn->recv = NULL;
    conn->recv_next = NULL;
    conn->recv_done = NULL;

    conn->send = NULL;
    conn->send_next = NULL;
    conn->send_done = NULL;

    conn->close = NULL;
    conn->active = NULL;

    conn->ref = NULL;
    conn->unref = NULL;

    conn->enqueue_inq = NULL;
    conn->dequeue_inq = NULL;
    conn->enqueue_outq = NULL;
    conn->dequeue_outq = NULL;

	conn->owner = owner;

	conn->reload_conf = 1;

    log_debug(LOG_VVERB, "get conn %p proxy %d", conn, conn->proxy);

    return conn;
}

void
conn_put_for_reload(struct conn *conn)
{
    ASSERT(conn->sd < 0);
    ASSERT(conn->owner == NULL);

    log_debug(LOG_VVERB, "free conn %p", conn);

    ncurr_conn--;
	nc_free(conn);
}
#endif //shenzheng 2015-7-14 config-reload

#if 1 //shenzheng 2015-7-28 replace server
void conn_close_for_replace_server(struct conn *conn, int err)
{
	rstatus_t status;
	struct server *server;
	struct context *ctx;
	struct conn *c_conn;
	struct msg *pmsg, *msg;
	
	if(conn != NULL)
	{
		ASSERT(!conn->proxy && !conn->client);
		
		ctx = conn->ctx;
		server = conn->owner;
		
		ASSERT(ctx != NULL);

		status = event_del_conn(ctx->evb, conn);
	    if (status < 0) {
	        log_warn("event del conn server %d failed, ignored: %s",
	                 conn->sd, strerror(errno));
	    }

		ASSERT(server->ns_conn_q > 0);
		//conn = TAILQ_FIRST(&server->s_conn_q);
		conn->err = err;
		
		if(!TAILQ_EMPTY(&conn->imsg_q))
		{
			pmsg = TAILQ_FIRST(&conn->imsg_q);
			TAILQ_REMOVE(&conn->imsg_q, pmsg, s_tqe);
		}
		else if(!TAILQ_EMPTY(&conn->omsg_q))
		{
			pmsg = TAILQ_FIRST(&conn->omsg_q);
			TAILQ_REMOVE(&conn->omsg_q, pmsg, s_tqe);
		}
		else
		{
			NOT_REACHED();
		}

		ASSERT(pmsg->request);

		msg_tmo_delete(pmsg);

		log_debug(LOG_DEBUG, "pmsg->swallow %d", pmsg->swallow);

		conn->unref(conn);

		if(pmsg->swallow)
		{
			req_put(pmsg);
		}
		else
		{
			c_conn = pmsg->owner;

			ASSERT(!c_conn->proxy && c_conn->client);
			
			ASSERT(!TAILQ_EMPTY(&c_conn->omsg_q));

			pmsg->done = 1;
			pmsg->error = 1;
	        pmsg->err = conn->err;

	        if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
	            event_add_out(ctx->evb, c_conn);
	        }
		}
		
		ASSERT(TAILQ_EMPTY(&conn->imsg_q));
		ASSERT(TAILQ_EMPTY(&conn->omsg_q));

		ASSERT(conn->smsg == NULL);
		msg = conn->rmsg;
	    if (msg != NULL) {
	        conn->rmsg = NULL;

	        ASSERT(!msg->request);
	        ASSERT(msg->peer == NULL);

	        rsp_put(msg);

	        log_debug(LOG_INFO, "close s %d discarding rsp %"PRIu64" len %"PRIu32" "
	                  "in error", conn->sd, msg->id, msg->mlen);
	    }

	    status = close(conn->sd);
	    if (status < 0) {
	        log_error("close s %d failed, ignored: %s", conn->sd, strerror(errno));
	    }
	    conn->sd = -1;

	    conn_put(conn);

		ASSERT(server->ns_conn_q == 0);
		ASSERT(TAILQ_EMPTY(&server->s_conn_q));
	}
}
#endif //shenzheng 2015-7-28 replace server
