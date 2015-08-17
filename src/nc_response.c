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

#include <nc_core.h>
#include <nc_server.h>

#if 1 //shenzheng 2015-6-25 replace server
#include <nc_conf.h>
#endif //shenzheng 2015-6-25 replace server

struct msg *
rsp_get(struct conn *conn)
{
    struct msg *msg;

    ASSERT(!conn->client && !conn->proxy);

    msg = msg_get(conn, false, conn->redis);
    if (msg == NULL) {
        conn->err = errno;
    }

    return msg;
}

void
rsp_put(struct msg *msg)
{
    ASSERT(!msg->request);
    ASSERT(msg->peer == NULL);
    msg_put(msg);
}

static struct msg *
rsp_make_error(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg;        /* peer message (response) */
    struct msg *cmsg, *nmsg; /* current and next message (request) */
    uint64_t id;
    err_t err;

    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request && req_error(conn, msg));
    ASSERT(msg->owner == conn);

    id = msg->frag_id;
    if (id != 0) {
        for (err = 0, cmsg = TAILQ_NEXT(msg, c_tqe);
             cmsg != NULL && cmsg->frag_id == id;
             cmsg = nmsg) {
            nmsg = TAILQ_NEXT(cmsg, c_tqe);

            /* dequeue request (error fragment) from client outq */
            conn->dequeue_outq(ctx, conn, cmsg);
            if (err == 0 && cmsg->err != 0) {
                err = cmsg->err;
            }

            req_put(cmsg);
        }
    } else {
        err = msg->err;
    }

    pmsg = msg->peer;
    if (pmsg != NULL) {
        ASSERT(!pmsg->request && pmsg->peer == msg);
        msg->peer = NULL;
        pmsg->peer = NULL;
        rsp_put(pmsg);
    }
#if 1 //shenzheng 2014-12-4 common
	//attention: the new error macro we defined must be a negative number.
	if(err >= 0)
	{
#endif
	return msg_get_error(conn->redis, err);
#if 1 //shenzheng 2014-12-4 common
	}
	else
	{
    	return msg_get_error_other(conn->redis, err);
	}
#endif
}

struct msg *
rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    ASSERT(!conn->client && !conn->proxy);

    if (conn->eof) {
        msg = conn->rmsg;

        /* server sent eof before sending the entire request */
        if (msg != NULL) {
            conn->rmsg = NULL;

            ASSERT(msg->peer == NULL);
            ASSERT(!msg->request);

            log_error("eof s %d discarding incomplete rsp %"PRIu64" len "
                      "%"PRIu32"", conn->sd, msg->id, msg->mlen);

            rsp_put(msg);
        }

        /*
         * We treat TCP half-close from a server different from how we treat
         * those from a client. On a FIN from a server, we close the connection
         * immediately by sending the second FIN even if there were outstanding
         * or pending requests. This is actually a tricky part in the FA, as
         * we don't expect this to happen unless the server is misbehaving or
         * it crashes
         */
        conn->done = 1;
        log_error("s %d active %d is done", conn->sd, conn->active(conn));

        return NULL;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        ASSERT(!msg->request);
        return msg;
    }

    if (!alloc) {
        return NULL;
    }

    msg = rsp_get(conn);
    if (msg != NULL) {
        conn->rmsg = msg;
    }

    return msg;
}

static bool
rsp_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg;

    ASSERT(!conn->client && !conn->proxy);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "filter empty rsp %"PRIu64" on s %d", msg->id,
                  conn->sd);
        rsp_put(msg);
        return true;
    }

    pmsg = TAILQ_FIRST(&conn->omsg_q);
    if (pmsg == NULL) {
        log_debug(LOG_ERR, "filter stray rsp %"PRIu64" len %"PRIu32" on s %d",
                  msg->id, msg->mlen, conn->sd);
        rsp_put(msg);

        /*
         * Memcached server can respond with an error response before it has
         * received the entire request. This is most commonly seen for set
         * requests that exceed item_size_max. IMO, this behavior of memcached
         * is incorrect. The right behavior for update requests that are over
         * item_size_max would be to either:
         * - close the connection Or,
         * - read the entire item_size_max data and then send CLIENT_ERROR
         *
         * We handle this stray packet scenario in nutcracker by closing the
         * server connection which would end up sending SERVER_ERROR to all
         * clients that have requests pending on this server connection. The
         * fix is aggresive, but not doing so would lead to clients getting
         * out of sync with the server and as a result clients end up getting
         * responses that don't correspond to the right request.
         *
         * See: https://github.com/twitter/twemproxy/issues/149
         */
        conn->err = EINVAL;
        conn->done = 1;
        return true;
    }

    ASSERT(pmsg->peer == NULL);
    ASSERT(pmsg->request && !pmsg->done);

    if (pmsg->swallow) {
        conn->dequeue_outq(ctx, conn, pmsg);
        pmsg->done = 1;

        log_debug(LOG_INFO, "swallow rsp %"PRIu64" len %"PRIu32" of req "
                  "%"PRIu64" on s %d", msg->id, msg->mlen, pmsg->id,
                  conn->sd);

        rsp_put(msg);
        req_put(pmsg);
        return true;
    }

    return false;
}

static void
rsp_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(!msg->request);

    stats_server_incr(ctx, server, responses);
    stats_server_incr_by(ctx, server, response_bytes, msg->mlen);
}

static void
rsp_forward(struct context *ctx, struct conn *s_conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *pmsg;
    struct conn *c_conn;

    ASSERT(!s_conn->client && !s_conn->proxy);

    /* response from server implies that server is ok and heartbeating */
    server_ok(ctx, s_conn);

    /* dequeue peer message (request) from server */
    pmsg = TAILQ_FIRST(&s_conn->omsg_q);
    ASSERT(pmsg != NULL && pmsg->peer == NULL);
    ASSERT(pmsg->request && !pmsg->done);

    s_conn->dequeue_outq(ctx, s_conn, pmsg);
	
    pmsg->done = 1;

    /* establish msg <-> pmsg (response <-> request) link */
    pmsg->peer = msg;
    msg->peer = pmsg;

#if 1 //shenzheng 2015-6-25 replace server
	if(pmsg->replace_server)
	{
		log_debug(LOG_DEBUG, "msg->error : %d", msg->error);
		struct server_pool *sp;
		struct conf_server *cs;
		struct server *ser_curr, *ser_new;
		struct conf_pool *cp;
		struct string host;
		struct stats_pool *stp;
		struct stats_server *sts;
		uint32_t p_idx, s_idx;
		uint8_t *p, *q, *last;
		int k = 0;

		ser_new = s_conn->owner;

		ASSERT(pmsg->server == ser_new);

		ASSERT(pmsg->conf_version_curr == ctx->conf_version);
		
		while(msg->error == 0 && k < 1)
		{			
			string_init(&host);
			
			sp = ser_new->owner;
			
			p_idx = sp->idx;
			cp = array_get(&(ctx->cf->pool), p_idx);

			s_idx = ser_new->idx;
			
			ser_curr = array_get(&sp->server, s_idx);
			cs = array_get(&cp->server, s_idx);
			
			ASSERT(ser_curr->idx == ser_new->idx);
			ASSERT(ser_curr->owner == ser_new->owner);
			ASSERT(ser_curr->weight == ser_new->weight);
			ASSERT(ser_curr->name_null == ser_new->name_null);
			
			p = ser_new->pname.data;
			last = ser_new->pname.data + ser_new->pname.len;
			q = nc_strchr(p, last, ':');
			if(q == NULL || q >= last || q <= ser_new->pname.data)
			{
				log_debug(LOG_DEBUG, "new server address(%s) error", ser_new->pname.data);
				break;
			}
			
			string_copy(&host, ser_new->pname.data, (uint32_t)(q - ser_new->pname.data));
			log_debug(LOG_DEBUG, "new server host : %.*s", host.len, host.data);
			log_debug(LOG_DEBUG, "new server port : %d", ser_new->port);
			status = nc_resolve(&host, ser_new->port, &cs->info);
		    if (status != NC_OK) 
			{
				log_debug(LOG_DEBUG, "resolve new server address error(%d)", status);
				string_deinit(&host);
				break;
		    }
			
			k ++;
			while (!TAILQ_EMPTY(&ser_curr->s_conn_q)) {
				struct conn *conn;

				ASSERT(ser_curr->ns_conn_q > 0);		
				conn = TAILQ_FIRST(&ser_curr->s_conn_q);
				conn->err = ERROR_REPLACE_SERVER_TRY_AGAIN;
				status = event_del_conn(ctx->evb, conn);
				if (status < 0) {
					log_warn("event del conn s %d failed, ignored: %s",
				         conn->sd, strerror(errno));
				}

				conn->close(ctx, conn);
			}
			
			log_debug(LOG_DEBUG, "ser_curr->pname : %.*s", ser_curr->pname.len, ser_curr->pname.data);
			log_debug(LOG_DEBUG, "ser_new->pname : %.*s", ser_new->pname.len, ser_new->pname.data);
			status = conf_write_back_yaml(ctx, &ser_curr->pname, &ser_new->pname);
			if(status != NC_OK)
			{
				log_warn("warning: conf file write back error, but replace_server %.*s %.*s success.", 
					ser_curr->pname.len, ser_curr->pname.data,
					ser_new->pname.len, ser_new->pname.data);
			}
			
			string_deinit(&cs->pname);
			cs->pname = ser_new->pname;
			string_init(&ser_new->pname);
			ser_curr->pname = cs->pname;
			
			if(ser_curr->name_null)
			{
				string_deinit(&cs->name);
				cs->name = ser_new->name;
				string_init(&ser_new->name);
				ser_curr->name = cs->name;

				stp = array_get(&ctx->stats->current, p_idx);
				sts = array_get(&stp->server, s_idx);
				sts->name = ser_curr->name;

				stp = array_get(&ctx->stats->shadow, p_idx);
				sts = array_get(&stp->server, s_idx);
				sts->name = ser_curr->name;

				stp = array_get(&ctx->stats->sum, p_idx);
				sts = array_get(&stp->server, s_idx);
				sts->name = ser_curr->name;
			}
			
			ser_curr->port = ser_new->port;			
			
			ser_curr->family = cs->info.family;
		    ser_curr->addrlen = cs->info.addrlen;
		    ser_curr->addr = (struct sockaddr *)&cs->info.addr;
			
			ser_curr->next_retry = 0;
			ser_curr->failure_count = 0;
			
			string_deinit(&host);

			while (!TAILQ_EMPTY(&ser_new->s_conn_q)) {
				struct conn *conn;

				ASSERT(ser_new->ns_conn_q > 0);		
				conn = TAILQ_FIRST(&ser_new->s_conn_q);

				ASSERT(conn->replace_server == 1);
				conn->replace_server = 0;
				conn->conf_version_curr = -1;
				conn->ctx = NULL;
				
				conn->unref(conn);
				conn->ref(conn, ser_curr);
			}
		}
		
		
	}
#endif //shenzheng 2015-6-25 replace server

    msg->pre_coalesce(msg);

    c_conn = pmsg->owner;
    ASSERT(c_conn->client && !c_conn->proxy);

    if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
        status = event_add_out(ctx->evb, c_conn);
        if (status != NC_OK) {
            c_conn->err = errno;
        }
    }

    rsp_forward_stats(ctx, s_conn->owner, msg);
}

void
rsp_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg)
{
    ASSERT(!conn->client && !conn->proxy);
    ASSERT(msg != NULL && conn->rmsg == msg);
    ASSERT(!msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(nmsg == NULL || !nmsg->request);

    /* enqueue next message (response), if any */
    conn->rmsg = nmsg;

#if 1 //shenzheng 2015-8-10 for debug
	msg_print(msg, LOG_DEBUG);
#endif //shenzheng 2015-8-10 for debug

    if (rsp_filter(ctx, conn, msg)) {
        return;
    }

    rsp_forward(ctx, conn, msg);
}

struct msg *
rsp_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *pmsg; /* response and it's peer request */

    ASSERT(conn->client && !conn->proxy);

    pmsg = TAILQ_FIRST(&conn->omsg_q);

    if (pmsg == NULL || !req_done(conn, pmsg)) {
        /* nothing is outstanding, initiate close? */
        if (pmsg == NULL && conn->eof) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->sd);
        }
		status = event_del_out(ctx->evb, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }
        return NULL;
    }



    msg = conn->smsg;
    if (msg != NULL) {
        ASSERT(!msg->request && msg->peer != NULL);
        ASSERT(req_done(conn, msg->peer));
        pmsg = TAILQ_NEXT(msg->peer, c_tqe);
    }

    if (pmsg == NULL || !req_done(conn, pmsg)) {
		conn->smsg = NULL;     
        return NULL;
    }
    ASSERT(pmsg->request && !pmsg->swallow);

    if (req_error(conn, pmsg)) {
        msg = rsp_make_error(ctx, conn, pmsg);
        if (msg == NULL) {
            conn->err = errno;
            return NULL;
        }
        msg->peer = pmsg;
        pmsg->peer = msg;
        stats_pool_incr(ctx, conn->owner, forward_error);
    } else {
        msg = pmsg->peer;
    }
    ASSERT(!msg->request);

    conn->smsg = msg;

    log_debug(LOG_VVERB, "send next rsp %"PRIu64" on c %d", msg->id, conn->sd);

    return msg;
}

void
rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg; /* peer message (request) */

    ASSERT(conn->client && !conn->proxy);
    ASSERT(conn->smsg == NULL);

    log_debug(LOG_VVERB, "send done rsp %"PRIu64" on c %d", msg->id, conn->sd);

    pmsg = msg->peer;

    ASSERT(!msg->request && pmsg->request);
    ASSERT(pmsg->peer == msg);
    ASSERT(pmsg->done && !pmsg->swallow);

    /* dequeue request from client outq */
    conn->dequeue_outq(ctx, conn, pmsg);
  	req_put(pmsg);

}

#if 1 //shenzheng 2015-4-28 proxy administer
struct msg *
rsp_send_next_proxy_adm(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *pmsg; /* response and it's peer request */
	struct proxy_adm *padm = ctx->padm;
	ASSERT(padm != NULL);

    ASSERT(conn->client && !conn->proxy);

    pmsg = TAILQ_FIRST(&conn->omsg_q);

	msg_print(pmsg, LOG_DEBUG);

    if (pmsg == NULL || !req_done(conn, pmsg)) {
        /* nothing is outstanding, initiate close? */
        if (pmsg == NULL && conn->eof) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->sd);
        }
		status = event_del_out(padm->evb, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }
        return NULL;
    }



    msg = conn->smsg;
    if (msg != NULL) {
        ASSERT(!msg->request && msg->peer != NULL);
        ASSERT(req_done(conn, msg->peer));
        pmsg = TAILQ_NEXT(msg->peer, c_tqe);
    }

    if (pmsg == NULL || !req_done(conn, pmsg)) {
		conn->smsg = NULL;     
        return NULL;
    }
    ASSERT(pmsg->request && !pmsg->swallow);

	ASSERT(!req_error(conn, pmsg));
    msg = pmsg->peer;
    
    ASSERT(!msg->request);

    conn->smsg = msg;

    log_debug(LOG_VVERB, "send next rsp %"PRIu64" on c %d", msg->id, conn->sd);

    return msg;
}

void
rsp_send_done_proxy_adm(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg; /* peer message (request) */

    ASSERT(conn->client && !conn->proxy);
    ASSERT(conn->smsg == NULL);

    log_debug(LOG_VVERB, "send done rsp %"PRIu64" on c %d", msg->id, conn->sd);

	msg_print(msg, LOG_DEBUG);

    pmsg = msg->peer;

    ASSERT(!msg->request && pmsg->request);
    ASSERT(pmsg->peer == msg);
    ASSERT(pmsg->done && !pmsg->swallow);

    /* dequeue request from client outq */
    conn->dequeue_outq(ctx, conn, pmsg);
  	req_put_proxy_adm(pmsg);
}

#endif //shenzheng 2015-4-28 proxy administer
