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

struct msg *
req_get(struct conn *conn)
{
    struct msg *msg;

    ASSERT(conn->client && !conn->proxy);

    msg = msg_get(conn, true, conn->redis);
    if (msg == NULL) {
        conn->err = errno;
    }
    return msg;
}

static void
req_log(struct msg *req)
{
    struct msg *rsp;           /* peer message (response) */
    int64_t req_time;          /* time cost for this request */
    char *peer_str;            /* peer client ip:port */
    uint32_t req_len, rsp_len; /* request and response length */
    struct string *req_type;   /* request type string */
    struct keypos *kpos;

    if (log_loggable(LOG_NOTICE) == 0) {
        return;
    }

    /* a fragment? */
    if (req->frag_id != 0 && req->frag_owner != req) {
        return;
    }

    /* conn close normally? */
    if (req->mlen == 0) {
        return;
    }
    /*
     * there is a race scenario where a requests comes in, the log level is not LOG_NOTICE,
     * and before the response arrives you modify the log level to LOG_NOTICE
     * using SIGTTIN OR SIGTTOU, then req_log() wouldn't have msg->start_ts set
     */
    if (req->start_ts == 0) {
        return;
    }

    req_time = nc_usec_now() - req->start_ts;

    rsp = req->peer;
    req_len = req->mlen;
    rsp_len = (rsp != NULL) ? rsp->mlen : 0;

    if (array_n(req->keys) < 1) {
        return;
    }

    kpos = array_get(req->keys, 0);
    if (kpos->end != NULL) {
        *(kpos->end) = '\0';
    }

    /*
     * FIXME: add backend addr here
     * Maybe we can store addrstr just like server_pool in conn struct
     * when connections are resolved
     */
    peer_str = nc_unresolve_peer_desc(req->owner->sd);

    req_type = msg_type_string(req->type);

#if 1 //shenzheng 2014-9-2 comman
	log_debug(LOG_VERB, "req %"PRIu64" done on c %d req_time %"PRIi64".%03"PRIi64
#else //shenzheng 2014-9-2 comman
	 log_debug(LOG_NOTICE, "req %"PRIu64" done on c %d req_time %"PRIi64".%03"PRIi64
#endif //shenzheng 2014-9-2 comman
              " msec type %.*s narg %"PRIu32" req_len %"PRIu32" rsp_len %"PRIu32
              " key0 '%s' peer '%s' done %d error %d",
              req->id, req->owner->sd, req_time / 1000, req_time % 1000,
              req_type->len, req_type->data, req->narg, req_len, rsp_len,
              kpos->start, peer_str, req->done, req->error);
}

void
req_put(struct msg *msg)
{
    struct msg *pmsg; /* peer message (response) */

    ASSERT(msg->request);

#if 1 //shenzheng 2015-1-19 replication pool
	if(!msg->frag_id && msg->replication_mode == 1 && !msg->noreply && !msg->swallow)
	{
		/*
		if(!msg->self_done || !replication_msgs_done(msg))
		{
			return;
		}
		*/
		
		if(msg->nreplication_msgs >= 0)
		{
			if(!replication_msgs_done(msg))
			{
				return;
			}
		}
		else
		{
			struct msg *master_msg;
			master_msg = msg->master_msg;
			//if(master_msg == NULL)
			//{
			//	msg_print(msg, LOG_ERR);
			//}
			ASSERT(master_msg != NULL);
			ASSERT(master_msg->nreplication_msgs >= 0);
			ASSERT(master_msg->request);
			if(!replication_msgs_done(master_msg))
			{
				return;
			}
		}
	}
	
	if(!msg->master_send)
	{
#endif //shenzheng 2015-1-19 replication pool
	req_log(msg);
#if 1 //shenzheng 2015-1-19 replication pool
	}	
#endif //shenzheng 2015-1-19 replication pool

    pmsg = msg->peer;
    if (pmsg != NULL) {
        ASSERT(!pmsg->request && pmsg->peer == msg);
        msg->peer = NULL;
        pmsg->peer = NULL;
        rsp_put(pmsg);
    }
	
#if 1 //shenzheng 2015-1-15 replication pool
	msg_print(msg, LOG_DEBUG);

	if(msg->nreplication_msgs > 0 && msg->replication_msgs != NULL)
	{
		struct msg *replication_msg;
		int i;
		for(i = 0; i < msg->nreplication_msgs; i ++)
		{
			replication_msg = msg->replication_msgs[i];
			if(replication_msg == NULL)
			{
				log_debug(LOG_DEBUG, "replication_msg[%d] is null", i);
				continue;
			}
			else
			{
				msg->replication_msgs[i] = NULL;
			}
			
			if(replication_msg->swallow && !replication_msg->self_done)
			{
				ASSERT(replication_msg->master_msg != NULL);
				replication_msg->master_msg = NULL;
				continue;
			}
			
			ASSERT(replication_msg != NULL);

			log_debug(LOG_DEBUG, " replication_msg->master_msg->id: %d", replication_msg->master_msg->id);
			msg_print(replication_msg, LOG_DEBUG);

			ASSERT(replication_msg->master_msg != NULL);
			
			ASSERT(replication_msg->request);
			ASSERT(replication_msg->server_pool_id >= 0);
			ASSERT(replication_msg->master_msg->server_pool_id == -1);
			pmsg = replication_msg->peer;
    		if (pmsg != NULL) {
       		 	ASSERT(!pmsg->request && pmsg->peer == replication_msg);
       		 	replication_msg->peer = NULL;
        		pmsg->peer = NULL;
        		rsp_put(pmsg);
    		}

			msg_tmo_delete(replication_msg);
			msg_put(replication_msg);
		}
		nc_free(msg->replication_msgs);
		msg->replication_msgs = NULL;
	}
#endif //shenzheng 2015-1-15 replication pool
	
    msg_tmo_delete(msg);

    msg_put(msg);
}

/*
 * Return true if request is done, false otherwise
 *
 * A request is done, if we received response for the given request.
 * A request vector is done if we received responses for all its
 * fragments.
 */
bool
req_done(struct conn *conn, struct msg *msg)
{
    struct msg *cmsg, *pmsg; /* current and previous message */
    uint64_t id;             /* fragment id */
    uint32_t nfragment;      /* # fragment */

    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);

    if (!msg->done) {
        return false;
    }

    id = msg->frag_id;
    if (id == 0) {
        return true;
    }

    if (msg->fdone) {
        /* request has already been marked as done */
        return true;
    }

    if (msg->nfrag_done < msg->nfrag) {
        return false;
    }

    /* check all fragments of the given request vector are done */

    for (pmsg = msg, cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (!cmsg->done) {
            return false;
        }
    }

    for (pmsg = msg, cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (!cmsg->done) {
            return false;
        }
    }

    /*
     * At this point, all the fragments including the last fragment have
     * been received.
     *
     * Mark all fragments of the given request vector to be done to speed up
     * future req_done calls for any of fragments of this request
     */

    msg->fdone = 1;
    nfragment = 0;

    for (pmsg = msg, cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;
    }

    for (pmsg = msg, cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;
    }

    ASSERT(msg->frag_owner->nfrag == nfragment);

    msg->post_coalesce(msg->frag_owner);

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is done", conn->sd, id, nfragment);

    return true;
}


/*
 * Return true if request is in error, false otherwise
 *
 * A request is in error, if there was an error in receiving response for the
 * given request. A multiget request is in error if there was an error in
 * receiving response for any its fragments.
 */
bool
req_error(struct conn *conn, struct msg *msg)
{
    struct msg *cmsg; /* current message */
    uint64_t id;
    uint32_t nfragment;
	
#if 1 //shenzheng 2015-3-2 common
	msg_print(msg, LOG_DEBUG);
#endif //shenzheng 2015-3-2 common

    ASSERT(msg->request && req_done(conn, msg));

    if (msg->error) {
        return true;
    }

    id = msg->frag_id;
    if (id == 0) {
        return false;
    }

    if (msg->ferror) {
        /* request has already been marked to be in error */
        return true;
    }

    /* check if any of the fragments of the given request are in error */

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (cmsg->error) {
            goto ferror;
        }
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (cmsg->error) {
            goto ferror;
        }
    }

    return false;

ferror:

    /*
     * Mark all fragments of the given request to be in error to speed up
     * future req_error calls for any of fragments of this request
     */

    msg->ferror = 1;
    nfragment = 1;

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->ferror = 1;
        nfragment++;
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->ferror = 1;
        nfragment++;
    }

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is in error", conn->sd, id, nfragment);

    return true;
}

void
req_server_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    /*
     * timeout clock starts ticking the instant the message is enqueued into
     * the server in_q; the clock continues to tick until it either expires
     * or the message is dequeued from the server out_q
     *
     * noreply request are free from timeouts because client is not intrested
     * in the reponse anyway!
     */
    if (!msg->noreply) {
        msg_tmo_insert(msg, conn);
    }

    TAILQ_INSERT_TAIL(&conn->imsg_q, msg, s_tqe);

    stats_server_incr(ctx, conn->owner, in_queue);
    stats_server_incr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

void
req_server_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_REMOVE(&conn->imsg_q, msg, s_tqe);

    stats_server_decr(ctx, conn->owner, in_queue);
    stats_server_decr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

void
req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->client && !conn->proxy);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);
}

void
req_server_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, s_tqe);

    stats_server_incr(ctx, conn->owner, out_queue);
    stats_server_incr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

void
req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->client && !conn->proxy);

    TAILQ_REMOVE(&conn->omsg_q, msg, c_tqe);
#if 1 //shenzheng 2015-4-15 replication pool
	msg->master_send = 1;
#endif //shenzheng 2015-4-15 replication pool
}

void
req_server_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    msg_tmo_delete(msg);

    TAILQ_REMOVE(&conn->omsg_q, msg, s_tqe);

    stats_server_decr(ctx, conn->owner, out_queue);
    stats_server_decr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

struct msg *
req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    ASSERT(conn->client && !conn->proxy);

    if (conn->eof) {
        msg = conn->rmsg;

        /* client sent eof before sending the entire request */
        if (msg != NULL) {
            conn->rmsg = NULL;

            ASSERT(msg->peer == NULL);
            ASSERT(msg->request && !msg->done);

            log_error("eof c %d discarding incomplete req %"PRIu64" len "
                      "%"PRIu32"", conn->sd, msg->id, msg->mlen);

            req_put(msg);
        }

        /*
         * TCP half-close enables the client to terminate its half of the
         * connection (i.e. the client no longer sends data), but it still
         * is able to receive data from the proxy. The proxy closes its
         * half (by sending the second FIN) when the client has no
         * outstanding requests
         */
        if (!conn->active(conn)) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->sd);
        }
        return NULL;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        ASSERT(msg->request);
        return msg;
    }

    if (!alloc) {
        return NULL;
    }

    msg = req_get(conn);
    if (msg != NULL) {
        conn->rmsg = msg;
    }
	
    return msg;
}

static rstatus_t
req_make_reply(struct context *ctx, struct conn *conn, struct msg *req)
{
    struct msg *msg;

    msg = msg_get(conn, true, conn->redis); /* replay */
    if (msg == NULL) {
        conn->err = errno;
        return NC_ENOMEM;
    }

    req->peer = msg;
    msg->peer = req;
    msg->request = 0;

    req->done = 1;
    conn->enqueue_outq(ctx, conn, req);
    return NC_OK;
}

#if 1 //shenzheng 2014-9-3 replace server
static int 
server_pname_compare(struct string * pname, struct string * ser_addr)
{
	uint32_t pos;
	bool equal_flag = true;

	if(pname->len <= ser_addr->len)
	{
		return -1;
	}
	
	for(pos = 0; pos < ser_addr->len; pos ++)
	{
		if(*(pname->data + pos) != *(ser_addr->data + pos))
		{
			equal_flag = false;
			break;
		}
	}

	if(equal_flag == false)
	{
		return -1;
	}

	if(':' != *(pname->data + pos))
	{
		return -1;
	}
	
	return 0;
}

static rstatus_t
req_examine_replace_server(struct context *ctx, struct conn *conn, struct msg *msg)
{
	rstatus_t status;
	uint32_t i, nelem;
	bool find_flag = false;
	char *content = NULL;
	struct server_pool *pool;
	struct server *ser;
	struct keypos *kp;
	struct string old_ser_addr, new_ser_addr;
	struct string old_ser_host, new_ser_host;
	struct string old_ser_port_str, new_ser_port_str;
	int old_ser_port, new_ser_port;
	uint8_t *p, *q, *last;

	log_debug(LOG_DEBUG, "array_n(msg->keys) : %d", array_n(msg->keys));

	msg->conf_version_curr = ctx->conf_version;

	if(array_n(msg->keys) != 2)
	{
		content = "-command args error!(example : "
			"replace_server old_ip:old_port new_ip:new_port) \r\n";
		goto error;
	}
	
	pool = conn->owner;

	kp = array_get(msg->keys, 0);
	old_ser_addr.data = kp->start;
	old_ser_addr.len = (uint32_t)(kp->end - kp->start);
	log_debug(LOG_DEBUG, "old_ser_addr.len : %d", old_ser_addr.len);
	log_debug(LOG_DEBUG, "old_ser_addr.data : %.*s", old_ser_addr.len, old_ser_addr.data);

	kp = array_get(msg->keys, 1);
	new_ser_addr.data = kp->start;
	new_ser_addr.len = (uint32_t)(kp->end - kp->start);
	log_debug(LOG_DEBUG, "new_ser_addr.len : %d", new_ser_addr.len);
	log_debug(LOG_DEBUG, "new_ser_addr.data : %.*s", new_ser_addr.len, new_ser_addr.data);
	
	ASSERT(old_ser_addr.len > 0 && new_ser_addr.len > 0);


	p = old_ser_addr.data;
	last = old_ser_addr.data + old_ser_addr.len;

	q = nc_strchr(p, last, ':');
	if(q == NULL || q >= last || q <= old_ser_addr.data)
	{
		content = "-first args error!(example : "
			"replace_server old_ip:old_port new_ip:new_port) \r\n";
		goto error;
	}
	p = q + 1;
	if(nc_strchr(p, last, ':') != NULL)
	{
		content = "-first args error!(example : "
			"replace_server old_ip:old_port new_ip:new_port) \r\n";
		goto error;
	}

	old_ser_host.data = old_ser_addr.data;
	old_ser_host.len = (uint32_t)(q - old_ser_addr.data);
	log_debug(LOG_DEBUG, "old_ser_host: %.*s", old_ser_host.len, old_ser_host.data);

	old_ser_port_str.data = q + 1;
	old_ser_port_str.len = (uint32_t)(last - old_ser_port_str.data);
	log_debug(LOG_DEBUG, "old_ser_port_str: %.*s", old_ser_port_str.len, old_ser_port_str.data);

	old_ser_port = nc_atoi(old_ser_port_str.data, old_ser_port_str.len);	
	log_debug(LOG_DEBUG, "old_ser_port: %d", old_ser_port);
	
	if(nc_valid_port(old_ser_port) == false)
	{
		content = "-the port in first args invalid!(example : "
			"replace_server old_ip:old_port new_ip:new_port) \r\n";
		goto error;
	}
	
	
	p = new_ser_addr.data;
	last = new_ser_addr.data + new_ser_addr.len;

	q = nc_strchr(p, last, ':');
	if(q == NULL || q >= last)
	{
		goto error;
	}
	p = q + 1;
	if(nc_strchr(p, last, ':') != NULL)
	{
		content = "-first args error!(example : "
			"replace_server old_ip:old_port new_ip:new_port) \r\n";
		goto error;
	}

	new_ser_host.data = new_ser_addr.data;
	new_ser_host.len = (uint32_t)(q - new_ser_addr.data);

	new_ser_port_str.data = q + 1;
	new_ser_port_str.len = (uint32_t)(last - new_ser_port_str.data);

	new_ser_port = nc_atoi(new_ser_port_str.data, new_ser_port_str.len);
	if(nc_valid_port(new_ser_port) == false)
	{
		content = "-the port in second args invalid!(example : "
			"replace_server old_ip:old_port new_ip:new_port) \r\n";
		goto error;
	}
	
	log_debug(LOG_DEBUG, "new_ser_host: %.*s", new_ser_host.len, new_ser_host.data);
	log_debug(LOG_DEBUG, "new_ser_port_str: %.*s", new_ser_port_str.len, new_ser_port_str.data);
	log_debug(LOG_DEBUG, "new_ser_port: %d", new_ser_port);	
	
	for (i = 0, nelem = array_n(&pool->server); i < nelem; i++) 
	{
		ser = array_get(&pool->server, i);
		if(0 == server_pname_compare(&ser->pname, &old_ser_addr))
		{
			log_debug(LOG_DEBUG, "target server idx is %d", ser->idx);
			msg->mser_idx = ser->idx;
			find_flag = true;
			break;
		}
	}
	
	if(find_flag == false)
	{
		content = "-the modifid server is not in the current server pool!\r\n";
		goto error;
	}

	return NC_OK;

error:

	msg->replace_server = 0;
	status = req_make_reply(ctx, conn, msg);
	if (status != NC_OK) {
		conn->err = errno;
		return status;
	}

	status = msg_append(msg->peer, (uint8_t *)content, (size_t)strlen(content));
	if (status != NC_OK) {
		conn->err = errno;
		return status;
	}

	status = event_add_out(ctx->evb, conn);
	if (status != NC_OK) {
		conn->err = errno;
		return status;
	}

	return NC_ERROR;
}

#endif //shenzheng 2015-6-24 replace server

static bool
req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(conn->client && !conn->proxy);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "filter empty req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        req_put(msg);
        return true;
    }

    /*
     * Handle "quit\r\n", which is the protocol way of doing a
     * passive close
     */
    if (msg->quit) {
        //ASSERT(conn->rmsg == NULL);
        log_debug(LOG_INFO, "filter quit req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        conn->eof = 1;
        conn->recv_ready = 0;
        req_put(msg);
        return true;
    }

    /*
     * if this conn is not authenticated, we will mark it as noforward,
     * and handle it in the redis_reply handler.
     *
     */
    if (conn->need_auth) {
        msg->noforward = 1;
    }
	
#if 1 //shenzheng 2015-6-23 replace server
	if(msg->replace_server)
	{
		if(req_examine_replace_server(ctx, conn, msg) != NC_OK)
		{
			return true;
		}		
	}
#endif //shenzheng 2015-6-23 replace server

    return false;
}

static void
req_forward_error(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    ASSERT(conn->client && !conn->proxy);

    log_debug(LOG_INFO, "forward req %"PRIu64" len %"PRIu32" type %d from "
              "c %d failed: %s", msg->id, msg->mlen, msg->type, conn->sd,
              strerror(errno));

#if 1 //shenzheng 2015-3-3  replication pool
#else //shenzheng 2015-3-3  replication pool
    msg->done = 1;
#endif //shenzheng 2015-3-3  replication pool
#if 1 //shenzheng 2015-3-3  replication pool
	msg->self_done = 1;
#endif //shenzheng 2015-3-3  replication pool
    msg->error = 1;
    msg->err = errno;

#if 1 //shenzheng 2015-3-3  replication pool
	if(msg->frag_id)
	{
		if(req_need_penetrate(msg, msg->peer))
		{
			req_penetrate(ctx, conn, msg, msg->peer);
		}
		else
		{
			msg->done = 1;
		}
	}
	else if(msg->server_pool_id == -2)
	{
		//ASSERT(msg->replication_mode != 0);
		msg->handle_result(ctx, conn, msg);
		req_put(msg);
		return;
	}
#endif //shenzheng 2015-3-3  replication pool

    /* noreply request don't expect any response */
    if (msg->noreply) {
        req_put(msg);
        return;
    }

    if (req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = event_add_out(ctx->evb, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }
    }
}

static void
req_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(msg->request);

    stats_server_incr(ctx, server, requests);
    stats_server_incr_by(ctx, server, request_bytes, msg->mlen);
}

#if 1 //shenzheng 2015-1-7 replication pool
void
#else
static void
#endif //shenzheng 2015-1-7 replication pool
req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg)
{
    rstatus_t status;
    struct conn *s_conn;
    struct server_pool *pool;
    uint8_t *key;
    uint32_t keylen;
    struct keypos *kpos;

    ASSERT(c_conn->client && !c_conn->proxy);

    /* enqueue message (request) into client outq, if response is expected */
    if (!msg->noreply) {
#if 1 //shenzheng 2014-12-17 replication pool
		//if(msg->server_pool_id == -1 || !msg->frag_id)
		if(msg->server_pool_id == -1)
		{
#endif //shenzheng 2014-12-26 replication pool
        c_conn->enqueue_outq(ctx, c_conn, msg);
#if 1 //shenzheng 2014-12-17 replication pool
		}
#endif //shenzheng 2014-12-17 replication pool
    }

    pool = c_conn->owner;
    ASSERT(array_n(msg->keys) > 0);
    kpos = array_get(msg->keys, 0);
    key = kpos->start;
    keylen = (uint32_t)(kpos->end - kpos->start);

#if 1 //shenzheng 2015-1-7 replication pool
	int i, nreplication_request;
	nreplication_request = c_conn->nreplication_request;
	log_debug(LOG_DEBUG, "replication_requests : %d\n", nreplication_request);
	
	i = 0;
	
	if(msg->frag_id)
	{
		i = nreplication_request;
		
		if(msg->server_pool_id == -1)
		{
			s_conn = server_pool_conn(ctx, c_conn->owner, key, keylen);
		}
		else if(msg->server_pool_id >= 0)
		{
			ASSERT(msg->server_pool_id < c_conn->nreplication_request);
			struct server_pool **sp_slave = array_get(&pool->replication_pools, msg->server_pool_id);
			s_conn = server_pool_conn(ctx, *sp_slave, key, keylen);
		}
		else
		{
			s_conn = NULL;
			log_debug(LOG_ERR, "error: msg(%d)->server_pool_id < -1!", msg->id);
			
		}
	}
	else
	{
#endif //shenzheng 2015-1-7 replication pool

#if 1 //shenzheng 2015-6-24 replace server
		if(1 == msg->replace_server)
		{
			ASSERT(msg->conf_version_curr == ctx->conf_version);
			log_debug(LOG_DEBUG, "msg->mser_idx : %d", msg->mser_idx);
			log_debug(LOG_DEBUG, "array_n(&pool->server) : %d", array_n(&pool->server));
			if(array_n(&pool->server) <= msg->mser_idx)
			{
				s_conn = NULL; 
			}
			else
			{
				s_conn = server_pool_conn_for_replace(ctx, c_conn->owner, msg);
				if(s_conn != NULL)
				{
					struct mbuf *mbuf;
					char *content = NULL;
					
					while(!STAILQ_EMPTY(&msg->mhdr))
					{
						mbuf = STAILQ_FIRST(&msg->mhdr);
						mbuf_remove(&msg->mhdr, mbuf);
						mbuf_put(mbuf);
					}
					msg->mlen = 0;
					content = "*1\r\n$4\r\nping\r\n";
					status = msg_append(msg, (uint8_t *)content, (size_t)strlen(content));
					if(status != NC_OK)
					{
						struct server *ser;
						ser = s_conn->owner;
						server_close(ctx, s_conn);
						s_conn = NULL;
						server_close_for_replace_server(ser);
					}
				}
			}
		}
		else
		{
#endif //shenzheng 2015-6-24 replace server
    s_conn = server_pool_conn(ctx, c_conn->owner, key, keylen);
#if 1 //shenzheng 2015-6-24 replace server
		}
#endif //shenzheng 2015-6-24 replace server

#if 1 //shenzheng 2015-1-7 replication pool
		if(msg->server_pool_id == -2)
		{
			i = nreplication_request;
		}
		else if(msg->noreply && msg->replication_mode != 0)
		{
			msg->replication_mode = 0;
		}
	}
#endif //shenzheng 2015-3-9 replication pool

#if 1 //shenzheng 2014-12-23 replication pool
	struct msg *msg_replication, *msg_origin;
	msg_origin = msg;
	msg_origin->nreplication_msgs = nreplication_request - i;

	//if(pool->replication_mode && msg_origin->nreplication_msgs > 0)
	if(msg_origin->replication_mode && msg_origin->nreplication_msgs > 0)
	{
		msg_origin->replication_msgs = nc_zalloc(msg_origin->nreplication_msgs * sizeof(*msg_origin->replication_msgs));
		if(msg_origin->replication_msgs == NULL)
		{
			log_error("error: nc_zalloc msg(%d)->replication_msgs(nreplication_msgs:%d) error!", msg_origin->id, msg_origin->nreplication_msgs);
			msg_origin->nreplication_msgs = 0;
			i = nreplication_request;
		}
		else
		{
			int j;
			for(j = 0; j < msg_origin->nreplication_msgs; j ++)
			{
				msg_origin->replication_msgs[j] = NULL;
			}
		}
	}
replication_loop:
	if(i < nreplication_request)
	{
		msg_replication = msg_copy_for_replication(c_conn, msg_origin, i);
		
	}
	if(msg == NULL)
	{
		log_debug(LOG_DEBUG, "msg is null");
		log_debug(LOG_DEBUG, "i: %d", i);
		if(i < nreplication_request)
		{
			goto next_message;
		}
		return;
	}
#endif //shenzheng 2014-12-26 replication pool

    if (s_conn == NULL) {
#if 1 //shenzheng 2015-3-2 common
		log_debug(LOG_DEBUG, "s_conn is null");
		log_debug(LOG_DEBUG, "i: %d", i);
		msg_print(msg, LOG_DEBUG);
#endif //shenzheng 2015-3-2 common

#if 1 //shenzheng 2015-4-9 replication pool
		if(msg->frag_id)
		{
			
		}
#endif //shenzheng 2015-4-9 replication pool

        req_forward_error(ctx, c_conn, msg);

#if 1 //shenzheng 2015-3-3 replication pool
		if(i < nreplication_request)
		{
			goto next_message;
		}
#endif //shenzheng 2015-3-3 replication pool

        return;
    }
    ASSERT(!s_conn->client && !s_conn->proxy);

    /* enqueue the message (request) into server inq */
    if (TAILQ_EMPTY(&s_conn->imsg_q)) {
        status = event_add_out(ctx->evb, s_conn);
        if (status != NC_OK) {
#if 1 //shenzheng 2015-3-2 common
			log_debug(LOG_DEBUG, "event_add_out error");
#endif //shenzheng 2015-3-2 common
            req_forward_error(ctx, c_conn, msg);
            s_conn->err = errno;
#if 1 //shenzheng 2015-3-3 replication pool
			if(i < nreplication_request)
			{
				goto next_message;
			}
#endif //shenzheng 2015-3-3 replication pool
            return;
        }
    }

    if (s_conn->need_auth) {
        status = msg->add_auth(ctx, c_conn, s_conn);
        if (status != NC_OK) {
#if 1 //shenzheng 2015-3-2 common
			log_debug(LOG_DEBUG, "add_auth error");
#endif //shenzheng 2015-3-2 common
            req_forward_error(ctx, c_conn, msg);
            s_conn->err = errno;
#if 1 //shenzheng 2015-3-3 replication pool
			if(i < nreplication_request)
			{
				goto next_message;
			}
#endif //shenzheng 2015-3-3 replication pool
            return;
        }
    }

    s_conn->enqueue_inq(ctx, s_conn, msg);

    req_forward_stats(ctx, s_conn->owner, msg);

    log_debug(LOG_VERB, "forward from c %d to s %d req %"PRIu64" len %"PRIu32
              " type %d with key '%.*s'", c_conn->sd, s_conn->sd, msg->id,
              msg->mlen, msg->type, keylen, key);

#if 1 //shenzheng 2014-12-23 replication pool
next_message:
	if(i < nreplication_request)
	{
		
		msg = msg_replication;
		if(msg != NULL)
		{
			struct server_pool **sp_slave = array_get(&pool->replication_pools, i);

			s_conn = server_pool_conn(ctx, *sp_slave, key, keylen);
			//if (!msg->noreply || pool->replication_mode == 1)
			if(!msg->noreply)
			{
	    		ASSERT(msg->request);
				ASSERT(c_conn->client && !c_conn->proxy);

				msg_origin->replication_msgs[i] = msg;
				msg->master_msg = msg_origin;
			}
			ASSERT(array_n(msg->keys) > 0);
			kpos = array_get(msg->keys, 0);
			key = kpos->start;
			keylen = (uint32_t)(kpos->end - kpos->start);
		}
		
		i++;
		goto replication_loop;
	}
#endif //shenzheng 2015-3-3 replication pool
}

void
req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg)
{
    rstatus_t status;
    struct server_pool *pool;
    struct msg_tqh frag_msgq;
    struct msg *sub_msg;
    struct msg *tmsg; 			/* tmp next message */

    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(conn->rmsg == msg);
    ASSERT(nmsg == NULL || nmsg->request);

    /* enqueue next message (request), if any */
    conn->rmsg = nmsg;

#if 0 //shenzheng 2015-3-24 common
	msg_print(msg, LOG_ALERT);
#endif //shenzheng 2015-3-24 common

    if (req_filter(ctx, conn, msg)) {
        return;
    }

    if (msg->noforward) {
        status = req_make_reply(ctx, conn, msg);
        if (status != NC_OK) {
            conn->err = errno;
            return;
        }

        status = msg->reply(msg);
        if (status != NC_OK) {
            conn->err = errno;
            return;
        }

        status = event_add_out(ctx->evb, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }

        return;
    }

    /* do fragment */
    pool = conn->owner;
	
    TAILQ_INIT(&frag_msgq);
    status = msg->fragment(msg, pool->ncontinuum, &frag_msgq);
    if (status != NC_OK) {
        if (!msg->noreply) {
            conn->enqueue_outq(ctx, conn, msg);
        }
#if 1 //shenzheng 2015-3-2 common
		log_debug(LOG_DEBUG, "fragment error");
#endif //shenzheng 2015-3-2 common
        req_forward_error(ctx, conn, msg);
    }

    /* if no fragment happened */
    if (TAILQ_EMPTY(&frag_msgq)) {

        req_forward(ctx, conn, msg);
        return;
    }

    status = req_make_reply(ctx, conn, msg);
    if (status != NC_OK) {
        if (!msg->noreply) {
            conn->enqueue_outq(ctx, conn, msg);
        }
        req_forward_error(ctx, conn, msg);
    }

    for (sub_msg = TAILQ_FIRST(&frag_msgq); sub_msg != NULL; sub_msg = tmsg) {
        tmsg = TAILQ_NEXT(sub_msg, m_tqe);

        TAILQ_REMOVE(&frag_msgq, sub_msg, m_tqe);
        req_forward(ctx, conn, sub_msg);
    }

    ASSERT(TAILQ_EMPTY(&frag_msgq));
    return;
}

struct msg *
req_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */

    ASSERT(!conn->client && !conn->proxy);

    if (conn->connecting) {
        server_connected(ctx, conn);
    }

    nmsg = TAILQ_FIRST(&conn->imsg_q);
    if (nmsg == NULL) {
        /* nothing to send as the server inq is empty */
        status = event_del_out(ctx->evb, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }

        return NULL;
    }

    msg = conn->smsg;
    if (msg != NULL) {
        ASSERT(msg->request && !msg->done);
        nmsg = TAILQ_NEXT(msg, s_tqe);
    }

    conn->smsg = nmsg;

    if (nmsg == NULL) {
        return NULL;
    }
	
#if 1 //shenzheng 2015-3-2 common
	msg_print(nmsg, LOG_DEBUG);
#endif //shenzheng 2015-3-2 common

    ASSERT(nmsg->request && !nmsg->done);

    log_debug(LOG_VVERB, "send next req %"PRIu64" len %"PRIu32" type %d on "
              "s %d", nmsg->id, nmsg->mlen, nmsg->type, conn->sd);

    return nmsg;
}

void
req_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(!conn->client && !conn->proxy);
    ASSERT(msg != NULL && conn->smsg == NULL);
    ASSERT(msg->request && !msg->done);
    ASSERT(msg->owner != conn);

    log_debug(LOG_VVERB, "send done req %"PRIu64" len %"PRIu32" type %d on "
              "s %d", msg->id, msg->mlen, msg->type, conn->sd);

    /* dequeue the message (request) from server inq */
    conn->dequeue_inq(ctx, conn, msg);

    /*
     * noreply request instructs the server not to send any response. So,
     * enqueue message (request) in server outq, if response is expected.
     * Otherwise, free the noreply request
     */
    if (!msg->noreply) {
        conn->enqueue_outq(ctx, conn, msg);
    } else {
        req_put(msg);
    }
}

#if 1 //shenzheng 2015-4-2 replication pool
bool req_need_penetrate(struct msg *pmsg, struct msg *msg)
{
	struct conn *c_conn;
	struct server_pool *sp;

	ASSERT(pmsg != NULL);
    ASSERT(pmsg->request && !pmsg->done);

	c_conn = pmsg->owner;
	ASSERT(c_conn->client && !c_conn->proxy);
	
	sp = c_conn->owner;
	ASSERT(sp != NULL);
	log_debug(LOG_DEBUG, "sp->penetrate_mode: %d", sp->penetrate_mode);

	if(pmsg->frag_id == 0)
	{
		return false;
	}

	if(pmsg->server_pool_id >= c_conn->nreplication_request - 1)
	{
		return false;
	}

	if(msg == NULL)
	{
		ASSERT(pmsg->error == 1);
	}
	else
	{
		ASSERT(!msg->request);
	}

	if(sp->penetrate_mode == 0)
	{
		if(!pmsg->error && array_n(msg->keys) < array_n(pmsg->keys))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	else if(sp->penetrate_mode == 1)
	{
		if(pmsg->error)
		{
			if(msg != NULL)
			{
				ASSERT(array_n(msg->keys) == 0);
			}
			ASSERT(array_n(pmsg->keys) > 0);
			return true;
		}
		else
		{
			return false;
		}
	}
	else if(sp->penetrate_mode == 2)
	{
		if(pmsg->error || array_n(msg->keys) < array_n(pmsg->keys))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	else if(sp->penetrate_mode == 3)
	{
		return false;
	}
	else
	{
		log_error("error: server_pool[%s]'s penetrate_mode is %d!(penetrate_mode only can be 0, 1, 2 or 3.)", 
			sp->name.data, sp->penetrate_mode);
		return false;
	}
	
	return false;
}

void
req_penetrate(struct context *ctx, struct conn *s_conn, struct msg *pmsg, struct msg *msg)
{
	rstatus_t status;
	struct server_pool *sp;
	struct conn *c_conn;

	ASSERT(pmsg != NULL && pmsg->request);
	ASSERT(pmsg->frag_id);

	c_conn = pmsg->owner;
	ASSERT(c_conn->client && !c_conn->proxy);

	sp = c_conn->owner;
	ASSERT(sp != NULL);

	stats_server_incr(ctx, s_conn->owner, penetrate);

	if(pmsg->error)
	{
		pmsg->error = 0;
		pmsg->err = 0;
	}
	else
	{
		ASSERT(msg != NULL);
		//rsp_forward_stats(ctx, s_conn->owner, msg);
		stats_server_incr(ctx, s_conn->owner, responses);
    	stats_server_incr_by(ctx, s_conn->owner, response_bytes, msg->mlen);
	}
	
	if(array_n(pmsg->keys) == 1)
	{
		//rsp_forward_stats(ctx, s_conn->owner, msg);

		pmsg->peer = NULL;
		if(msg != NULL)
		{
			msg->peer = NULL;
			rsp_put(msg);
		}
		struct mbuf *mbuf_elem;
		STAILQ_FOREACH(mbuf_elem, &pmsg->mhdr, next) {
			mbuf_elem->pos = mbuf_elem->start;
		}
		pmsg->server_pool_id ++;
		msg_tmo_delete(pmsg);
		req_forward(ctx, c_conn, pmsg);
		return;
	}


	struct msg_tqh frag_msgq;
	struct msg *sub_msg;
	struct msg *tmsg; 			/* tmp next message */

	TAILQ_INIT(&frag_msgq);
	
	//ASSERT(msg != NULL);
	pmsg->server_pool_id ++;

	status = pmsg->replication_penetrate(pmsg, msg, &frag_msgq);

	pmsg->self_done = 1;
	pmsg->done = 1;
	
	if(status == NC_OK && !TAILQ_EMPTY(&frag_msgq))
	{
		log_debug(LOG_DEBUG, "replication_penetrate ok!");
		for (sub_msg = TAILQ_FIRST(&frag_msgq); sub_msg != NULL; sub_msg = tmsg) {
        	tmsg = TAILQ_NEXT(sub_msg, m_tqe);

	        TAILQ_REMOVE(&frag_msgq, sub_msg, m_tqe);
	        req_forward(ctx, c_conn, sub_msg);
	    }
		ASSERT(TAILQ_EMPTY(&frag_msgq));
		log_debug(LOG_DEBUG, "all sub_msg had been send.");
		if(pmsg->narg == 0)
		{
			if(msg != NULL && array_n(msg->keys) != 0)
			{
				log_debug(LOG_WARN, "array_n(msg->keys) : %d", array_n(msg->keys));
				msg_print(pmsg, LOG_WARN);
				msg_print(msg, LOG_WARN);
				msg_print(pmsg->frag_owner, LOG_WARN);
			}
			ASSERT(msg == NULL || array_n(msg->keys) == 0);
			pmsg->frag_owner->nfrag --;
			TAILQ_REMOVE(&c_conn->omsg_q, pmsg, c_tqe);
			req_put(pmsg);
		}
	}
	else
	{
		log_error("error: replication_penetrate error occur!");
		
		struct msg *frag_owner;
		frag_owner = pmsg->frag_owner;
		ASSERT(frag_owner != NULL);
		int i;
		for (i = 0; i < array_n(frag_owner->keys); i++) {      /* for each  key */
			sub_msg = frag_owner->frag_seq[i];    
			if(sub_msg == NULL)
			{
				frag_owner->frag_seq[i] = pmsg;
			}
		}
		stats_server_incr(ctx, s_conn->owner, penetrate_err);
	}

}
#endif //shenzheng 2015-4-2 replication pool

#if 1 //shenzheng 2015-4-28 proxy administer
void
req_put_proxy_adm(struct msg *msg)
{
    struct msg *pmsg; /* peer message (response) */

    ASSERT(msg->request);

	pmsg = msg->peer;
    if (pmsg != NULL) {
        ASSERT(!pmsg->request && pmsg->peer == msg);
        msg->peer = NULL;
        pmsg->peer = NULL;
        
		ASSERT(!pmsg->request);
	    ASSERT(pmsg->peer == NULL);
	    msg_put_proxy_adm(pmsg);
    }

    msg_put_proxy_adm(msg);
}


static bool
proxy_adm_req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(conn->client && !conn->proxy);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "filter empty req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        req_put_proxy_adm(msg);
        return true;
    }

    /*
     * Handle "quit\r\n", which is the protocol way of doing a
     * passive close
     */
    if (msg->quit) {
        log_debug(LOG_INFO, "filter quit req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        conn->eof = 1;
        conn->recv_ready = 0;
        req_put_proxy_adm(msg);
        return true;
    }

    return false;
}

struct msg *
req_recv_next_proxy_adm(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    ASSERT(conn->client && !conn->proxy);
	
    if (conn->eof) {
        msg = conn->rmsg;

        /* client sent eof before sending the entire request */
        if (msg != NULL) {
            conn->rmsg = NULL;

            ASSERT(msg->peer == NULL);
            ASSERT(msg->request && !msg->done);

            log_error("eof c %d discarding incomplete req %"PRIu64" len "
                      "%"PRIu32"", conn->sd, msg->id, msg->mlen);

            req_put_proxy_adm(msg);
        }

        /*
         * TCP half-close enables the client to terminate its half of the
         * connection (i.e. the client no longer sends data), but it still
         * is able to receive data from the proxy. The proxy closes its
         * half (by sending the second FIN) when the client has no
         * outstanding requests
         */
        if (!conn->active(conn)) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->sd);
        }
        return NULL;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        ASSERT(msg->request);
        return msg;
    }

    if (!alloc) {
        return NULL;
    }

    msg = msg_get_proxy_adm(conn, true);
    if (msg != NULL) {
        conn->rmsg = msg;
    }
	else
	{
		conn->err = errno;
	}
	
    return msg;
}

void
req_recv_done_proxy_adm(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg)
{
    rstatus_t status;
	struct proxy_adm *padm;
	struct msg *res_msg;
	
    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(conn->rmsg == msg);
	ASSERT(conn->owner == ctx);
    ASSERT(nmsg == NULL || nmsg->request);

	padm = ctx->padm;
	ASSERT(padm != NULL);

	msg_print(msg, LOG_DEBUG);
	
    /* enqueue next message (request), if any */
    conn->rmsg = nmsg;

    if (proxy_adm_req_filter(ctx, conn, msg)) {
        return;
    }

	
	res_msg = proxy_adm_make_response(ctx, conn, msg);
	if(res_msg == NULL)
	{
		return;
	}

	res_msg->peer = msg;
    msg->peer = res_msg;

    msg->done = 1;
    conn->enqueue_outq(ctx, conn, msg);

    status = event_add_out(padm->evb, conn);
    if (status != NC_OK) {
        conn->err = errno;
    }
	
    //req_forward_error(ctx, conn, msg);

    return;
}

#endif //shenzheng 2015-4-28 proxy administer

