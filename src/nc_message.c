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

#include <stdio.h>
#include <stdlib.h>

#include <sys/uio.h>

#include <nc_core.h>
#include <nc_server.h>
#include <proto/nc_proto.h>

#if (IOV_MAX > 128)
#define NC_IOV_MAX 128
#else
#define NC_IOV_MAX IOV_MAX
#endif

/*
 *            nc_message.[ch]
 *         message (struct msg)
 *            +        +            .
 *            |        |            .
 *            /        \            .
 *         Request    Response      .../ nc_mbuf.[ch]  (mesage buffers)
 *      nc_request.c  nc_response.c .../ nc_memcache.c; nc_redis.c (message parser)
 *
 * Messages in nutcracker are manipulated by a chain of processing handlers,
 * where each handler is responsible for taking the input and producing an
 * output for the next handler in the chain. This mechanism of processing
 * loosely conforms to the standard chain-of-responsibility design pattern
 *
 * At the high level, each handler takes in a message: request or response
 * and produces the message for the next handler in the chain. The input
 * for a handler is either a request or response, but never both and
 * similarly the output of an handler is either a request or response or
 * nothing.
 *
 * Each handler itself is composed of two processing units:
 *
 * 1). filter: manipulates output produced by the handler, usually based
 *     on a policy. If needed, multiple filters can be hooked into each
 *     location.
 * 2). forwarder: chooses one of the backend servers to send the request
 *     to, usually based on the configured distribution and key hasher.
 *
 * Handlers are registered either with Client or Server or Proxy
 * connections. A Proxy connection only has a read handler as it is only
 * responsible for accepting new connections from client. Read handler
 * (conn_recv_t) registered with client is responsible for reading requests,
 * while that registered with server is responsible for reading responses.
 * Write handler (conn_send_t) registered with client is responsible for
 * writing response, while that registered with server is responsible for
 * writing requests.
 *
 * Note that in the above discussion, the terminology send is used
 * synonymously with write or OUT event. Similarly recv is used synonymously
 * with read or IN event
 *
 *             Client+             Proxy           Server+
 *                              (nutcracker)
 *                                   .
 *       msg_recv {read event}       .       msg_recv {read event}
 *         +                         .                         +
 *         |                         .                         |
 *         \                         .                         /
 *         req_recv_next             .             rsp_recv_next
 *           +                       .                       +
 *           |                       .                       |       Rsp
 *           req_recv_done           .           rsp_recv_done      <===
 *             +                     .                     +
 *             |                     .                     |
 *    Req      \                     .                     /
 *    ===>     req_filter*           .           *rsp_filter
 *               +                   .                   +
 *               |                   .                   |
 *               \                   .                   /
 *               req_forward-//  (a) . (c)  \\-rsp_forward
 *                                   .
 *                                   .
 *       msg_send {write event}      .      msg_send {write event}
 *         +                         .                         +
 *         |                         .                         |
 *    Rsp' \                         .                         /     Req'
 *   <===  rsp_send_next             .             req_send_next     ===>
 *           +                       .                       +
 *           |                       .                       |
 *           \                       .                       /
 *           rsp_send_done-//    (d) . (b)    //-req_send_done
 *
 *
 * (a) -> (b) -> (c) -> (d) is the normal flow of transaction consisting
 * of a single request response, where (a) and (b) handle request from
 * client, while (c) and (d) handle the corresponding response from the
 * server.
 */

static uint64_t msg_id;          /* message id counter */
static uint64_t frag_id;         /* fragment id counter */
static uint32_t nfree_msgq;      /* # free msg q */
static struct msg_tqh free_msgq; /* free msg q */
static struct rbtree tmo_rbt;    /* timeout rbtree */
static struct rbnode tmo_rbs;    /* timeout rbtree sentinel */

#if 1 //shenzheng 2015-5-13 proxy administer
static uint64_t msg_id_proxy_adm;          /* message id counter for proxy administer */
static uint32_t nfree_msgq_proxy_adm;      /* # free msg q for proxy administer */
static struct msg_tqh free_msgq_proxy_adm; /* free msg q for proxy administer */
#endif //shenzheng 2015-5-13 proxy administer

#if 1 //shenzheng 2015-3-26 for debug
#ifdef NC_DEBUG_LOG
static uint32_t nused_msgq;      /* # used msg q */
static struct msg_tqh used_msgq; /* used msg q */
#endif
#endif //shenzheng 2015-3-26 for debug

#if 1 //shenzheng 2015-3-23 common
#ifdef NC_DEBUG_LOG
static uint64_t ntotal_msg;
#endif
#endif //shenzheng 2015-3-23 common

#if 1 //shenzheng 2015-7-9 proxy administer
#ifdef NC_DEBUG_LOG
static uint64_t ntotal_msg_proxy_adm;
#endif
#endif //shenzheng 2015-7-9 proxy administer

#define DEFINE_ACTION(_name) string(#_name),
static struct string msg_type_strings[] = {
    MSG_TYPE_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

static struct msg *
msg_from_rbe(struct rbnode *node)
{
    struct msg *msg;
    int offset;

    offset = offsetof(struct msg, tmo_rbe);
    msg = (struct msg *)((char *)node - offset);

    return msg;
}

struct msg *
msg_tmo_min(void)
{
    struct rbnode *node;

    node = rbtree_min(&tmo_rbt);
    if (node == NULL) {
        return NULL;
    }

    return msg_from_rbe(node);
}

void
msg_tmo_insert(struct msg *msg, struct conn *conn)
{
    struct rbnode *node;
    int timeout;

    ASSERT(msg->request);
    ASSERT(!msg->quit && !msg->noreply);

    timeout = server_timeout(conn);
    if (timeout <= 0) {
        return;
    }

    node = &msg->tmo_rbe;
    node->key = nc_msec_now() + timeout;
    node->data = conn;

    rbtree_insert(&tmo_rbt, node);

    log_debug(LOG_VERB, "insert msg %"PRIu64" into tmo rbt with expiry of "
              "%d msec", msg->id, timeout);
}

void
msg_tmo_delete(struct msg *msg)
{
    struct rbnode *node;

    node = &msg->tmo_rbe;

    /* already deleted */

    if (node->data == NULL) {
        return;
    }

    rbtree_delete(&tmo_rbt, node);

    log_debug(LOG_VERB, "delete msg %"PRIu64" from tmo rbt", msg->id);
}

static struct msg *
_msg_get(void)
{
    struct msg *msg;

    if (!TAILQ_EMPTY(&free_msgq)) {
        ASSERT(nfree_msgq > 0);

        msg = TAILQ_FIRST(&free_msgq);
        nfree_msgq--;
        TAILQ_REMOVE(&free_msgq, msg, m_tqe);
        goto done;
    }

    msg = nc_alloc(sizeof(*msg));
    if (msg == NULL) {
        return NULL;
    }

#if 1 //shenzheng 2015-3-23 common
#ifdef NC_DEBUG_LOG
	ntotal_msg ++;
#endif
#endif //shenzheng 2015-3-23 common

done:
    /* c_tqe, s_tqe, and m_tqe are left uninitialized */
    msg->id = ++msg_id;
    msg->peer = NULL;
    msg->owner = NULL;

    rbtree_node_init(&msg->tmo_rbe);

    STAILQ_INIT(&msg->mhdr);
    msg->mlen = 0;
    msg->start_ts = 0;

    msg->state = 0;
    msg->pos = NULL;
    msg->token = NULL;

    msg->parser = NULL;
    msg->add_auth = NULL;
    msg->result = MSG_PARSE_OK;

    msg->fragment = NULL;
    msg->reply = NULL;
    msg->pre_coalesce = NULL;
    msg->post_coalesce = NULL;

    msg->type = MSG_UNKNOWN;

    msg->keys = array_create(1, sizeof(struct keypos));
    if (msg->keys == NULL) {
        nc_free(msg);
        return NULL;
    }

    msg->vlen = 0;
    msg->end = NULL;

    msg->frag_owner = NULL;
    msg->frag_seq = NULL;
    msg->nfrag = 0;
    msg->nfrag_done = 0;
    msg->frag_id = 0;

    msg->narg_start = NULL;
    msg->narg_end = NULL;
    msg->narg = 0;
    msg->rnarg = 0;
    msg->rlen = 0;
    msg->integer = 0;

    msg->err = 0;
    msg->error = 0;
    msg->ferror = 0;
    msg->request = 0;
    msg->quit = 0;
    msg->noreply = 0;
    msg->noforward = 0;
    msg->done = 0;
    msg->fdone = 0;
    msg->swallow = 0;
    msg->redis = 0;

#if 1 //shenzheng 2014-9-4 replace server
	msg->replace_server = 0;
	msg->mser_idx = 0;
	msg->server = NULL;
	msg->conf_version_curr = -1;
#endif //shenzheng 2015-7-27 replace server

#if 1 //shenzheng 2015-3-26 for debug
#ifdef NC_DEBUG_LOG
		nused_msgq ++;
		TAILQ_INSERT_HEAD(&used_msgq, msg, u_tqe);
#endif
#endif //shenzheng 2015-3-30 for debug

    return msg;
}

struct msg *
msg_get(struct conn *conn, bool request, bool redis)
{
    struct msg *msg;

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    msg->owner = conn;
    msg->request = request ? 1 : 0;
    msg->redis = redis ? 1 : 0;

    if (redis) {
        if (request) {
            msg->parser = redis_parse_req;
        } else {
            msg->parser = redis_parse_rsp;
        }

        msg->add_auth = redis_add_auth_packet;
        msg->fragment = redis_fragment;
        msg->reply = redis_reply;
        msg->pre_coalesce = redis_pre_coalesce;
        msg->post_coalesce = redis_post_coalesce;
    } else {
        if (request) {
            msg->parser = memcache_parse_req;
        } else {
            msg->parser = memcache_parse_rsp;
        }
        msg->add_auth = memcache_add_auth_packet;
        msg->fragment = memcache_fragment;
        msg->pre_coalesce = memcache_pre_coalesce;
        msg->post_coalesce = memcache_post_coalesce;
    }

    if (log_loggable(LOG_NOTICE) != 0) {
        msg->start_ts = nc_usec_now();
    }

    log_debug(LOG_VVERB, "get msg %p id %"PRIu64" request %d owner sd %d",
              msg, msg->id, msg->request, conn->sd);

    return msg;
}

struct msg *
msg_get_error(bool redis, err_t err)
{
    struct msg *msg;
    struct mbuf *mbuf;
    int n;
    char *errstr = err ? strerror(err) : "unknown";
    char *protstr = redis ? "-ERR" : "SERVER_ERROR";

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    msg->state = 0;
    msg->type = MSG_RSP_MC_SERVER_ERROR;

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        msg_put(msg);
        return NULL;
    }
    mbuf_insert(&msg->mhdr, mbuf);

    n = nc_scnprintf(mbuf->last, mbuf_size(mbuf), "%s %s"CRLF, protstr, errstr);
    mbuf->last += n;
    msg->mlen = (uint32_t)n;

    log_debug(LOG_VVERB, "get msg %p id %"PRIu64" len %"PRIu32" error '%s'",
              msg, msg->id, msg->mlen, errstr);

    return msg;
}

#if 1 //shenzheng 2014-12-4 common
/*
  * argument err must be a  negative number that wo defined a macro.
  */
struct msg *
msg_get_error_other(bool redis, err_t err)
{
    struct msg *msg;
    struct mbuf *mbuf;
    int n;
    char *errstr; //= err ? strerror(err) : "unknown";
    char *protstr = redis ? "-ERR" : "SERVER_ERROR";

	ASSERT(err < 0);

	switch(err){
		case ERROR_REPLACE_SERVER_TRY_AGAIN:
			errstr = "twemproxy now replace_server, try command again!";
			break;
		case ERROR_REPLACE_SERVER_CONF_VERSION_CHANGE:
			errstr = "conf version changed!(maybe reload_conf running now.)";
			break;
		default:
			errstr = "unknown";
			break;
	}

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    msg->state = 0;
    msg->type = MSG_RSP_MC_SERVER_ERROR;

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        msg_put(msg);
        return NULL;
    }
    mbuf_insert(&msg->mhdr, mbuf);

    n = nc_scnprintf(mbuf->last, mbuf_size(mbuf), "%s %s"CRLF, protstr, errstr);
    mbuf->last += n;
    msg->mlen = (uint32_t)n;

    log_debug(LOG_VVERB, "get msg %p id %"PRIu64" len %"PRIu32" error '%s'",
              msg, msg->id, msg->mlen, errstr);

    return msg;
}

/*
  * true: 	msg1 content same as msg2
  * false:	msg1 content different from msg2
  */

bool
msg_content_cmp(struct msg * msg1, struct msg * msg2)
{
	if(msg1 == NULL && msg2 == NULL)
	{
		return true;
	}
	else if(msg1 == NULL || msg2 == NULL)
	{
		return false;
	}

	if(msg1->mlen != msg2->mlen)
	{
		return false;
	}
	
	struct mbuf *mbuf1, *mbuf2;
	uint32_t mbuf1_to_cmp_len, mbuf2_to_cmp_len;
	uint8_t *cmp_pos1, *cmp_pos2;
	bool done_flag = false;
	
	mbuf1 = STAILQ_FIRST(&msg1->mhdr);
	mbuf2 = STAILQ_FIRST(&msg2->mhdr);

	if(mbuf1 == NULL && mbuf2 == NULL)
	{
		return true;
	}
	else if(mbuf1 == NULL && msg2->mlen == 0)
	{
		ASSERT(msg1->mlen == 0);
		return true;
	}
	else if(mbuf2 == NULL && msg1->mlen == 0)
	{
		ASSERT(msg2->mlen == 0);
		return true;
	}
	else if(msg1->mlen == 0 && msg2->mlen == 0)
	{
		return true;
	}
		
	mbuf1_to_cmp_len = mbuf_storage_length(mbuf1);
	mbuf2_to_cmp_len = mbuf_storage_length(mbuf2);
	cmp_pos1 = mbuf1->start;
	cmp_pos2 = mbuf2->start;

	log_debug(LOG_DEBUG, "mbuf1_to_cmp_len : %d", mbuf1_to_cmp_len);
	log_debug(LOG_DEBUG, "mbuf2_to_cmp_len : %d", mbuf2_to_cmp_len);

	while(!done_flag)
	{
		if(mbuf1_to_cmp_len == mbuf2_to_cmp_len)
		{
			if(memcmp(cmp_pos1, cmp_pos2, mbuf1_to_cmp_len) != 0)
			{
				return false;
			}
			else
			{
				mbuf1 = STAILQ_NEXT(mbuf1, next);
				mbuf2 = STAILQ_NEXT(mbuf2, next);
				if(mbuf1 == NULL)
				{
					while(mbuf2 != NULL)
					{
						mbuf2_to_cmp_len = mbuf_storage_length(mbuf2);
						if(mbuf2_to_cmp_len > 0)
						{
							return false;
						}
						mbuf2 = STAILQ_NEXT(mbuf2, next);
					}
					return true;
				}
				else if(mbuf2 == NULL)
				{
					while(mbuf1 != NULL)
					{
						mbuf1_to_cmp_len = mbuf_storage_length(mbuf1);
						if(mbuf1_to_cmp_len > 0)
						{
							return false;
						}
						mbuf1 = STAILQ_NEXT(mbuf1, next);
					}
					return true;
				}
				mbuf1_to_cmp_len = mbuf_storage_length(mbuf1);
				mbuf2_to_cmp_len = mbuf_storage_length(mbuf2);
				cmp_pos1 = mbuf1->start;
				cmp_pos2 = mbuf2->start;
			}
		}
		else if(mbuf1_to_cmp_len < mbuf2_to_cmp_len)
		{
			if(memcmp(cmp_pos1, cmp_pos2, mbuf1_to_cmp_len) != 0)
			{
				return false;
			}
			else
			{
				mbuf1 = STAILQ_NEXT(mbuf1, next);
				if(mbuf1 == NULL)
				{
					return false;
				}
				mbuf1_to_cmp_len = mbuf_storage_length(mbuf1);
				cmp_pos1 = mbuf1->start;
				mbuf2_to_cmp_len -= mbuf1_to_cmp_len;
				cmp_pos2 += mbuf1_to_cmp_len;
			}
		}
		else
		{
			if(memcmp(cmp_pos1, cmp_pos2, mbuf2_to_cmp_len) != 0)
			{
				return false;
			}
			else
			{
				mbuf2 = STAILQ_NEXT(mbuf2, next);
				if(mbuf2 == NULL)
				{
					return false;
				}
				mbuf2_to_cmp_len = mbuf_storage_length(mbuf2);
				cmp_pos2 = mbuf2->start;
				mbuf1_to_cmp_len -= mbuf2_to_cmp_len;
				cmp_pos1 += mbuf2_to_cmp_len;
			}
		}
	}
	
	return true;
}

#endif //shenzheng 2015-4-16 common

static void
msg_free(struct msg *msg)
{
    ASSERT(STAILQ_EMPTY(&msg->mhdr));

    log_debug(LOG_VVERB, "free msg %p id %"PRIu64"", msg, msg->id);
    nc_free(msg);
}

void
msg_put(struct msg *msg)
{
    log_debug(LOG_VVERB, "put msg %p id %"PRIu64"", msg, msg->id);

    while (!STAILQ_EMPTY(&msg->mhdr)) {
        struct mbuf *mbuf = STAILQ_FIRST(&msg->mhdr);
        mbuf_remove(&msg->mhdr, mbuf);
        mbuf_put(mbuf);
    }

    if (msg->frag_seq) {
        nc_free(msg->frag_seq);
        msg->frag_seq = NULL;
    }

    if (msg->keys) {
        msg->keys->nelem = 0; /* a hack here */
        array_destroy(msg->keys);
        msg->keys = NULL;
    }

#if 1 //shenzheng 2015-6-26 replace server
	if (msg->replace_server && msg->server)
	{
		server_close_for_replace_server(msg->server);
		msg->server = NULL;
	}
#endif //shenzheng 2015-6-26 replace server

    nfree_msgq++;
    TAILQ_INSERT_HEAD(&free_msgq, msg, m_tqe);

#if 1 //shenzheng 2015-3-26 for debug
#ifdef NC_DEBUG_LOG
	nused_msgq --;
	TAILQ_REMOVE(&used_msgq, msg, u_tqe);
#endif
#endif //shenzheng 2015-3-26 for debug

}

void
msg_dump(struct msg *msg, int level)
{
    struct mbuf *mbuf;

    if (log_loggable(level) == 0) {
        return;
    }

    loga("msg dump id %"PRIu64" request %d len %"PRIu32" type %d done %d "
         "error %d (err %d)", msg->id, msg->request, msg->mlen, msg->type,
         msg->done, msg->error, msg->err);

    STAILQ_FOREACH(mbuf, &msg->mhdr, next) {
        uint8_t *p, *q;
        long int len;

        p = mbuf->start;
        q = mbuf->last;
        len = q - p;

        loga_hexdump(p, len, "mbuf [%p] with %ld bytes of data", p, len);
    }
}

void
msg_init(void)
{
    log_debug(LOG_DEBUG, "msg size %d", sizeof(struct msg));
    msg_id = 0;
    frag_id = 0;
    nfree_msgq = 0;
    TAILQ_INIT(&free_msgq);
    rbtree_init(&tmo_rbt, &tmo_rbs);
	
#if 1 //shenzheng 2015-3-23 common
#ifdef NC_DEBUG_LOG
	ntotal_msg = 0;
#endif
#endif //shenzheng 2015-3-23 common

#if 1 //shenzheng 2015-7-9 proxy administer
#ifdef NC_DEBUG_LOG
	ntotal_msg_proxy_adm = 0;
#endif
#endif //shenzheng 2015-7-9 proxy administer

#if 1 //shenzheng 2015-3-26 for debug
#ifdef NC_DEBUG_LOG
	nused_msgq = 0;
	TAILQ_INIT(&used_msgq);
#endif
#endif //shenzheng 2015-3-26 for debug

}

void
msg_deinit(void)
{
    struct msg *msg, *nmsg;

    for (msg = TAILQ_FIRST(&free_msgq); msg != NULL;
         msg = nmsg, nfree_msgq--) {
        ASSERT(nfree_msgq > 0);
        nmsg = TAILQ_NEXT(msg, m_tqe);
        msg_free(msg);
		
#if 1 //shenzheng 2015-3-23 common
#ifdef NC_DEBUG_LOG
		ntotal_msg --;
#endif
#endif //shenzheng 2015-3-23 common

    }
    ASSERT(nfree_msgq == 0);

#if 1 //shenzheng 2015-3-23 common
#ifdef NC_DEBUG_LOG
	ASSERT(ntotal_msg == 0);
#endif
#endif //shenzheng 2015-3-23 common

#if 1 //shenzheng 2015-3-26 for debug
#ifdef NC_DEBUG_LOG
	ASSERT(nused_msgq == 0);
	//TAILQ_INIT(&used_msgq);
#endif
#endif //shenzheng 2015-3-26 for debug

#if 1 //shenzheng 2015-7-9 proxy administer
	for (msg = TAILQ_FIRST(&free_msgq_proxy_adm); msg != NULL;
         msg = nmsg, nfree_msgq_proxy_adm--) {
        ASSERT(nfree_msgq_proxy_adm > 0);
        nmsg = TAILQ_NEXT(msg, m_tqe);
        msg_free(msg);
		
#ifdef NC_DEBUG_LOG
		ntotal_msg_proxy_adm--;
#endif
    }
    ASSERT(nfree_msgq_proxy_adm == 0);

#ifdef NC_DEBUG_LOG
	ASSERT(ntotal_msg_proxy_adm == 0);
#endif

#endif //shenzheng 2015-7-9 proxy administer
}

struct string *
msg_type_string(msg_type_t type)
{
    return &msg_type_strings[type];
}

bool
msg_empty(struct msg *msg)
{
    return msg->mlen == 0 ? true : false;
}

uint32_t
msg_backend_idx(struct msg *msg, uint8_t *key, uint32_t keylen)
{
    struct conn *conn = msg->owner;
    struct server_pool *pool = conn->owner;

    return server_pool_idx(pool, key, keylen);
}

struct mbuf *
msg_ensure_mbuf(struct msg *msg, size_t len)
{
    struct mbuf *mbuf;

    if (STAILQ_EMPTY(&msg->mhdr) ||
        mbuf_size(STAILQ_LAST(&msg->mhdr, mbuf, next)) < len) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NULL;
        }
        mbuf_insert(&msg->mhdr, mbuf);
    } else {
        mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    }
    return mbuf;
}

/*
 * append small(small than a mbuf) content into msg
 */
rstatus_t
msg_append(struct msg *msg, uint8_t *pos, size_t n)
{
    struct mbuf *mbuf;

    ASSERT(n <= mbuf_data_size());

    mbuf = msg_ensure_mbuf(msg, n);
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    ASSERT(n <= mbuf_size(mbuf));

    mbuf_copy(mbuf, pos, n);
    msg->mlen += (uint32_t)n;
    return NC_OK;
}

/*
 * prepend small(small than a mbuf) content into msg
 */
rstatus_t
msg_prepend(struct msg *msg, uint8_t *pos, size_t n)
{
    struct mbuf *mbuf;

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    ASSERT(n <= mbuf_size(mbuf));

    mbuf_copy(mbuf, pos, n);
    msg->mlen += (uint32_t)n;

    STAILQ_INSERT_HEAD(&msg->mhdr, mbuf, next);
    return NC_OK;
}

/*
 * prepend small(small than a mbuf) content into msg
 */
rstatus_t
msg_prepend_format(struct msg *msg, const char *fmt, ...)
{
    struct mbuf *mbuf;
    int32_t n;
    va_list args;

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    va_start(args, fmt);
    n = nc_vscnprintf(mbuf->last, mbuf_size(mbuf), fmt, args);
    va_end(args);

    mbuf->last += n;
    msg->mlen += (uint32_t)n;

    ASSERT(mbuf_size(mbuf) >= 0);
    STAILQ_INSERT_HEAD(&msg->mhdr, mbuf, next);
    return NC_OK;
}

inline uint64_t
msg_gen_frag_id(void)
{
    return ++frag_id;
}

static rstatus_t
msg_parsed(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *nmsg;
    struct mbuf *mbuf, *nbuf;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (msg->pos == mbuf->last) {
        /* no more data to parse */
        conn->recv_done(ctx, conn, msg, NULL);
        return NC_OK;
    }

    /*
     * Input mbuf has un-parsed data. Split mbuf of the current message msg
     * into (mbuf, nbuf), where mbuf is the portion of the message that has
     * been parsed and nbuf is the portion of the message that is un-parsed.
     * Parse nbuf as a new message nmsg in the next iteration.
     */
    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
    if (nbuf == NULL) {
        return NC_ENOMEM;
    }

    nmsg = msg_get(msg->owner, msg->request, conn->redis);
    if (nmsg == NULL) {
        mbuf_put(nbuf);
        return NC_ENOMEM;
    }
    mbuf_insert(&nmsg->mhdr, nbuf);
    nmsg->pos = nbuf->pos;

    /* update length of current (msg) and new message (nmsg) */
    nmsg->mlen = mbuf_length(nbuf);
    msg->mlen -= nmsg->mlen;

    conn->recv_done(ctx, conn, msg, nmsg);

    return NC_OK;
}

static rstatus_t
msg_repair(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct mbuf *nbuf;

    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
    if (nbuf == NULL) {
        return NC_ENOMEM;
    }
    mbuf_insert(&msg->mhdr, nbuf);
    msg->pos = nbuf->pos;

    return NC_OK;
}

static rstatus_t
msg_parse(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    if (msg_empty(msg)) {
        /* no data to parse */
        conn->recv_done(ctx, conn, msg, NULL);
        return NC_OK;
    }

    msg->parser(msg);

    switch (msg->result) {
    case MSG_PARSE_OK:
        status = msg_parsed(ctx, conn, msg);
        break;

    case MSG_PARSE_REPAIR:
        status = msg_repair(ctx, conn, msg);
        break;

    case MSG_PARSE_AGAIN:
        status = NC_OK;
        break;

    default:
        status = NC_ERROR;
        conn->err = errno;
        break;
    }

    return conn->err != 0 ? NC_ERROR : status;
}

static rstatus_t
msg_recv_chain(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *nmsg;
    struct mbuf *mbuf;
    size_t msize;
    ssize_t n;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (mbuf == NULL || mbuf_full(mbuf)) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NC_ENOMEM;
        }
        mbuf_insert(&msg->mhdr, mbuf);
        msg->pos = mbuf->pos;
    }
    ASSERT(mbuf->end - mbuf->last > 0);

    msize = mbuf_size(mbuf);

    n = conn_recv(conn, mbuf->last, msize);
    if (n < 0) {
        if (n == NC_EAGAIN) {
            return NC_OK;
        }
        return NC_ERROR;
    }

    ASSERT((mbuf->last + n) <= mbuf->end);
    mbuf->last += n;
    msg->mlen += (uint32_t)n;

    for (;;) {
        status = msg_parse(ctx, conn, msg);	//parse one command
        if (status != NC_OK) {
            return status;
        }

        /* get next message to parse */
        nmsg = conn->recv_next(ctx, conn, false);
        if (nmsg == NULL || nmsg == msg) {
            /* no more data to parse */
            break;
        }

        msg = nmsg;
    }

    return NC_OK;
}

rstatus_t
msg_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;
	
#if 1 //shenzheng 2015-7-28 for debug
	log_debug(LOG_DEBUG, "msg_recv %s(%d)", conn->proxy?"p":(conn->client?"c":"s"), conn->sd);
#endif //shenzheng 2015-7-28 for debug

    ASSERT(conn->recv_active);

    conn->recv_ready = 1;
    do {
        msg = conn->recv_next(ctx, conn, true);
        if (msg == NULL) {
            return NC_OK;
        }

        status = msg_recv_chain(ctx, conn, msg);
        if (status != NC_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return NC_OK;
}

static rstatus_t
msg_send_chain(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg_tqh send_msgq;            /* send msg q */
    struct msg *nmsg;                    /* next msg */
    struct mbuf *mbuf, *nbuf;            /* current and next mbuf */
    size_t mlen;                         /* current mbuf data length */
    struct iovec *ciov, iov[NC_IOV_MAX]; /* current iovec */
    struct array sendv;                  /* send iovec */
    size_t nsend, nsent;                 /* bytes to send; bytes sent */
    size_t limit;                        /* bytes to send limit */
    ssize_t n;                           /* bytes sent by sendv */

    TAILQ_INIT(&send_msgq);

    array_set(&sendv, iov, sizeof(iov[0]), NC_IOV_MAX);

    /* preprocess - build iovec */

    nsend = 0;
    /*
     * readv() and writev() returns EINVAL if the sum of the iov_len values
     * overflows an ssize_t value Or, the vector count iovcnt is less than
     * zero or greater than the permitted maximum.
     */
    limit = SSIZE_MAX;

    for (;;) {
        ASSERT(conn->smsg == msg);

        TAILQ_INSERT_TAIL(&send_msgq, msg, m_tqe);

        for (mbuf = STAILQ_FIRST(&msg->mhdr);
             mbuf != NULL && array_n(&sendv) < NC_IOV_MAX && nsend < limit;
             mbuf = nbuf) {
            nbuf = STAILQ_NEXT(mbuf, next);

            if (mbuf_empty(mbuf)) {
                continue;
            }

            mlen = mbuf_length(mbuf);

			if ((nsend + mlen) > limit) {
                mlen = limit - nsend;
            }

            ciov = array_push(&sendv);
            ciov->iov_base = mbuf->pos;
            ciov->iov_len = mlen;

            nsend += mlen;
        }

        if (array_n(&sendv) >= NC_IOV_MAX || nsend >= limit) {
            break;
        }

        msg = conn->send_next(ctx, conn);
        if (msg == NULL) {
            break;
        }
    }

    /*
     * (nsend == 0) is possible in redis multi-del
     * see PR: https://github.com/twitter/twemproxy/pull/225
     */
    conn->smsg = NULL;
    if (!TAILQ_EMPTY(&send_msgq) && nsend != 0) {
        n = conn_sendv(conn, &sendv, nsend);
    } else {
        n = 0;
    }

    nsent = n > 0 ? (size_t)n : 0;

    /* postprocess - process sent messages in send_msgq */

    for (msg = TAILQ_FIRST(&send_msgq); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, m_tqe);

        TAILQ_REMOVE(&send_msgq, msg, m_tqe);

        if (nsent == 0) {
            if (msg->mlen == 0) {
                conn->send_done(ctx, conn, msg);
            }
            continue;
        }

        /* adjust mbufs of the sent message */
        for (mbuf = STAILQ_FIRST(&msg->mhdr); mbuf != NULL; mbuf = nbuf) {
            nbuf = STAILQ_NEXT(mbuf, next);

            if (mbuf_empty(mbuf)) {
                continue;
            }
						
            mlen = mbuf_length(mbuf);

			if (nsent < mlen) {
                /* mbuf was sent partially; process remaining bytes later */
                mbuf->pos += nsent;
                ASSERT(mbuf->pos < mbuf->last);
                nsent = 0;
                break;
            }

            /* mbuf was sent completely; mark it empty */
            mbuf->pos = mbuf->last;
            nsent -= mlen;
        }

        /* message has been sent completely, finalize it */
        if (mbuf == NULL) {
            conn->send_done(ctx, conn, msg);
        }
    }

    ASSERT(TAILQ_EMPTY(&send_msgq));

    if (n >= 0) {
        return NC_OK;
    }

    return (n == NC_EAGAIN) ? NC_OK : NC_ERROR;
}

rstatus_t
msg_send(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;
	
#if 1 //shenzheng 2015-7-28 for debug
	log_debug(LOG_DEBUG, "msg_send %s(%d)", conn->proxy?"p":(conn->client?"c":"s"), conn->sd);
#endif //shenzheng 2015-7-28 for debug

    ASSERT(conn->send_active);

    conn->send_ready = 1;
    do {
        msg = conn->send_next(ctx, conn);
        if (msg == NULL) {
            /* nothing to send */

            return NC_OK;
        }

        status = msg_send_chain(ctx, conn, msg);
        if (status != NC_OK) {

            return status;
        }

    } while (conn->send_ready);

    return NC_OK;
}

#if 1 //shenzheng 2015-2-3 common
void _msg_print(const char *file, int line, struct msg *msg, bool real, int level)
{
	if(log_loggable(level) == 0)
	{
		return;
	}

	if(msg == NULL)
	{
		_log(file, line, 0, "msg is NULL");
		return;
	}
	
	uint8_t *ch;
	struct mbuf *mbuf;
	uint8_t	mbuf_num = 0;
	bool msg_done = false;
	uint8_t step_len = 50;
	size_t content_len = 0;
	struct string msg_content;
	mbuf = STAILQ_FIRST(&msg->mhdr);
	_log(file, line, 0, "msg->id : %d", msg->id);	
	_log(file, line, 0, "msg->frag_id : %d", msg->frag_id);
	
	_log(file, line, 0, "msg->swallow : %d", msg->swallow);
	_log(file, line, 0, "msg->done : %d", msg->done);
	_log(file, line, 0, "msg->request : %d", msg->request);
	_log(file, line, 0, "msg->error : %d", msg->error);
	_log(file, line, 0, "msg->mlen : %d", msg->mlen);
	_log(file, line, 0, "msg->vlen : %d", msg->vlen);
	if(msg->peer != NULL)
	{
		_log(file, line, 0, "msg->peer->id : %d", msg->peer->id);
		_log(file, line, 0, "msg->peer->frag_id : %d", msg->peer->frag_id);
		_log(file, line, 0, "msg->peer->request : %d", msg->peer->request);
		if(msg->peer->peer != NULL)
		{
			_log(file, line, 0, "msg->peer->peer->id : %d", msg->peer->peer->id);
		}
		else
		{
			_log(file, line, 0, "msg->peer->peer is null!!");
		}
	}
	else
	{
		_log(file, line, 0, "msg->peer is NULL");
	}

	if(msg->frag_owner != NULL)
	{
		_log(file, line, 0, "msg->frag_owner->id : %d", msg->frag_owner->id);
	}
	else
	{
		_log(file, line, 0, "msg->frag_owner is NULL");
	}

	uint32_t array_len;
	array_len = array_n(msg->keys);
	if(array_len > 0)
	{
		struct keypos *kpos_msg;
		size_t klen_msg;
		uint32_t i;
		for(i = 0; i < array_len; i ++)
		{
			kpos_msg = array_get(msg->keys, i);
			klen_msg = kpos_msg->end - kpos_msg->start;
			_log(file, line, 0, "keys[%d](len:%lld) : %.*s", i, klen_msg, klen_msg, kpos_msg->start);
		}
	}
	else
	{
		_log(file, line, 0, "array_n(msg->keys) is %d", array_len);
	}

	
	if(mbuf == NULL)
	{
		_log(file, line, 0, "error: the first mbuf in msg is null");
		return;
	}
	mbuf_num ++;
	string_init(&msg_content);
	msg_content.len = step_len;
	msg_content.data = nc_alloc(msg_content.len * sizeof(uint8_t));
	
	ch = mbuf->start;
	//ch = mbuf->pos;
	do
	{
		if(ch >= mbuf->last)
		{
			mbuf = STAILQ_NEXT(mbuf, next);
			if(mbuf == NULL)
			{
				msg_done = true;
				break;
			}
			ch = mbuf->start;
			mbuf_num ++;
			//ch = mbuf->pos;
		}
		if(content_len >= msg_content.len)
		{
			msg_content.len = content_len + step_len;
			msg_content.data = nc_realloc(msg_content.data, msg_content.len*sizeof(uint8_t));
			
		}
		
		if(*ch == LF)
		{
			if(real)
			{
				msg_content.data[content_len] = *ch;
			}
			else
			{
				msg_content.data[content_len] = '\\';
				content_len ++;
				if(content_len >= msg_content.len)
				{
					msg_content.len = content_len + step_len;
					msg_content.data = nc_realloc(msg_content.data, msg_content.len*sizeof(uint8_t));
				
				}
				msg_content.data[content_len] = 'n';
			}
		}
		else if(*ch == CR)
		{
			if(real)
			{
				msg_content.data[content_len] = *ch;
			}
			else
			{
				msg_content.data[content_len] = '\\';
				content_len ++;
				if(content_len >= msg_content.len)
				{
					msg_content.len = content_len + step_len;
					msg_content.data = nc_realloc(msg_content.data, msg_content.len*sizeof(uint8_t));
					
				}
				msg_content.data[content_len] = 'r';
			}
		}
		else
		{
			msg_content.data[content_len] = *ch;
		}
		ch ++;
		content_len ++;
	}while(!msg_done);
	//log_debug(LOG_INFO, "content_len : %d", content_len);
	_log(file, line, 0, "mbuf_num : %d", mbuf_num);
	_log(file, line, 0, "content_len : %d", content_len);
	if(content_len <= LOG_MAX_LEN)
	{
		//log_debug(LOG_DEBUG, "msg content: %.*s", content_len, msg_content.data);
		_log(file, line, 0, "msg content: %.*s", 
			content_len, msg_content.data);
	}
	else
	{
		log_all(file, line, content_len, msg_content.data, "msg content: ");
	}
	string_deinit(&msg_content);
}
#endif //shenzheng 2015-2-3 common

#if 1 //shenzheng 2015-3-23 common
#ifdef NC_DEBUG_LOG
uint32_t
msg_nfree_msg()
{
	return nfree_msgq;
}

uint64_t
msg_ntotal_msg()
{
	return ntotal_msg;
}
#endif
#endif //shenzheng 2015-3-23 common

#if 1 //shenzheng 2015-7-9 proxy administer
#ifdef NC_DEBUG_LOG
uint32_t
msg_nfree_msg_proxy_adm()
{
	return nfree_msgq_proxy_adm;
}

uint64_t
msg_ntotal_msg_proxy_adm()
{
	return ntotal_msg_proxy_adm;
}
#endif
#endif //shenzheng 2015-7-9 proxy administer

#if 1 //shenzheng 2015-3-26 for debug
#ifdef NC_DEBUG_LOG
void print_timeout_used_msgs()
{
	struct msg *msg, *nmsg;
	int i = 0;
	log_debug(LOG_WARN, "nused_msgq : %d", nused_msgq);
    for (msg = TAILQ_FIRST(&used_msgq); msg != NULL;
         msg = nmsg) {
        ASSERT(nused_msgq > 0);
        nmsg = TAILQ_NEXT(msg, u_tqe);

		int64_t now = nc_usec_now();
		if(now - msg->start_ts > 30000000)
		{
        	msg_print(msg, LOG_WARN);
			i++;
		}

		if(i >= 50)
		{
			break;
		}
    }
}

void print_used_msgs()
{
	struct msg *msg, *pmsg;
	int i = 0;
	log_debug(LOG_WARN, "nused_msgq : %d", nused_msgq);
    for (msg = TAILQ_LAST(&used_msgq, msg_tqh); msg != NULL;
         msg = pmsg) {
        ASSERT(nused_msgq > 0);
        pmsg = TAILQ_PREV(msg, msg_tqh, u_tqe);

		int64_t now = nc_usec_now();
		if(now - msg->start_ts > 30000000)
		{
        	msg_print(msg, LOG_WARN);
			i++;
		}

		if(i >= 500)
		{
			break;
		}
    }
}

#endif
#endif //shenzheng 2015-3-26 for debug

#if 1 //shenzheng 2015-4-27 proxy administer
static struct msg *
_msg_get_proxy_adm(void)
{
    struct msg *msg;

    if (!TAILQ_EMPTY(&free_msgq_proxy_adm)) {
        ASSERT(nfree_msgq_proxy_adm > 0);

        msg = TAILQ_FIRST(&free_msgq_proxy_adm);
        nfree_msgq_proxy_adm --;
        TAILQ_REMOVE(&free_msgq_proxy_adm, msg, m_tqe);
        goto done;
    }

    msg = nc_alloc(sizeof(*msg));
    if (msg == NULL) {
        return NULL;
    }
	
#ifdef NC_DEBUG_LOG
	ntotal_msg_proxy_adm ++;
#endif

done:
    /* c_tqe, s_tqe, and m_tqe are left uninitialized */
    msg->id = ++msg_id_proxy_adm;
    msg->peer = NULL;
    msg->owner = NULL;

    rbtree_node_init(&msg->tmo_rbe);

    STAILQ_INIT(&msg->mhdr);
    msg->mlen = 0;
    msg->start_ts = 0;

    msg->state = 0;
    msg->pos = NULL;
    msg->token = NULL;

    msg->parser = NULL;
    msg->add_auth = NULL;
    msg->result = MSG_PARSE_OK;

    msg->fragment = NULL;
    msg->reply = NULL;
    msg->pre_coalesce = NULL;
    msg->post_coalesce = NULL;

    msg->type = MSG_UNKNOWN;

    msg->keys = array_create(1, sizeof(struct keypos));
    if (msg->keys == NULL) {
        nc_free(msg);
        return NULL;
    }	

    msg->vlen = 0;
    msg->end = NULL;

    msg->frag_owner = NULL;
    msg->frag_seq = NULL;
    msg->nfrag = 0;
    msg->nfrag_done = 0;
    msg->frag_id = 0;

    msg->narg_start = NULL;
    msg->narg_end = NULL;
    msg->narg = 0;
    msg->rnarg = 0;
    msg->rlen = 0;
    msg->integer = 0;

    msg->err = 0;
    msg->error = 0;
    msg->ferror = 0;
    msg->request = 0;
    msg->quit = 0;
    msg->noreply = 0;
    msg->noforward = 0;
    msg->done = 0;
    msg->fdone = 0;
    msg->swallow = 0;
    msg->redis = 0;

    return msg;
}

struct msg *
msg_get_proxy_adm(struct conn *conn, bool request)
{
    struct msg *msg;
	ASSERT(!conn->proxy && conn->client);

    msg = _msg_get_proxy_adm();
    if (msg == NULL) {
        return NULL;
    }

    msg->owner = conn;
    msg->request = request ? 1 : 0;
	
    if (request) {
        msg->parser = proxy_adm_parse_req;
    } else {
        msg->parser = proxy_adm_parse_rsp;
    }
    if (log_loggable(LOG_NOTICE) != 0) {
        msg->start_ts = nc_usec_now();
    }

    log_debug(LOG_VVERB, "get msg %p id %"PRIu64" request %d owner sd %d",
              msg, msg->id, msg->request, conn->sd);

    return msg;
}


void
msg_put_proxy_adm(struct msg *msg)
{
    log_debug(LOG_VVERB, "put msg %p id %"PRIu64"", msg, msg->id);

    while (!STAILQ_EMPTY(&msg->mhdr)) {
        struct mbuf *mbuf = STAILQ_FIRST(&msg->mhdr);
        mbuf_remove(&msg->mhdr, mbuf);
        mbuf_put_proxy_adm(mbuf);
    }

    if (msg->frag_seq) {
        nc_free(msg->frag_seq);
        msg->frag_seq = NULL;
    }

    if (msg->keys) {
        msg->keys->nelem = 0; /* a hack here */
        array_destroy(msg->keys);
        msg->keys = NULL;
    }

    nfree_msgq_proxy_adm ++;
    TAILQ_INSERT_HEAD(&free_msgq_proxy_adm, msg, m_tqe);

}


struct msg *
msg_get_error_proxy_adm(err_t err)
{
    struct msg *msg;
    struct mbuf *mbuf;
    int n;
    char *errstr = err ? strerror(err) : "unknown";
    char *protstr = "ERR";

    msg = _msg_get_proxy_adm();
    if (msg == NULL) {
        return NULL;
    }

    msg->state = 0;

    mbuf = mbuf_get_proxy_adm();
    if (mbuf == NULL) {
        msg_put_proxy_adm(msg);
        return NULL;
    }
    mbuf_insert(&msg->mhdr, mbuf);

    n = nc_scnprintf(mbuf->last, mbuf_size(mbuf), "%s %s"CRLF, protstr, errstr);
    mbuf->last += n;
    msg->mlen = (uint32_t)n;

    log_debug(LOG_VVERB, "get msg %p id %"PRIu64" len %"PRIu32" error '%s'",
              msg, msg->id, msg->mlen, errstr);

    return msg;
}


struct mbuf *
msg_ensure_mbuf_proxy_adm(struct msg *msg, size_t len)
{
    struct mbuf *mbuf;

    if (STAILQ_EMPTY(&msg->mhdr) ||
        mbuf_size(STAILQ_LAST(&msg->mhdr, mbuf, next)) < len) {
        mbuf = mbuf_get_proxy_adm();
        if (mbuf == NULL) {
            return NULL;
        }
        mbuf_insert(&msg->mhdr, mbuf);
    } else {
        mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    }
    return mbuf;
}

/*
 * append small(small than a mbuf) content into msg
 */
rstatus_t
msg_append_proxy_adm(struct msg *msg, uint8_t *pos, size_t n)
{
    struct mbuf *mbuf;

    ASSERT(n <= mbuf_data_size());

    mbuf = msg_ensure_mbuf_proxy_adm(msg, n);
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    ASSERT(n <= mbuf_size(mbuf));

    mbuf_copy(mbuf, pos, n);
    msg->mlen += (uint32_t)n;
    return NC_OK;
}

struct mbuf *
mbuf_split_proxy_adm(struct mhdr *h, uint8_t *pos, mbuf_copy_t cb, void *cbarg)
{
    struct mbuf *mbuf, *nbuf;
    size_t size;

    ASSERT(!STAILQ_EMPTY(h));

    mbuf = STAILQ_LAST(h, mbuf, next);
    ASSERT(pos >= mbuf->pos && pos <= mbuf->last);

    nbuf = mbuf_get_proxy_adm();
    if (nbuf == NULL) {
        return NULL;
    }

    if (cb != NULL) {
        /* precopy nbuf */
        cb(nbuf, cbarg);
    }

	/* copy data from mbuf to nbuf */
    size = (size_t)(mbuf->last - pos);
    mbuf_copy(nbuf, pos, size);

    /* adjust mbuf */
    mbuf->last = pos;

    log_debug(LOG_VVERB, "split into mbuf %p len %"PRIu32" and nbuf %p len "
              "%"PRIu32" copied %zu bytes", mbuf, mbuf_length(mbuf), nbuf,
              mbuf_length(nbuf), size);

    return nbuf;
}

static rstatus_t
msg_parsed_proxy_adm(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *nmsg;
    struct mbuf *mbuf, *nbuf;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (msg->pos == mbuf->last) {
        /* no more data to parse */
        conn->recv_done(ctx, conn, msg, NULL);
        return NC_OK;
    }

    /*
     * Input mbuf has un-parsed data. Split mbuf of the current message msg
     * into (mbuf, nbuf), where mbuf is the portion of the message that has
     * been parsed and nbuf is the portion of the message that is un-parsed.
     * Parse nbuf as a new message nmsg in the next iteration.
     */
    nbuf = mbuf_split_proxy_adm(&msg->mhdr, msg->pos, NULL, NULL);
    if (nbuf == NULL) {
        return NC_ENOMEM;
    }

    nmsg = msg_get_proxy_adm(msg->owner, msg->request);
    if (nmsg == NULL) {
        mbuf_put_proxy_adm(nbuf);
        return NC_ENOMEM;
    }
    mbuf_insert(&nmsg->mhdr, nbuf);
    nmsg->pos = nbuf->pos;

    /* update length of current (msg) and new message (nmsg) */
    nmsg->mlen = mbuf_length(nbuf);
    msg->mlen -= nmsg->mlen;

    conn->recv_done(ctx, conn, msg, nmsg);

    return NC_OK;
}

static rstatus_t
msg_repair_proxy_adm(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct mbuf *nbuf;

    nbuf = mbuf_split_proxy_adm(&msg->mhdr, msg->pos, NULL, NULL);
    if (nbuf == NULL) {
        return NC_ENOMEM;
    }
    mbuf_insert(&msg->mhdr, nbuf);
    msg->pos = nbuf->pos;

    return NC_OK;
}

static rstatus_t
msg_parse_proxy_adm(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    if (msg_empty(msg)) {
        /* no data to parse */
        conn->recv_done(ctx, conn, msg, NULL);
        return NC_OK;
    }

    msg->parser(msg);

    switch (msg->result) {
    case MSG_PARSE_OK:
        status = msg_parsed_proxy_adm(ctx, conn, msg);
        break;

    case MSG_PARSE_REPAIR:
        status = msg_repair_proxy_adm(ctx, conn, msg);
        break;

    case MSG_PARSE_AGAIN:
        status = NC_OK;
        break;

    default:
        status = NC_ERROR;
        conn->err = errno;
        break;
    }

    return conn->err != 0 ? NC_ERROR : status;
}

static rstatus_t
msg_recv_chain_proxy_adm(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *nmsg;
    struct mbuf *mbuf;
    size_t msize;
    ssize_t n;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (mbuf == NULL || mbuf_full(mbuf)) {
        mbuf = mbuf_get_proxy_adm();
        if (mbuf == NULL) {
            return NC_ENOMEM;
        }
        mbuf_insert(&msg->mhdr, mbuf);
        msg->pos = mbuf->pos;
    }
    ASSERT(mbuf->end - mbuf->last > 0);

    msize = mbuf_size(mbuf);

    n = conn_recv(conn, mbuf->last, msize);
    if (n < 0) {
        if (n == NC_EAGAIN) {
            return NC_OK;
        }
        return NC_ERROR;
    }

    ASSERT((mbuf->last + n) <= mbuf->end);
    mbuf->last += n;
    msg->mlen += (uint32_t)n;

    for (;;) {
        status = msg_parse_proxy_adm(ctx, conn, msg);	//parse one command
        if (status != NC_OK) {
            return status;
        }

        /* get next message to parse */
        nmsg = conn->recv_next(ctx, conn, false);
        if (nmsg == NULL || nmsg == msg) {
            /* no more data to parse */
            break;
        }

        msg = nmsg;
    }

    return NC_OK;
}

rstatus_t
msg_recv_proxy_adm(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;

    ASSERT(conn->recv_active);

    conn->recv_ready = 1;
    do {
        msg = conn->recv_next(ctx, conn, true);
        if (msg == NULL) {
            return NC_OK;
        }

        status = msg_recv_chain_proxy_adm(ctx, conn, msg);
        if (status != NC_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return NC_OK;
}

rstatus_t
msg_send_proxy_adm(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;

    ASSERT(conn->send_active);

    conn->send_ready = 1;
    do {
        msg = conn->send_next(ctx, conn);
        if (msg == NULL) {
            /* nothing to send */

            return NC_OK;
        }

        status = msg_send_chain(ctx, conn, msg);
        if (status != NC_OK) {

            return status;
        }

    } while (conn->send_ready);

    return NC_OK;
}


rstatus_t
msg_append_server_pool_info(struct server_pool *sp, struct msg *msg)
{
	rstatus_t status;
	char * key;
	struct string content = null_string;
	struct string *content_ptr;
	struct server *ser;
	uint32_t i, nservers;

	ASSERT(sp != NULL);
	ASSERT(msg != NULL);

	//append server pool name
	status = msg_append_proxy_adm(msg, sp->name.data, sp->name.len);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)":", 1);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }

	//append listen
	key = "  listen: ";
	status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, sp->addrstr.data, sp->addrstr.len);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }

	//append hash
	key = "  hash: ";
	status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		return status;
    }
	content_ptr = hash_type_to_string(sp->key_hash_type);
	status = msg_append_proxy_adm(msg, content_ptr->data, content_ptr->len);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }

	//append hash_tag
	key = "  hash_tag: ";
	status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		return status;
    }
	string_set_raw(&content, "\"");
	status = msg_append_proxy_adm(msg, content.data, content.len);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, sp->hash_tag.data, sp->hash_tag.len);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, content.data, content.len);
	if (status != NC_OK) {
		return status;
    }
	string_init(&content);
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }

	//append distribution
	key = "  distribution: ";
	status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		return status;
    }
	content_ptr = dist_type_to_string(sp->dist_type);
	status = msg_append_proxy_adm(msg, content_ptr->data, content_ptr->len);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }

	//append timeout
	if(sp->timeout >= 0)
	{
		key = "  timeout: ";
		status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
		if (status != NC_OK) {
			return status;
	    }
		nc_itos(&content, sp->timeout);
		status = msg_append_proxy_adm(msg, content.data, content.len);
		string_deinit(&content);
		if (status != NC_OK) {
			return status;
	    }
		status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			return status;
	    }
	}

	//append backlog
	if(sp->timeout >= 0)
	{
		key = "  backlog: ";
		status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
		if (status != NC_OK) {
			return status;
	    }
		nc_itos(&content, sp->backlog);
		status = msg_append_proxy_adm(msg, content.data, content.len);
		string_deinit(&content);
		if (status != NC_OK) {
			return status;
	    }
		status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			return status;
	    }
	}

	//append preconnect
	key = "  preconnect: ";
	status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		return status;
    }
	if(sp->preconnect)
	{
		string_set_raw(&content, "true");
	}
	else
	{
		string_set_raw(&content, "false");
	}
	status = msg_append_proxy_adm(msg, content.data, content.len);
	string_init(&content);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }

	//append redis
	key = "  redis: ";
	status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		return status;
    }
	if(sp->redis)
	{
		string_set_raw(&content, "true");
	}
	else
	{
		string_set_raw(&content, "false");
	}
	status = msg_append_proxy_adm(msg, content.data, content.len);
	string_init(&content);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }

	//append server_connections
	key = "  server_connections: ";
	status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		return status;
    }
	nc_utos(&content, sp->server_connections);
	status = msg_append_proxy_adm(msg, content.data, content.len);
	string_deinit(&content);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }
	
	//append auto_eject_hosts
	key = "  auto_eject_hosts: ";
	status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		return status;
    }
	if(sp->auto_eject_hosts)
	{
		string_set_raw(&content, "true");
	}
	else
	{
		string_set_raw(&content, "false");
	}
	status = msg_append_proxy_adm(msg, content.data, content.len);
	string_init(&content);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }

	//append server_retry_timeout
	if(sp->server_retry_timeout >= 0)
	{
		key = "  server_retry_timeout: ";
		status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
		if (status != NC_OK) {
			return status;
	    }
		nc_ltos(&content, sp->server_retry_timeout/1000LL);
		status = msg_append_proxy_adm(msg, content.data, content.len);
		string_deinit(&content);
		if (status != NC_OK) {
			return status;
	    }
		status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			return status;
	    }
	}

	//append server_failure_limit
	key = "  server_failure_limit: ";
	status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		return status;
    }
	nc_utos(&content, sp->server_failure_limit);
	status = msg_append_proxy_adm(msg, content.data, content.len);
	string_deinit(&content);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }
	
#if 1 //shenzheng 2015-6-16 tcpkeepalive

	//append tcpkeepalive
	key = "  tcpkeepalive: ";
	status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		return status;
    }
	if(sp->tcpkeepalive)
	{
		string_set_raw(&content, "true");
	}
	else
	{
		string_set_raw(&content, "false");
	}
	status = msg_append_proxy_adm(msg, content.data, content.len);
	string_init(&content);
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }

	//append tcpkeepidle
	if(sp->tcpkeepidle > 0)
	{
		key = "  tcpkeepidle: ";
		status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
		if (status != NC_OK) {
			return status;
	    }
		nc_itos(&content, sp->tcpkeepidle);
		status = msg_append_proxy_adm(msg, content.data, content.len);
		string_deinit(&content);
		if (status != NC_OK) {
			return status;
	    }
		status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			return status;
	    }
	}

	//append tcpkeepcnt
	if(sp->tcpkeepcnt > 0)
	{
		key = "  tcpkeepcnt: ";
		status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
		if (status != NC_OK) {
			return status;
	    }
		nc_itos(&content, sp->tcpkeepcnt);
		status = msg_append_proxy_adm(msg, content.data, content.len);
		string_deinit(&content);
		if (status != NC_OK) {
			return status;
	    }
		status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			return status;
	    }
	}
	
	//append tcpkeepintvl
	if(sp->tcpkeepintvl > 0)
	{
		key = "  tcpkeepintvl: ";
		status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
		if (status != NC_OK) {
			return status;
	    }
		nc_itos(&content, sp->tcpkeepintvl);
		status = msg_append_proxy_adm(msg, content.data, content.len);
		string_deinit(&content);
		if (status != NC_OK) {
			return status;
	    }
		status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			return status;
	    }
	}
#endif //shenzheng 2015-6-16 tcpkeepalive

	//append servers
	key = "  servers: ";
	status = msg_append_proxy_adm(msg, (uint8_t *)key, strlen(key));
	if (status != NC_OK) {
		return status;
    }
	status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
    if (status != NC_OK) {
		return status;
    }
	nservers = array_n(&sp->server);
	string_set_raw(&content, "   - ");
	for(i = 0; i < nservers; i++)
	{
		ser = array_get(&sp->server, i);
		status = msg_append_proxy_adm(msg, content.data, content.len);
		if (status != NC_OK) {
			return status;
	    }

		status = msg_append_proxy_adm(msg, ser->pname.data, ser->pname.len);
	    if (status != NC_OK) {
			return status;
	    }

		status = msg_append_proxy_adm(msg, " ", 1);
	    if (status != NC_OK) {
			return status;
	    }

		status = msg_append_proxy_adm(msg, ser->name.data, ser->name.len);
	    if (status != NC_OK) {
			return status;
	    }

		status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			return status;
	    }
		
	}
	string_init(&content);
	return NC_OK;
}

rstatus_t
msg_append_servers_info(struct server_pool *sp, struct msg *msg)
{
	rstatus_t status;
	struct server *ser;
	uint32_t i, nservers;

	ASSERT(sp != NULL);
	ASSERT(msg != NULL);
	
	nservers = array_n(&sp->server);
	for(i = 0; i < nservers; i++)
	{
		ser = array_get(&sp->server, i);

		status = msg_append_proxy_adm(msg, ser->pname.data, ser->pname.len);
	    if (status != NC_OK) {
			return status;
	    }

		status = msg_append_proxy_adm(msg, " ", 1);
	    if (status != NC_OK) {
			return status;
	    }

		status = msg_append_proxy_adm(msg, ser->name.data, ser->name.len);
	    if (status != NC_OK) {
			return status;
	    }

		status = msg_append_proxy_adm(msg, (uint8_t *)CRLF, CRLF_LEN);
	    if (status != NC_OK) {
			return status;
	    }
		
	}

	return NC_OK;
}
#endif //shenzheng 2015-4-29 proxy administer

