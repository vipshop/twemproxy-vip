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

#ifndef _NC_MESSAGE_H_
#define _NC_MESSAGE_H_

#include <nc_core.h>

#if 1 //shenzheng 2015-2-3 common
#ifdef NC_DEBUG_LOG
#define msg_print(msg, level) do {                                                      \
    _msg_print(__FILE__, __LINE__, msg, false, level);                                         \
} while (0)
#define msg_print_real(msg, level) do {                                                      \
	_msg_print(__FILE__, __LINE__, msg, true, level);										 \
} while (0)
#else
#define msg_print(msg, level)
#define msg_print_real(msg, level)
#endif
#endif //shenzheng 2015-2-3 common

typedef void (*msg_parse_t)(struct msg *);
typedef rstatus_t (*msg_add_auth_t)(struct context *ctx, struct conn *c_conn, struct conn *s_conn);
typedef rstatus_t (*msg_fragment_t)(struct msg *, uint32_t, struct msg_tqh *);
typedef void (*msg_coalesce_t)(struct msg *r);
typedef rstatus_t (*msg_reply_t)(struct msg *r);

#if 1 //shenzheng 2015-1-15 replication pool
typedef rstatus_t (*msg_handle_result_t)(struct context *ctx,struct conn *conn, struct msg *r);
typedef rstatus_t (*msg_replication_penetrate_t)(struct msg *pmsg, struct msg *msg, struct msg_tqh *frag_msgq);
typedef rstatus_t (*msg_replication_write_back_t)(struct context *ctx, struct msg *pmsg, struct msg *msg);
#endif //shenzheng 2015-1-22 replication pool

typedef enum msg_parse_result {
    MSG_PARSE_OK,                         /* parsing ok */
    MSG_PARSE_ERROR,                      /* parsing error */
    MSG_PARSE_REPAIR,                     /* more to parse -> repair parsed & unparsed data */
    MSG_PARSE_AGAIN,                      /* incomplete -> parse again */
} msg_parse_result_t;

#define MSG_TYPE_CODEC(ACTION)                                                                      \
    ACTION( UNKNOWN )                                                                               \
    ACTION( REQ_MC_GET )                       /* memcache retrieval requests */                    \
    ACTION( REQ_MC_GETS )                                                                           \
    ACTION( REQ_MC_DELETE )                    /* memcache delete request */                        \
    ACTION( REQ_MC_CAS )                       /* memcache cas request and storage request */       \
    ACTION( REQ_MC_SET )                       /* memcache storage request */                       \
    ACTION( REQ_MC_ADD )                                                                            \
    ACTION( REQ_MC_REPLACE )                                                                        \
    ACTION( REQ_MC_APPEND )                                                                         \
    ACTION( REQ_MC_PREPEND )                                                                        \
    ACTION( REQ_MC_INCR )                      /* memcache arithmetic request */	/*10*/                    \
    ACTION( REQ_MC_DECR )                                                                           \
    ACTION( REQ_MC_QUIT )                      /* memcache quit request */                          \
    ACTION( RSP_MC_NUM )                       /* memcache arithmetic response */                   \
    ACTION( RSP_MC_STORED )                    /* memcache cas and storage response */              \
    ACTION( RSP_MC_NOT_STORED )                                                                     \
    ACTION( RSP_MC_EXISTS )                                                                         \
    ACTION( RSP_MC_NOT_FOUND )                                                                      \
    ACTION( RSP_MC_END )                                                                            \
    ACTION( RSP_MC_VALUE )                                                                          \
    ACTION( RSP_MC_DELETED )                   /* memcache delete response */     /*20*/                  \
    ACTION( RSP_MC_ERROR )                     /* memcache error responses */                       \
    ACTION( RSP_MC_CLIENT_ERROR )                                                                   \
    ACTION( RSP_MC_SERVER_ERROR )                                                                   \
    ACTION( REQ_REDIS_DEL )                    /* redis commands - keys */                          \
    ACTION( REQ_REDIS_EXISTS )                                                                      \
    ACTION( REQ_REDIS_EXPIRE )                                                                      \
    ACTION( REQ_REDIS_EXPIREAT )                                                                    \
    ACTION( REQ_REDIS_PEXPIRE )                                                                     \
    ACTION( REQ_REDIS_PEXPIREAT )                                                                   \
    ACTION( REQ_REDIS_PERSIST )                                                /*30*/                     \
    ACTION( REQ_REDIS_PTTL )                                                                        \
    ACTION( REQ_REDIS_SORT )                                                                        \
    ACTION( REQ_REDIS_TTL )                                                                         \
    ACTION( REQ_REDIS_TYPE )                                                                        \
    ACTION( REQ_REDIS_APPEND )                 /* redis requests - string */                        \
    ACTION( REQ_REDIS_BITCOUNT )                                                                    \
    ACTION( REQ_REDIS_DECR )                                                                        \
    ACTION( REQ_REDIS_DECRBY )                                                                      \
    ACTION( REQ_REDIS_DUMP )                                                                        \
    ACTION( REQ_REDIS_GET )                                                    /*40*/                     \
    ACTION( REQ_REDIS_GETBIT )                                                                      \
    ACTION( REQ_REDIS_GETRANGE )                                                                    \
    ACTION( REQ_REDIS_GETSET )                                                                      \
    ACTION( REQ_REDIS_INCR )                                                                        \
    ACTION( REQ_REDIS_INCRBY )                                                                      \
    ACTION( REQ_REDIS_INCRBYFLOAT )                                                                 \
    ACTION( REQ_REDIS_MGET )                                                                        \
    ACTION( REQ_REDIS_MSET )                                                                        \
    ACTION( REQ_REDIS_PSETEX )                                                                      \
    ACTION( REQ_REDIS_RESTORE )                                                /*50*/                     \
    ACTION( REQ_REDIS_SET )                                                                         \
    ACTION( REQ_REDIS_SETBIT )                                                                      \
    ACTION( REQ_REDIS_SETEX )                                                                       \
    ACTION( REQ_REDIS_SETNX )                                                                       \
    ACTION( REQ_REDIS_SETRANGE )                                                                    \
    ACTION( REQ_REDIS_STRLEN )                                                                      \
    ACTION( REQ_REDIS_HDEL )                   /* redis requests - hashes */                        \
    ACTION( REQ_REDIS_HEXISTS )                                                                     \
    ACTION( REQ_REDIS_HGET )                                                                        \
    ACTION( REQ_REDIS_HGETALL )                                                /*60*/                     \
    ACTION( REQ_REDIS_HINCRBY )                                                                     \
    ACTION( REQ_REDIS_HINCRBYFLOAT )                                                                \
    ACTION( REQ_REDIS_HKEYS )                                                                       \
    ACTION( REQ_REDIS_HLEN )                                                                        \
    ACTION( REQ_REDIS_HMGET )                                                                       \
    ACTION( REQ_REDIS_HMSET )                                                                       \
    ACTION( REQ_REDIS_HSET )                                                                        \
    ACTION( REQ_REDIS_HSETNX )                                                                      \
    ACTION( REQ_REDIS_HSCAN)                                                                        \
    ACTION( REQ_REDIS_HVALS )                                                   /*70*/                 \
    ACTION( REQ_REDIS_LINDEX )                 /* redis requests - lists */                         \
    ACTION( REQ_REDIS_LINSERT )                                                                     \
    ACTION( REQ_REDIS_LLEN )                                                                        \
    ACTION( REQ_REDIS_LPOP )                                                                        \
    ACTION( REQ_REDIS_LPUSH )                                                                       \
    ACTION( REQ_REDIS_LPUSHX )                                                                      \
    ACTION( REQ_REDIS_LRANGE )                                                                      \
    ACTION( REQ_REDIS_LREM )                                                                        \
    ACTION( REQ_REDIS_LSET )                                                                        \
    ACTION( REQ_REDIS_LTRIM )                                                   /*80*/                    \
    ACTION( REQ_REDIS_PFADD )                  /* redis requests - hyperloglog */                   \
    ACTION( REQ_REDIS_PFCOUNT )                                                                     \
    ACTION( REQ_REDIS_PFMERGE )                                                                     \
    ACTION( REQ_REDIS_RPOP )                                                                        \
    ACTION( REQ_REDIS_RPOPLPUSH )                                                                   \
    ACTION( REQ_REDIS_RPUSH )                                                                       \
    ACTION( REQ_REDIS_RPUSHX )                                                                      \
    ACTION( REQ_REDIS_SADD )                   /* redis requests - sets */                          \
    ACTION( REQ_REDIS_SCARD )                                                                       \
    ACTION( REQ_REDIS_SDIFF )                                                   /*90*/                    \
    ACTION( REQ_REDIS_SDIFFSTORE )                                                                  \
    ACTION( REQ_REDIS_SINTER )                                                                      \
    ACTION( REQ_REDIS_SINTERSTORE )                                                                 \
    ACTION( REQ_REDIS_SISMEMBER )                                                                   \
    ACTION( REQ_REDIS_SMEMBERS )                                                                    \
    ACTION( REQ_REDIS_SMOVE )                                                                       \
    ACTION( REQ_REDIS_SPOP )                                                                        \
    ACTION( REQ_REDIS_SRANDMEMBER )                                                                 \
    ACTION( REQ_REDIS_SREM )                                                                        \
    ACTION( REQ_REDIS_SUNION )                                                  /*100*/                    \
    ACTION( REQ_REDIS_SUNIONSTORE )                                                                 \
    ACTION( REQ_REDIS_SSCAN)                                                                        \
    ACTION( REQ_REDIS_ZADD )                   /* redis requests - sorted sets */                   \
    ACTION( REQ_REDIS_ZCARD )                                                                       \
    ACTION( REQ_REDIS_ZCOUNT )                                                                      \
    ACTION( REQ_REDIS_ZINCRBY )                                                                     \
    ACTION( REQ_REDIS_ZINTERSTORE )                                                                 \
    ACTION( REQ_REDIS_ZLEXCOUNT )                                                                   \
    ACTION( REQ_REDIS_ZRANGE )                                                                      \
    ACTION( REQ_REDIS_ZRANGEBYLEX )                                             /*110*/                    \
    ACTION( REQ_REDIS_ZRANGEBYSCORE )                                                               \
    ACTION( REQ_REDIS_ZRANK )                                                                       \
    ACTION( REQ_REDIS_ZREM )                                                                        \
    ACTION( REQ_REDIS_ZREMRANGEBYRANK )                                                             \
    ACTION( REQ_REDIS_ZREMRANGEBYLEX )                                                              \
    ACTION( REQ_REDIS_ZREMRANGEBYSCORE )                                                            \
    ACTION( REQ_REDIS_ZREVRANGE )                                                                   \
    ACTION( REQ_REDIS_ZREVRANGEBYSCORE )                                                            \
    ACTION( REQ_REDIS_ZREVRANK )                                                                    \
    ACTION( REQ_REDIS_ZSCORE )                                                  /*120*/                    \
    ACTION( REQ_REDIS_ZUNIONSTORE )                                                                 \
    ACTION( REQ_REDIS_ZSCAN)                                                                        \
    ACTION( REQ_REDIS_EVAL )                   /* redis requests - eval */                          \
    ACTION( REQ_REDIS_EVALSHA )                                                                     \
    ACTION( REQ_REDIS_PING )                   /* redis requests - ping/quit */                     \
    ACTION( REQ_REDIS_QUIT)                                                                         \
    ACTION( REQ_REDIS_AUTH)                                                                         \
    ACTION( RSP_REDIS_STATUS )                 /* redis response */                                 \
    ACTION( RSP_REDIS_ERROR )                                                                       \
    ACTION( RSP_REDIS_INTEGER )                                                 /*130*/                    \
    ACTION( RSP_REDIS_BULK )                                                                        \
    ACTION( RSP_REDIS_MULTIBULK )   														\
    ACTION( REQ_REDIS_REPLACE_SERVER )														\
    ACTION( REQ_PROXY_ADM_SHOW_CONF )														\
    ACTION( REQ_PROXY_ADM_SHOW_OCONF )														\
    ACTION( REQ_PROXY_ADM_SHOW_POOL )														\
    ACTION( REQ_PROXY_ADM_SHOW_POOLS )														\
    ACTION( REQ_PROXY_ADM_SHOW_SERVERS )														\
	ACTION( REQ_PROXY_ADM_FIND_KEY )														\
    ACTION( REQ_PROXY_ADM_FIND_KEYS )														\
    ACTION( REQ_PROXY_ADM_RELOAD_CONF )														\
    ACTION( REQ_PROXY_ADM_QUIT )														\
    ACTION( REQ_PROXY_ADM_HELP )														\
    ACTION( REQ_PROXY_ADM_SET_WATCH )														\
    ACTION( REQ_PROXY_ADM_DEL_WATCH )														\
    ACTION( REQ_PROXY_ADM_RESET_WATCH )														\
    ACTION( REQ_PROXY_ADM_SHOW_WATCH )														\
    ACTION( SENTINEL )                                                                              \

#define DEFINE_ACTION(_name) MSG_##_name,
typedef enum msg_type {
    MSG_TYPE_CODEC(DEFINE_ACTION)
} msg_type_t;
#undef DEFINE_ACTION

struct keypos {
    uint8_t             *start;           /* key start pos */
    uint8_t             *end;             /* key end pos */
};

#if 1 //shenzheng 2015-3-26 replication pool
/* keypos with mbuf, this is used for the 
 *  start pos and end pos may not be in 
 *  one mbuf
 */
struct keypos_wmb {
    uint8_t             *start;           /* key start pos */
    uint8_t             *end;             /* key end pos */
	struct mbuf			*start_mbuf;	  /* key start pos in this mbuf */
	struct mbuf			*end_mbuf;		  /* key end pos in this mbuf */
};
#endif //shenzheng 2015-3-26 replication pool

struct msg {
    TAILQ_ENTRY(msg)     c_tqe;           /* link in client q */
    TAILQ_ENTRY(msg)     s_tqe;           /* link in server q */
    TAILQ_ENTRY(msg)     m_tqe;           /* link in send q / free q */
#if 1 //shenzheng 2015-3-26 for debug
#ifdef NC_DEBUG_LOG
	TAILQ_ENTRY(msg)     u_tqe;           /* link in used q */
#endif
#endif //shenzheng 2015-3-26 for debug


    uint64_t             id;              /* message id */
    struct msg           *peer;           /* message peer */
    struct conn          *owner;          /* message owner - client | server */

    struct rbnode        tmo_rbe;         /* entry in rbtree */

    struct mhdr          mhdr;            /* message mbuf header */
    uint32_t             mlen;            /* message length */
    int64_t              start_ts;        /* request start timestamp in usec */

    int                  state;           /* current parser state */
    uint8_t              *pos;            /* parser position marker */
    uint8_t              *token;          /* token marker */
#if 1 //shenzheng 2015-4-3 replication pool
	uint8_t              *res_key_token;  /* token marker for respose key token */
#endif //shenzheng 2015-4-3 replication pool

    msg_parse_t          parser;          /* message parser */
    msg_parse_result_t   result;          /* message parsing result */

    msg_fragment_t       fragment;        /* message fragment */
    msg_reply_t          reply;           /* gen message reply (example: ping) */
    msg_add_auth_t       add_auth;        /* add auth message when we forward msg */

    msg_coalesce_t       pre_coalesce;    /* message pre-coalesce */
    msg_coalesce_t       post_coalesce;   /* message post-coalesce */

    msg_type_t           type;            /* message type */

    struct array         *keys;           /* array of keypos, for req */

    uint32_t             vlen;            /* value length (memcache) */
    uint8_t              *end;            /* end marker (memcache) */

    uint8_t              *narg_start;     /* narg start (redis) */
    uint8_t              *narg_end;       /* narg end (redis) */
    uint32_t             narg;            /* # arguments (redis) */
    uint32_t             rnarg;           /* running # arg used by parsing fsa (redis) */
    uint32_t             rlen;            /* running length in parsing fsa (redis) */
    uint32_t             integer;         /* integer reply value (redis) */

    struct msg           *frag_owner;     /* owner of fragment message */
    uint32_t             nfrag;           /* # fragment */
    uint32_t             nfrag_done;      /* # fragment done */
    uint64_t             frag_id;         /* id of fragmented message */
    struct msg           **frag_seq;      /* sequence of fragment message, map from keys to fragments*/

    err_t                err;             /* errno on error? */
    unsigned             error:1;         /* error? */
    unsigned             ferror:1;        /* one or more fragments are in error? */
    unsigned             request:1;       /* request? or response? */
    unsigned             quit:1;          /* quit request? */
    unsigned             noreply:1;       /* noreply? */
    unsigned             noforward:1;     /* not need forward (example: ping) */
    unsigned             done:1;          /* done? */
    unsigned             fdone:1;         /* all fragments are done? */
    unsigned             swallow:1;       /* swallow response? */
    unsigned             redis:1;         /* redis? */

#if 1 //shenzheng 2014-9-2 replace server
	unsigned			 replace_server:1;/* 1:this msg is for replace_server command, 0:other msgs */
	long long			 conf_version_curr;
	uint32_t			 mser_idx;		  /* replace server index of pool->server */
	struct server        *server;		  /* new server in the replace server command */
#endif //shenzheng 2015-7-27 replace server

#if 1 //shenzheng 2015-1-7 replication pool
	/* [replication_mode] 
	  * 0:asynchronous write(default), 
	  * 1:semi-synchronous write(just sent the master response to client, 
	  *    and get the slave write response, if not success, notes in a file, 
	  *	 not response slave to client),
	  * 2: synchronous write.
	  */
	int		           replication_mode;   /* equal the conf_pool->replication_mode(for master server pool) */
	 /* [server_pool_id](for request msg)
	    * -2:msg is for write back;
	    * -1:msg transmit using master pool; 
	    * N(N>=0):msg transmit using the 
	    * N replication pool that in the master 
	    * server pool->replication_pools. 
	   */
	int			 		 server_pool_id; 
	int					 nreplication_msgs;		/* the num of the replication msgs.(for request write msg. if master, >=0; if slave, -1) */
	struct msg			 **replication_msgs;	/* the replication msgs.(for master request write msg), this value of slave is -1. */
	struct msg			 *master_msg;			/* the master msg for the replication msgs.(for slave request write msg) */
	/* [self_done](for request write msg)
	  * 0:this msg is done
	  * 1:only this msg is done
	  */
	unsigned             self_done:1;
	/* [master_send](for request write msg)
	  * 0:this msg has not send to user.
	  * 1:this msg has send to user.
	  */
	unsigned             master_send:1;
	msg_handle_result_t	 handle_result;			/* if necessary, combine the master and slaves results.(for master request write msg) */
	msg_replication_penetrate_t	replication_penetrate;	/* if query miss in the master pool,  the msg penetrate the replication pool */
	msg_replication_write_back_t replication_write_back;	/* if the result from slave pool is not null, write back to master pool. */
#endif //shenzheng 2015-1-21 replication pool

};

TAILQ_HEAD(msg_tqh, msg);

struct msg *msg_tmo_min(void);
void msg_tmo_insert(struct msg *msg, struct conn *conn);
void msg_tmo_delete(struct msg *msg);

void msg_init(void);
void msg_deinit(void);
struct string *msg_type_string(msg_type_t type);
struct msg *msg_get(struct conn *conn, bool request, bool redis);
void msg_put(struct msg *msg);
struct msg *msg_get_error(bool redis, err_t err);
#if 1 //shenzheng 2014-9-4 common
struct msg *msg_get_error_other(bool redis, err_t err);

bool msg_content_cmp(struct msg * msg1, struct msg * msg2);
#endif //shenzheng 2015-4-16 common
void msg_dump(struct msg *msg, int level);
bool msg_empty(struct msg *msg);
rstatus_t msg_recv(struct context *ctx, struct conn *conn);
rstatus_t msg_send(struct context *ctx, struct conn *conn);
uint64_t msg_gen_frag_id(void);
uint32_t msg_backend_idx(struct msg *msg, uint8_t *key, uint32_t keylen);
struct mbuf *msg_ensure_mbuf(struct msg *msg, size_t len);
rstatus_t msg_append(struct msg *msg, uint8_t *pos, size_t n);
rstatus_t msg_prepend(struct msg *msg, uint8_t *pos, size_t n);
rstatus_t msg_prepend_format(struct msg *msg, const char *fmt, ...);

struct msg *req_get(struct conn *conn);
void req_put(struct msg *msg);
bool req_done(struct conn *conn, struct msg *msg);
bool req_error(struct conn *conn, struct msg *msg);
void req_server_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_server_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_server_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_server_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
struct msg *req_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg, struct msg *nmsg);
struct msg *req_send_next(struct context *ctx, struct conn *conn);
void req_send_done(struct context *ctx, struct conn *conn, struct msg *msg);

struct msg *rsp_get(struct conn *conn);
void rsp_put(struct msg *msg);
struct msg *rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void rsp_recv_done(struct context *ctx, struct conn *conn, struct msg *msg, struct msg *nmsg);
struct msg *rsp_send_next(struct context *ctx, struct conn *conn);
void rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg);

#if 1 //shenzheng 2014-12-26 replication pool
struct msg * msg_copy_for_replication(struct conn * c_conn, struct msg * msg_f, int sp_id);
#endif //shenzheng 2014-12-26 replication pool

#if 1 //shenzheng 2015-1-7 replication pool
void req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg);
bool replication_msgs_done(struct msg *msg);
bool msg_pass(struct msg *msg);
#endif //shenzheng 2015-1-7 replication pool

#if 1 //shenzheng 2015-4-2 replication pool
bool req_need_penetrate(struct msg *pmsg, struct msg *msg);
void req_penetrate(struct context *ctx, struct conn *s_conn, struct msg *pmsg, struct msg *msg);
#endif //shenzheng 2015-4-8 replication pool

#if 1 //shenzheng 2015-2-3 common
void _msg_print(const char *file, int line, struct msg *msg, bool real, int level);
#endif //shenzheng 2015-2-3 common

#if 1 //shenzheng 2015-3-23 common
#ifdef NC_DEBUG_LOG
uint32_t msg_nfree_msg(void);
uint64_t msg_ntotal_msg(void);
#endif
#endif //shenzheng 2015-3-23 common

#if 1 //shenzheng 2015-7-9 proxy administer
#ifdef NC_DEBUG_LOG
uint32_t msg_nfree_msg_proxy_adm(void);
uint64_t msg_ntotal_msg_proxy_adm(void);
#endif
#endif //shenzheng 2015-7-9 proxy administer


#if 1 //shenzheng 2015-3-26 for debug
#ifdef NC_DEBUG_LOG
void print_timeout_used_msgs(void);
void print_used_msgs(void);
#endif
#endif //shenzheng 2015-3-26 for debug

#if 1 //shenzheng 2015-4-27 proxy administer
struct msg *msg_get_proxy_adm(struct conn *conn, bool request);
void msg_put_proxy_adm(struct msg *msg);
struct msg *msg_get_error_proxy_adm(err_t err);
struct mbuf *msg_ensure_mbuf_proxy_adm(struct msg *msg, size_t len);
rstatus_t msg_append_proxy_adm(struct msg *msg, uint8_t *pos, size_t n);
struct mbuf *mbuf_split_proxy_adm(struct mhdr *h, uint8_t *pos, mbuf_copy_t cb, void *cbarg);
rstatus_t msg_recv_proxy_adm(struct context *ctx, struct conn *conn);
rstatus_t msg_send_proxy_adm(struct context *ctx, struct conn *conn);

void req_put_proxy_adm(struct msg *msg);

struct msg *req_recv_next_proxy_adm(struct context *ctx, struct conn *conn, bool alloc);
void req_recv_done_proxy_adm(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg);
struct msg * rsp_send_next_proxy_adm(struct context *ctx, struct conn *conn);
void rsp_send_done_proxy_adm(struct context *ctx, struct conn *conn, struct msg *msg);

rstatus_t msg_append_server_pool_info(struct server_pool *sp, struct msg *msg);
rstatus_t msg_append_servers_info(struct server_pool *sp, struct msg *msg);

#endif //shenzheng 2015-4-29 proxy administer

#endif
