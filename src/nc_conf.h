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

#ifndef _NC_CONF_H_
#define _NC_CONF_H_

#include <unistd.h>
#include <sys/types.h>
#include <sys/un.h>
#include <yaml.h>

#include <nc_core.h>
#include <hashkit/nc_hashkit.h>

#define CONF_OK             (void *) NULL
#define CONF_ERROR          (void *) "has an invalid value"

#define CONF_ROOT_DEPTH     1
#define CONF_MAX_DEPTH      CONF_ROOT_DEPTH + 1

#define CONF_DEFAULT_ARGS       3
#define CONF_DEFAULT_POOL       8
#define CONF_DEFAULT_SERVERS    8

#define CONF_UNSET_NUM  -1
#define CONF_UNSET_PTR  NULL
#define CONF_UNSET_HASH (hash_type_t) -1
#define CONF_UNSET_DIST (dist_type_t) -1

#if 1 //shenzheng 2015-1-8 log rotating
#define CONF_UNSET_LOG_FILE_COUNT -2
#endif //shenzheng 2015-1-8 log rotating

#define CONF_DEFAULT_HASH                    HASH_FNV1A_64
#define CONF_DEFAULT_DIST                    DIST_KETAMA
#define CONF_DEFAULT_TIMEOUT                 -1
#define CONF_DEFAULT_LISTEN_BACKLOG          512
#define CONF_DEFAULT_CLIENT_CONNECTIONS      0
#define CONF_DEFAULT_REDIS                   false
#define CONF_DEFAULT_PRECONNECT              false
#define CONF_DEFAULT_AUTO_EJECT_HOSTS        false
#define CONF_DEFAULT_SERVER_RETRY_TIMEOUT    30 * 1000      /* in msec */
#define CONF_DEFAULT_SERVER_FAILURE_LIMIT    2
#define CONF_DEFAULT_SERVER_CONNECTIONS      1
#define CONF_DEFAULT_KETAMA_PORT             11211

#if 1 //shenzheng 2015-1-7 replication pool
#define CONF_DEFAULT_REPLICATION_MODE      	 0
#define CONF_DEFAULT_WRITE_BACK_MODE		 0
#define CONF_DEFAULT_PENETRATE_MODE			 0
#endif //shenzheng 2015-1-26 replication pool

#if 1 //shenzheng 2015-1-8 log rotating
#define CONF_DEFAULT_LOG_RORATE			 	 1
#define CONF_DEFAULT_LOG_FILE_MAX_SIZE		 1073741824
#define CONF_DEFAULT_LOG_FILE_COUNT			 10
#endif //shenzheng 2015-1-8 log rotating

#if 1 //shenzheng 2015-6-5 tcpkeepalive
#define CONF_DEFAULT_TCPKEEPALIVE            false
#define CONF_DEFAULT_TCPKEEPIDLE             -1
#define CONF_DEFAULT_TCPKEEPINTVL            -1
#define CONF_DEFAULT_TCPKEEPCNT              -1
#endif //shenzheng 2015-6-5 tcpkeepalive

#if 1 //shenzheng 2015-6-8 config-reload
typedef enum conf_parse_type {
    CONF_PARSE_FILE,                   /* conf parse from file */
    CONF_PARSE_STRING				   /* conf parse from string */
} conf_parse_type_t;
#endif //shenzheng 2015-6-8 config-reload

struct conf_listen {
    struct string   pname;   /* listen: as "name:port" */
    struct string   name;    /* name */
    int             port;    /* port */
    struct sockinfo info;    /* listen socket info */
    unsigned        valid:1; /* valid? */
};

struct conf_server {
    struct string   pname;      /* server: as "name:port:weight" */
    struct string   name;       /* name */
    int             port;       /* port */
    int             weight;     /* weight */
    struct sockinfo info;       /* connect socket info */
    unsigned        valid:1;    /* valid? */

#if 1 //shenzheng 2014-9-5 replace server
	unsigned		name_null:1;	/* name in "hostname:port:weight [name]" format string is null? */
#endif //shenzheng 2014-9-5 replace server

};

struct conf_pool {
    struct string      name;                  /* pool name (root node) */
    struct conf_listen listen;                /* listen: */
    hash_type_t        hash;                  /* hash: */
    struct string      hash_tag;              /* hash_tag: */
    dist_type_t        distribution;          /* distribution: */
    int                timeout;               /* timeout: */
    int                backlog;               /* backlog: */
    int                client_connections;    /* client_connections: */
    int                redis;                 /* redis: */
    struct string      redis_auth;            /* redis auth password */
    int                preconnect;            /* preconnect: */
    int                auto_eject_hosts;      /* auto_eject_hosts: */
    int                server_connections;    /* server_connections: */
    int                server_retry_timeout;  /* server_retry_timeout: in msec */
    int                server_failure_limit;  /* server_failure_limit: */
    struct array       server;                /* servers: conf_server[] */
    unsigned           valid:1;               /* valid? */
#if 1 //shenzheng 2014-12-20 replication pool
	struct string	   replication_from;	  /* the otherconf_pool that this conf_pool replicate from */
	/* [replication_mode] 
	  * 0:asynchronous write(default);
	  * 1:semi-synchronous write(just sent the master response to client, 
	  *    and get the slave write response, if not success, notes in a file, 
	  *	 not response slave to client);
	  * 2:synchronous write.
	  */
	int		   		   replication_mode;
	/* [write_back_mode] 
	  * 0:if master pool miss but slave pool hit, do not write back to master slave(default);
	  * 1:if master pool miss but slave pool hit, write back to master slave,
	  *    and if error occur, record in log file.
	  */
	int		   		   write_back_mode;
	/* [penetrate_mode] 
	  * just for get/gets command
	  * 0:only when master pool return no-error and get key miss, request will penetrate to slave pool(default);
	  * 1:only when master pool return error, request will penetrate to slave pool;
	  * 2:master pool return no-error or get key miss, request will penetrate to slave pool;
	  * 3:do not penetrate anyway.
	  */
	int		   		   penetrate_mode;
#endif //shenzheng 2015-1-26 replication pool

#if 1 //shenzheng 2015-6-5 tcpkeepalive
	int				   tcpkeepalive;		  /* tcpkeepalive: */
	int				   tcpkeepidle;			  /* tcpkeepidle: */
	int				   tcpkeepintvl;	      /* tcpkeepintvl: */
	int				   tcpkeepcnt;			  /* tcpkeepcnt: */
#endif //shenzheng 2015-6-5 tcpkeepalive
};

struct conf {
    char          *fname;           /* file name (ref in argv[]) */
    FILE          *fh;              /* file handle */
    struct array  arg;              /* string[] (parsed {key, value} pairs) */
    struct array  pool;             /* conf_pool[] (parsed pools) */
    uint32_t      depth;            /* parsed tree depth */
    yaml_parser_t parser;           /* yaml parser */
    yaml_event_t  event;            /* yaml event */
    yaml_token_t  token;            /* yaml token */
    unsigned      seq:1;            /* sequence? */
    unsigned      valid_parser:1;   /* valid parser? */
    unsigned      valid_event:1;    /* valid event? */
    unsigned      valid_token:1;    /* valid token? */
    unsigned      sound:1;          /* sound? */
    unsigned      parsed:1;         /* parsed? */
    unsigned      valid:1;          /* valid? */
};

struct command {
    struct string name;
    char          *(*set)(struct conf *cf, struct command *cmd, void *data);
    int           offset;
};

#define null_command { null_string, NULL, 0 }

char *conf_set_string(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_listen(struct conf *cf, struct command *cmd, void *conf);
char *conf_add_server(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_num(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_bool(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_hash(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_distribution(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_hashtag(struct conf *cf, struct command *cmd, void *conf);

rstatus_t conf_server_each_transform(void *elem, void *data);
rstatus_t conf_pool_each_transform(void *elem, void *data);

struct conf *conf_create(char *filename);
void conf_destroy(struct conf *cf);

#if 1 //shenzheng 2015-6-26 replace server
rstatus_t conf_write_back_yaml(struct context *ctx, struct string *old_ser, struct string *new_ser);
#endif //shenzheng 2015-6-26 replace server

#if 1 //shenzheng 2015-1-7 replication pool
char *conf_set_replication_mode(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_write_back_mode(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_penetrate_mode(struct conf *cf, struct command *cmd, void *conf);
#endif //shenzheng 2015-4-2 replication pool

#if 1 //shenzheng 2015-1-8 log rotating
char *conf_set_log_rorate(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_log_file_max_size(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_log_file_count(struct conf *cf, struct command *cmd, void *conf);
#endif //shenzheng 2015-1-8 log rotating

#if 1 //shenzheng 2015-4-29 common
struct string *hash_type_to_string(hash_type_t hash_type);
struct string *dist_type_to_string(dist_type_t dist_type);
#endif //shenzheng 2015-4-29 common

#if 1 //shenzheng 2015-5-29 config-reload
rstatus_t conf_two_check_diff(struct conf *cf, struct conf *cf_new);
struct conf *conf_create_from_string(struct string *cf_s);
rstatus_t conf_reload(struct context *ctx, 	struct conn *conn, conf_parse_type_t parse_type, struct string *cf_s, struct msg * msg);
#endif //shenzheng 2015-5-29 config-reload

#if 1 //shenzheng 2015-6-6 zookeeper
#ifdef NC_ZOOKEEPER
struct conf *conf_create_from_zk(struct context * ctx, char *zk_servers, char *zk_path);
rstatus_t conf_keep_from_zk(struct context * ctx, void *zkhandle, char *zk_path);
#endif
#endif //shenzheng 2015-6-6 zookeeper

#endif
