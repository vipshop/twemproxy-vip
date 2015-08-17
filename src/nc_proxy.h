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

#ifndef _NC_PROXY_H_
#define _NC_PROXY_H_

#include <nc_core.h>

void proxy_ref(struct conn *conn, void *owner);
void proxy_unref(struct conn *conn);
void proxy_close(struct context *ctx, struct conn *conn);

rstatus_t proxy_each_init(void *elem, void *data);
rstatus_t proxy_each_deinit(void *elem, void *data);

rstatus_t proxy_init(struct context *ctx);
void proxy_deinit(struct context *ctx);
rstatus_t proxy_recv(struct context *ctx, struct conn *conn);

#if 1 //shenzheng 2015-4-27 proxy administer

#define PROXY_ADM_ADDR      "0.0.0.0"
#define PROXY_ADM_PORT      0//22223

struct proxy_adm{

	struct string 		addrstr;
	uint16_t            port;          /* proxy administer monitoring port */

	struct sockinfo 	si;

	struct event_base   *evb;

	pthread_t           tid;           /* proxy administer aggregator thread */

	struct conn         *p_conn;       /* proxy administer connection (listener) */
	uint32_t            nc_conn_q;     /* # client connection */
    struct conn_tqh     c_conn_q;      /* client connection q */
};

struct proxy_adm * proxy_adm_create(struct context *ctx, char *proxy_adm_ip, uint16_t proxy_adm_port);
void proxy_adm_destroy(struct proxy_adm *padm);
rstatus_t proxy_adm_loop(void *arg, uint32_t events);

rstatus_t proxy_adm_recv(struct context *ctx, struct conn *conn);
void proxy_adm_close(struct context *ctx, struct conn *conn);
void proxy_adm_ref(struct conn *conn, void *owner);
void proxy_adm_unref(struct conn *conn);

bool proxy_adm_client_active(struct conn *conn);
void proxy_adm_client_close(struct context *ctx, struct conn *conn);
void proxy_adm_client_ref(struct conn *conn, void *owner);
void proxy_adm_client_unref(struct conn *conn);

struct msg * proxy_adm_make_response(struct context *ctx, struct conn *conn, struct msg * req);

#endif //shenzheng 2015-4-27 proxy administer

#if 1 //shenzheng 2015-7-9 config-reload
rstatus_t proxy_init_for_reload(struct server_pool *pool);
rstatus_t proxy_each_deinit_for_reload(void *elem, void *data);
#endif //shenzheng 2015-7-9 config-reload

#endif
