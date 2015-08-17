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

#ifndef _NC_ZOOKEEPER_H_
#define _NC_ZOOKEEPER_H_

#if 1 //shenzheng 2015-6-8 zookeeper
#ifdef NC_ZOOKEEPER
#include <nc_core.h>

#define ZOOKEEPER_START_DEFAULT		0
#define ZOOKEEPER_KEEP_DEFAULT		0
#define ZOOKEEPER_ADDR      		"127.0.0.1:2181"
#define ZOOKEEPER_PATH				"/twemproxy"


#define Zk_MAX_DATA_LEN				5000


void *zk_init(char *zk_servers);
rstatus_t zk_close(void *zhandle);

rstatus_t zk_get(void *zkhandle, char *zk_path, struct string *value);
rstatus_t zk_wget(void *zkhandle, char *zk_path, void *watcher, void *watcher_ctx, struct string *value);
rstatus_t zk_conf_set_watcher(void *zkhandle, char *zk_path, void *watcher_ctx);


#endif
#endif //shenzheng 2015-6-8 zookeeper

#endif

