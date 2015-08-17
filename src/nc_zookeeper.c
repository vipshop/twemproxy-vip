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
#include <nc_conf.h>

#if 1 //shenzheng 2015-6-8 zookeeper
#ifdef NC_ZOOKEEPER
#include"zookeeper.h"
//#include"zookeeper_log.h"



void * 
zk_init(char *zk_servers)
{
	zhandle_t* zkhandle;
	int timeout = 30000;

	if(zk_servers == NULL)
	{
		log_error("zookeeper servers address is null");
		return NULL;
	}
	
	zkhandle = zookeeper_init(zk_servers, NULL, timeout, 0, "twemproxy", 0);
	
	if(zkhandle == NULL)
	{
		log_error("get zookeeper handle error.");
		return NULL;
	}

	return zkhandle;
}



rstatus_t 
zk_close(void *zkhandle)
{
	zhandle_t *zh = zkhandle;
	if(zh == NULL)
	{
		log_error("zkhandle is null");
		return NC_ERROR;
	}

	zookeeper_close(zh);

	return NC_OK;
}

rstatus_t 
zk_get(void *zkhandle, char *zk_path, struct string *value)
{
	rstatus_t status;
	zhandle_t *zh = zkhandle;
	
	if(zh == NULL)
	{
		log_error("zkhandle is null");
		return NC_ERROR;
	}
	
	status = zoo_get(zh, zk_path, 0, value->data, &(value->len), NULL);

	if(status != ZOK)
	{
		log_error("zoo_get error(%d)", status);
		return NC_ERROR;
	}

	return NC_OK;
}

rstatus_t 
zk_wget(void *zkhandle, char *zk_path, 
	void *watcher, void *watcher_ctx, struct string *value)
{
	rstatus_t status;
	zhandle_t *zh = zkhandle;
	
	if(zh == NULL)
	{
		log_error("zkhandle is null");
		return NC_ERROR;
	}
	
	status = zoo_wget(zh, zk_path, watcher, watcher_ctx, 
		value->data, &(value->len), NULL);
	if(status != ZOK)
	{
		log_error("zoo_wget error(%d)", status);
		return NC_ERROR;
	}

	return NC_OK;
}

static void 
zk_conf_keep_watcher(zhandle_t* zh, int type, 
	int state, const char* path, void* watcherCtx)
{   
	rstatus_t status;
	struct context * ctx = watcherCtx;
	struct string cf_s;
		
	log_debug(LOG_DEBUG, "type:%d  state:%d", type, state);
	
	log_debug(LOG_DEBUG, "path : %s", path);

	if(type == ZOO_CHANGED_EVENT)
	{
		if(state != ZOO_CONNECTED_STATE)
		{
			log_warn("zookeeper path changed, but state is %d", state);
			return;
		}

		cf_s.data = nc_zalloc(Zk_MAX_DATA_LEN*sizeof(cf_s.data));
		cf_s.len = Zk_MAX_DATA_LEN;
		if(cf_s.data == NULL)
		{
			return;
		}
		
		status = zoo_wget(zh, path, zk_conf_keep_watcher, watcherCtx, 
    		cf_s.data, &(cf_s.len), NULL);
    	if(status != ZOK)
		{
			string_deinit(&cf_s);
			log_error("zoo_get error(%d)", status);
			return;
		}
		
		//status = zk_wget(zh, path,void * watcher,void * watcher_ctx,struct string * value)
		log_debug(LOG_DEBUG, "zookeeper data : %s", cf_s.data);

		status = conf_reload(ctx, NULL, CONF_PARSE_STRING, &cf_s, NULL);
		if(status != NC_OK)
		{
			log_debug(LOG_DEBUG, "conf reload failed(%d).", status);
		}
		string_deinit(&cf_s);
	}
	else if(type == ZOO_DELETED_EVENT)
	{
		if(state != ZOO_CONNECTED_STATE)
		{
			log_warn("warning: zookeeper path changed, but state is %d", state);
			return;
		}
		log_warn("warning: watch path(%s) is deleted.", path);

		status = zoo_wexists(zh, path, zk_conf_keep_watcher, watcherCtx, NULL);
		if(status != ZOK && status != ZNONODE)
		{
			log_error("error: zoo_wexists error(%d)", status);
			if(ctx->zkhandle != NULL)
			{
				zk_close(ctx->zkhandle);
				ctx->zkhandle = NULL;
			}
			return;
		}
	}
	else if(type == ZOO_CREATED_EVENT)
	{
		if(state != ZOO_CONNECTED_STATE)
		{
			log_warn("warning: zookeeper path changed, but state is %d", state);
			return;
		}
		log_warn("warning: watch path(%s) is created.", path);

		status = zoo_wexists(zh, path, zk_conf_keep_watcher, watcherCtx, NULL);
		if(status != ZOK && status != ZNONODE)
		{
			log_error("error: zoo_wexists error(%d)", status);
			if(ctx->zkhandle != NULL)
			{
				zk_close(ctx->zkhandle);
				ctx->zkhandle = NULL;
			}
			return;
		}
	}
	else 
	{
		//ZOO_CHILD_EVENT 
		//ZOO_SESSION_EVENT 
		//ZOO_NOTWATCHING_EVENT
		log_warn("warning: zookeeper watch called, type : %d", type);

		status = zoo_wexists(zh, path, zk_conf_keep_watcher, watcherCtx, NULL);
		if(status != ZOK && status != ZNONODE)
		{
			log_error("error: zoo_wexists error(%d)", status);
			if(ctx->zkhandle != NULL)
			{
				zk_close(ctx->zkhandle);
				ctx->zkhandle = NULL;
			}
			return;
		}
	}
	
}

rstatus_t
zk_conf_set_watcher(void *zkhandle, char *zk_path, void *watcher_ctx)
{
	rstatus_t status;
	zhandle_t* zh = zkhandle;

	if(zh == NULL)
	{
		log_error("error: zkhandle is null");
		return NC_ERROR;
	}

	status = zoo_wexists(zh, zk_path, zk_conf_keep_watcher, watcher_ctx, NULL);
	if(status != ZOK && status != ZNONODE)
	{
		log_error("error: zoo_wexists error(%d)", status);
		return NC_ERROR;
	}
	else if(status == ZNONODE)
	{
		log_warn("warning: watch path(%s) now did not exits.", zk_path);
	}

	return NC_OK;
}

#endif
#endif //shenzheng 2015-6-8 zookeeper


