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

#include <ctype.h>

#include <nc_core.h>
#include <nc_proto.h>

/*
 * From memcache protocol specification:
 *
 * Data stored by memcached is identified with the help of a key. A key
 * is a text string which should uniquely identify the data for clients
 * that are interested in storing and retrieving it.  Currently the
 * length limit of a key is set at 250 characters (of course, normally
 * clients wouldn't need to use such long keys); the key must not include
 * control characters or whitespace.
 */
#define MEMCACHE_MAX_KEY_LENGTH 250

#if 1 //shenzheng 2015-4-17 replication pool
/*
 * usage: if master response and slave response is different,
 *            we stored it in file.
 * incon_m_s_svd_in_f short for "inconsistent_master_slave_saved_in_file"
 * incon_m_s_svd_in_f storage format:
 * 0-18bytes: timestamp
 * 20byte: replication_mode
 * 22-24bytes: request type
 * 26byte: response error(if 0, master and slave all ok; 
 *                                    if 1, master ok and slave err; 
 *                                    if 2, master err and slave ok; 
 *                                    if 3, master and slave all err).
 * 28-30bytes: if master response ok, master response type; 
 *                    if master response err, master errno.
 * 32-34bytes: if slave response ok, slave response type; 
 *                    if slave response err, slave errno.
 * 19,21,25,27,31,35byte: blank character.
 * 36-endbytes: key.
 */
#define incon_m_s_svd_in_f_len 40+MEMCACHE_MAX_KEY_LENGTH
static uint8_t incon_m_s_svd_in_f[incon_m_s_svd_in_f_len];
#endif //shenzheng 2015-4-17 replication pool

/*
 * Return true, if the memcache command is a storage command, otherwise
 * return false
 */
static bool
memcache_storage(struct msg *r)
{
    switch (r->type) {
    case MSG_REQ_MC_SET:
    case MSG_REQ_MC_CAS:
    case MSG_REQ_MC_ADD:
    case MSG_REQ_MC_REPLACE:
    case MSG_REQ_MC_APPEND:
    case MSG_REQ_MC_PREPEND:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the memcache command is a cas command, otherwise
 * return false
 */
static bool
memcache_cas(struct msg *r)
{
    if (r->type == MSG_REQ_MC_CAS) {
        return true;
    }

    return false;
}

/*
 * Return true, if the memcache command is a retrieval command, otherwise
 * return false
 */
static bool
memcache_retrieval(struct msg *r)
{
    switch (r->type) {
    case MSG_REQ_MC_GET:
    case MSG_REQ_MC_GETS:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the memcache command is a arithmetic command, otherwise
 * return false
 */
static bool
memcache_arithmetic(struct msg *r)
{
    switch (r->type) {
    case MSG_REQ_MC_INCR:
    case MSG_REQ_MC_DECR:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the memcache command is a delete command, otherwise
 * return false
 */
static bool
memcache_delete(struct msg *r)
{
    if (r->type == MSG_REQ_MC_DELETE) {
        return true;
    }

    return false;
}

void
memcache_parse_req(struct msg *r)
{
    struct mbuf *b;
    uint8_t *p, *m;
    uint8_t ch;
    enum {
        SW_START,
        SW_REQ_TYPE,
        SW_SPACES_BEFORE_KEY,
        SW_KEY,
        SW_SPACES_BEFORE_KEYS,
        SW_SPACES_BEFORE_FLAGS,
        SW_FLAGS,
        SW_SPACES_BEFORE_EXPIRY,
        SW_EXPIRY,
        SW_SPACES_BEFORE_VLEN,
        SW_VLEN,
        SW_SPACES_BEFORE_CAS,
        SW_CAS,
        SW_RUNTO_VAL,
        SW_VAL,
        SW_SPACES_BEFORE_NUM,
        SW_NUM,
        SW_RUNTO_CRLF,
        SW_CRLF,
        SW_NOREPLY,
        SW_AFTER_NOREPLY,
        SW_ALMOST_DONE,
        SW_SENTINEL
    } state;

    state = r->state;
    b = STAILQ_LAST(&r->mhdr, mbuf, next);

    ASSERT(r->request);
    ASSERT(!r->redis);
    ASSERT(state >= SW_START && state < SW_SENTINEL);
    ASSERT(b != NULL);
    ASSERT(b->pos <= b->last);

    /* validate the parsing maker */
    ASSERT(r->pos != NULL);
    ASSERT(r->pos >= b->pos && r->pos <= b->last);

    for (p = r->pos; p < b->last; p++) {
        ch = *p;

        switch (state) {

        case SW_START:
            if (ch == ' ') {
                break;
            }

            if (!islower(ch)) {
                goto error;
            }

            /* req_start <- p; type_start <- p */
            r->token = p;
            state = SW_REQ_TYPE;

            break;

        case SW_REQ_TYPE:
            if (ch == ' ' || ch == CR) {
                /* type_end = p - 1 */
                m = r->token;
                r->token = NULL;
                r->type = MSG_UNKNOWN;
                r->narg++;

                switch (p - m) {

                case 3:
                    if (str4cmp(m, 'g', 'e', 't', ' ')) {
                        r->type = MSG_REQ_MC_GET;
                        break;
                    }

                    if (str4cmp(m, 's', 'e', 't', ' ')) {
                        r->type = MSG_REQ_MC_SET;
                        break;
                    }

                    if (str4cmp(m, 'a', 'd', 'd', ' ')) {
                        r->type = MSG_REQ_MC_ADD;
                        break;
                    }

                    if (str4cmp(m, 'c', 'a', 's', ' ')) {
                        r->type = MSG_REQ_MC_CAS;
                        break;
                    }

                    break;

                case 4:
                    if (str4cmp(m, 'g', 'e', 't', 's')) {
                        r->type = MSG_REQ_MC_GETS;
                        break;
                    }

                    if (str4cmp(m, 'i', 'n', 'c', 'r')) {
                        r->type = MSG_REQ_MC_INCR;
                        break;
                    }

                    if (str4cmp(m, 'd', 'e', 'c', 'r')) {
                        r->type = MSG_REQ_MC_DECR;
                        break;
                    }

                    if (str4cmp(m, 'q', 'u', 'i', 't')) {
                        r->type = MSG_REQ_MC_QUIT;
                        r->quit = 1;
                        break;
                    }

                    break;

                case 6:
                    if (str6cmp(m, 'a', 'p', 'p', 'e', 'n', 'd')) {
                        r->type = MSG_REQ_MC_APPEND;
                        break;
                    }

                    if (str6cmp(m, 'd', 'e', 'l', 'e', 't', 'e')) {
                        r->type = MSG_REQ_MC_DELETE;
                        break;
                    }

                    break;

                case 7:
                    if (str7cmp(m, 'p', 'r', 'e', 'p', 'e', 'n', 'd')) {
                        r->type = MSG_REQ_MC_PREPEND;
                        break;
                    }

                    if (str7cmp(m, 'r', 'e', 'p', 'l', 'a', 'c', 'e')) {
                        r->type = MSG_REQ_MC_REPLACE;
                        break;
                    }

                    break;
                }

                switch (r->type) {
                case MSG_REQ_MC_GET:
                case MSG_REQ_MC_GETS:
                case MSG_REQ_MC_DELETE:
                case MSG_REQ_MC_CAS:
                case MSG_REQ_MC_SET:
                case MSG_REQ_MC_ADD:
                case MSG_REQ_MC_REPLACE:
                case MSG_REQ_MC_APPEND:
                case MSG_REQ_MC_PREPEND:
                case MSG_REQ_MC_INCR:
                case MSG_REQ_MC_DECR:
                    if (ch == CR) {
                        goto error;
                    }
                    state = SW_SPACES_BEFORE_KEY;
                    break;

                case MSG_REQ_MC_QUIT:
                    p = p - 1; /* go back by 1 byte */
                    state = SW_CRLF;
                    break;

                case MSG_UNKNOWN:
                    goto error;

                default:
                    NOT_REACHED();
                }

            } else if (!islower(ch)) {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_KEY:
            if (ch != ' ') {
                p = p - 1; /* go back by 1 byte */
                r->token = NULL;
                state = SW_KEY;
            }
            break;

        case SW_KEY:
            if (r->token == NULL) {
                r->token = p;
            }
            if (ch == ' ' || ch == CR) {
                struct keypos *kpos;

                if ((p - r->token) > MEMCACHE_MAX_KEY_LENGTH) {
                    log_error("parsed bad req %"PRIu64" of type %d with key "
                              "prefix '%.*s...' and length %d that exceeds "
                              "maximum key length", r->id, r->type, 16,
                              r->token, p - r->token);
                    goto error;
                }

                kpos = array_push(r->keys);
                if (kpos == NULL) {
                    goto enomem;
                }
                kpos->start = r->token;
                kpos->end = p;
				
#if 1 //shenzheng 2015-5-12 fix bug: "get " command core dump
				if(kpos->start == kpos->end)
				{
					goto error;
				}
#endif //shenzheng 2015-5-12 fix bug: "get " command core dump

                r->narg++;
                r->token = NULL;

                /* get next state */
                if (memcache_storage(r)) {
                    state = SW_SPACES_BEFORE_FLAGS;
                } else if (memcache_arithmetic(r)) {
                    state = SW_SPACES_BEFORE_NUM;
                } else if (memcache_delete(r)) {
                    state = SW_RUNTO_CRLF;
                } else if (memcache_retrieval(r)) {
                    state = SW_SPACES_BEFORE_KEYS;
                } else {
                    state = SW_RUNTO_CRLF;
                }

                if (ch == CR) {
                    if (memcache_storage(r) || memcache_arithmetic(r)) {
                        goto error;
                    }
                    p = p - 1; /* go back by 1 byte */
                }
            }

            break;

        case SW_SPACES_BEFORE_KEYS:
            ASSERT(memcache_retrieval(r));
            switch (ch) {
            case ' ':
                break;

            case CR:
                state = SW_ALMOST_DONE;
                break;

            default:
                r->token = NULL;
                p = p - 1; /* go back by 1 byte */
                state = SW_KEY;
            }

            break;

        case SW_SPACES_BEFORE_FLAGS:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                /* flags_start <- p; flags <- ch - '0' */
                r->token = p;
                state = SW_FLAGS;
            }

            break;

        case SW_FLAGS:
            if (isdigit(ch)) {
                /* flags <- flags * 10 + (ch - '0') */
                ;
            } else if (ch == ' ') {
                /* flags_end <- p - 1 */
                r->token = NULL;
                state = SW_SPACES_BEFORE_EXPIRY;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_EXPIRY:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                /* expiry_start <- p; expiry <- ch - '0' */
                r->token = p;
                state = SW_EXPIRY;
            }

            break;

        case SW_EXPIRY:
            if (isdigit(ch)) {
                /* expiry <- expiry * 10 + (ch - '0') */
                ;
            } else if (ch == ' ') {
                /* expiry_end <- p - 1 */
                r->token = NULL;
                state = SW_SPACES_BEFORE_VLEN;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_VLEN:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                /* vlen_start <- p */
                r->vlen = (uint32_t)(ch - '0');
                state = SW_VLEN;
            }

            break;

        case SW_VLEN:
            if (isdigit(ch)) {
                r->vlen = r->vlen * 10 + (uint32_t)(ch - '0');
            } else if (memcache_cas(r)) {
                if (ch != ' ') {
                    goto error;
                }
                /* vlen_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                r->token = NULL;
                state = SW_SPACES_BEFORE_CAS;
            } else if (ch == ' ' || ch == CR) {
                /* vlen_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                r->token = NULL;
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_CAS:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                /* cas_start <- p; cas <- ch - '0' */
                r->token = p;
                state = SW_CAS;
            }

            break;

        case SW_CAS:
            if (isdigit(ch)) {
                /* cas <- cas * 10 + (ch - '0') */
                ;
            } else if (ch == ' ' || ch == CR) {
                /* cas_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                r->token = NULL;
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;


        case SW_RUNTO_VAL:
            switch (ch) {
            case LF:
                /* val_start <- p + 1 */
                state = SW_VAL;
                break;

            default:
                goto error;
            }

            break;

        case SW_VAL:
            m = p + r->vlen;
            if (m >= b->last) {
                ASSERT(r->vlen >= (uint32_t)(b->last - p));
                r->vlen -= (uint32_t)(b->last - p);
                m = b->last - 1;
                p = m; /* move forward by vlen bytes */
                break;
            }
            switch (*m) {
            case CR:
                /* val_end <- p - 1 */
                p = m; /* move forward by vlen bytes */
                state = SW_ALMOST_DONE;
                break;

            default:
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_NUM:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                /* num_start <- p; num <- ch - '0'  */
                r->token = p;
                state = SW_NUM;
            }

            break;

        case SW_NUM:
            if (isdigit(ch)) {
                /* num <- num * 10 + (ch - '0') */
                ;
            } else if (ch == ' ' || ch == CR) {
                r->token = NULL;
                /* num_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RUNTO_CRLF:
            switch (ch) {
            case ' ':
                break;
#if 1 //shenzheng 2015-4-1 support 'delete key 0\r\n'
			case '0':
				if(!memcache_delete(r))
				{
					goto error;
				}
				break;
#endif //shenzheng 2015-4-1 support 'delete key 0\r\n'

            case 'n':
                if (memcache_storage(r) || memcache_arithmetic(r) || memcache_delete(r)) {
                    /* noreply_start <- p */
                    r->token = p;
                    state = SW_NOREPLY;
                } else {
                    goto error;
                }

                break;

            case CR:
                if (memcache_storage(r)) {
                    state = SW_RUNTO_VAL;
                } else {
                    state = SW_ALMOST_DONE;
                }

                break;

            default:
                goto error;
            }

            break;

        case SW_NOREPLY:
            switch (ch) {
            case ' ':
            case CR:
                m = r->token;
                if (((p - m) == 7) && str7cmp(m, 'n', 'o', 'r', 'e', 'p', 'l', 'y')) {
                    ASSERT(memcache_storage(r) || memcache_arithmetic(r) || memcache_delete(r));
                    r->token = NULL;
                    /* noreply_end <- p - 1 */
                    r->noreply = 1;
                    state = SW_AFTER_NOREPLY;
                    p = p - 1; /* go back by 1 byte */
                } else {
                    goto error;
                }
            }

            break;

        case SW_AFTER_NOREPLY:
            switch (ch) {
            case ' ':
                break;

            case CR:
                if (memcache_storage(r)) {
                    state = SW_RUNTO_VAL;
                } else {
                    state = SW_ALMOST_DONE;
                }
                break;

            default:
                goto error;
            }

            break;

        case SW_CRLF:
            switch (ch) {
            case ' ':
                break;

            case CR:
                state = SW_ALMOST_DONE;
                break;

            default:
                goto error;
            }

            break;

        case SW_ALMOST_DONE:
            switch (ch) {
            case LF:
                /* req_end <- p */
                goto done;

            default:
                goto error;
            }

            break;

        case SW_SENTINEL:
        default:
            NOT_REACHED();
            break;

        }
    }

    /*
     * At this point, buffer from b->pos to b->last has been parsed completely
     * but we haven't been able to reach to any conclusion. Normally, this
     * means that we have to parse again starting from the state we are in
     * after more data has been read. The newly read data is either read into
     * a new mbuf, if existing mbuf is full (b->last == b->end) or into the
     * existing mbuf.
     *
     * The only exception to this is when the existing mbuf is full (b->last
     * is at b->end) and token marker is set, which means that we have to
     * copy the partial token into a new mbuf and parse again with more data
     * read into new mbuf.
     */
    ASSERT(p == b->last);
    r->pos = p;
    r->state = state;

    if (b->last == b->end && r->token != NULL) {
        r->pos = r->token;
        r->token = NULL;
        r->result = MSG_PARSE_REPAIR;
    } else {
        r->result = MSG_PARSE_AGAIN;
    }

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed req %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

done:
    ASSERT(r->type > MSG_UNKNOWN && r->type < MSG_SENTINEL);
    r->pos = p + 1;
    ASSERT(r->pos <= b->last);
    r->state = SW_START;
    r->result = MSG_PARSE_OK;

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed req %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

enomem:
    r->result = MSG_PARSE_ERROR;
    r->state = state;

    log_hexdump(LOG_INFO, b->pos, mbuf_length(b), "out of memory on parse req %"PRIu64" "
                "res %d type %d state %d", r->id, r->result, r->type, r->state);

    return;

error:
    r->result = MSG_PARSE_ERROR;
    r->state = state;
    errno = EINVAL;

    log_hexdump(LOG_INFO, b->pos, mbuf_length(b), "parsed bad req %"PRIu64" "
                "res %d type %d state %d", r->id, r->result, r->type,
                r->state);
}

void
memcache_parse_rsp(struct msg *r)
{
    struct mbuf *b;
    uint8_t *p, *m;
    uint8_t ch;
    enum {
        SW_START,
        SW_RSP_NUM,
        SW_RSP_STR,
        SW_SPACES_BEFORE_KEY,
        SW_KEY,
        SW_SPACES_BEFORE_FLAGS,     /* 5 */
        SW_FLAGS,
        SW_SPACES_BEFORE_VLEN,
        SW_VLEN,
        SW_RUNTO_VAL,
        SW_VAL,                     /* 10 */
        SW_VAL_LF,
        SW_END,
        SW_RUNTO_CRLF,
        SW_CRLF,
        SW_ALMOST_DONE,             /* 15 */
        SW_SENTINEL
    } state;
	
#if 1 //shenzheng 2015-2-4 replication pool
	//uint8_t *token = NULL;
#endif //shenzheng 2015-4-3 replication pool

    state = r->state;
    b = STAILQ_LAST(&r->mhdr, mbuf, next);

    ASSERT(!r->request);
    ASSERT(!r->redis);
    ASSERT(state >= SW_START && state < SW_SENTINEL);
    ASSERT(b != NULL);
    ASSERT(b->pos <= b->last);

    /* validate the parsing marker */
    ASSERT(r->pos != NULL);
    ASSERT(r->pos >= b->pos && r->pos <= b->last);

    for (p = r->pos; p < b->last; p++) {
        ch = *p;

        switch (state) {
        case SW_START:
            if (isdigit(ch)) {
                state = SW_RSP_NUM;
            } else {
                state = SW_RSP_STR;
            }
            p = p - 1; /* go back by 1 byte */

            break;

        case SW_RSP_NUM:
            if (r->token == NULL) {
                /* rsp_start <- p; type_start <- p */
                r->token = p;
            }

            if (isdigit(ch)) {
                /* num <- num * 10 + (ch - '0') */
                ;
            } else if (ch == ' ' || ch == CR) {
                /* type_end <- p - 1 */
                r->token = NULL;
                r->type = MSG_RSP_MC_NUM;
                p = p - 1; /* go back by 1 byte */
                state = SW_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RSP_STR:
            if (r->token == NULL) {
                /* rsp_start <- p; type_start <- p */
                r->token = p;
            }

            if (ch == ' ' || ch == CR) {
                /* type_end <- p - 1 */
                m = r->token;
                /* r->token = NULL; */
                r->type = MSG_UNKNOWN;

                switch (p - m) {
                case 3:
                    if (str4cmp(m, 'E', 'N', 'D', '\r')) {
                        r->type = MSG_RSP_MC_END;
                        /* end_start <- m; end_end <- p - 1 */
                        r->end = m;
                        break;
                    }

                    break;

                case 5:
                    if (str5cmp(m, 'V', 'A', 'L', 'U', 'E')) {
                        /*
                         * Encompasses responses for 'get', 'gets' and
                         * 'cas' command.
                         */
                        r->type = MSG_RSP_MC_VALUE;
                        break;
                    }

                    if (str5cmp(m, 'E', 'R', 'R', 'O', 'R')) {
                        r->type = MSG_RSP_MC_ERROR;
                        break;
                    }

                    break;

                case 6:
                    if (str6cmp(m, 'S', 'T', 'O', 'R', 'E', 'D')) {
                        r->type = MSG_RSP_MC_STORED;
                        break;
                    }

                    if (str6cmp(m, 'E', 'X', 'I', 'S', 'T', 'S')) {
                        r->type = MSG_RSP_MC_EXISTS;
                        break;
                    }

                    break;

                case 7:
                    if (str7cmp(m, 'D', 'E', 'L', 'E', 'T', 'E', 'D')) {
                        r->type = MSG_RSP_MC_DELETED;
                        break;
                    }

                    break;

                case 9:
                    if (str9cmp(m, 'N', 'O', 'T', '_', 'F', 'O', 'U', 'N', 'D')) {
                        r->type = MSG_RSP_MC_NOT_FOUND;
                        break;
                    }

                    break;

                case 10:
                    if (str10cmp(m, 'N', 'O', 'T', '_', 'S', 'T', 'O', 'R', 'E', 'D')) {
                        r->type = MSG_RSP_MC_NOT_STORED;
                        break;
                    }

                    break;

                case 12:
                    if (str12cmp(m, 'C', 'L', 'I', 'E', 'N', 'T', '_', 'E', 'R', 'R', 'O', 'R')) {
                        r->type = MSG_RSP_MC_CLIENT_ERROR;
                        break;
                    }

                    if (str12cmp(m, 'S', 'E', 'R', 'V', 'E', 'R', '_', 'E', 'R', 'R', 'O', 'R')) {
                        r->type = MSG_RSP_MC_SERVER_ERROR;
                        break;
                    }

                    break;
                }

                switch (r->type) {
                case MSG_UNKNOWN:
                    goto error;

                case MSG_RSP_MC_STORED:
                case MSG_RSP_MC_NOT_STORED:
                case MSG_RSP_MC_EXISTS:
                case MSG_RSP_MC_NOT_FOUND:
                case MSG_RSP_MC_DELETED:
                    state = SW_CRLF;
                    break;

                case MSG_RSP_MC_END:
                    state = SW_CRLF;
                    break;

                case MSG_RSP_MC_VALUE:
                    state = SW_SPACES_BEFORE_KEY;
                    break;

                case MSG_RSP_MC_ERROR:
                    state = SW_CRLF;
                    break;

                case MSG_RSP_MC_CLIENT_ERROR:
                case MSG_RSP_MC_SERVER_ERROR:
                    state = SW_RUNTO_CRLF;
                    break;

                default:
                    NOT_REACHED();
                }

                p = p - 1; /* go back by 1 byte */
            }

            break;

        case SW_SPACES_BEFORE_KEY:
            if (ch != ' ') {
                state = SW_KEY;
                p = p - 1; /* go back by 1 byte */
#if 1 //shenzheng 2015-1-20 replication pool
				//token = NULL;
				r->res_key_token = NULL;
#endif //shenzheng 2015-4-3 replication pool
            }

            break;

        case SW_KEY:
#if 1 //shenzheng 2015-1-20 replication pool
			/*if(token == NULL)
			{
				token = p;
			}
			*/
			if(r->res_key_token == NULL)
			{
				r->res_key_token = p;
			}
#endif //shenzheng 2015-4-3 replication pool
            if (ch == ' ') {
                /* r->token = NULL; */
                state = SW_SPACES_BEFORE_FLAGS;
#if 1 //shenzheng 2015-1-20 replication pool
				struct keypos *kpos;

				kpos = array_push(r->keys);
				if (kpos == NULL) {
					goto error;
				}
				//ASSERT(token != NULL);
				//ASSERT(token >= b->pos && token < b->last);
				//kpos->start = token;

				ASSERT(r->res_key_token != NULL);
				ASSERT(r->res_key_token >= b->pos && r->res_key_token < b->last);
				kpos->start = r->res_key_token;
				
				kpos->end = p;

				ASSERT(kpos->start >= b->pos && kpos->start < b->last);
				ASSERT(kpos->end >= b->pos && kpos->end < b->last);
				//kpos->end_mbuf = b;
#endif //shenzheng 2015-4-3 replication pool
            }

            break;

        case SW_SPACES_BEFORE_FLAGS:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                state = SW_FLAGS;
                p = p - 1; /* go back by 1 byte */
            }

            break;

        case SW_FLAGS:
            if (r->token == NULL) {
                /* flags_start <- p */
                /* r->token = p; */
            }

            if (isdigit(ch)) {
                /* flags <- flags * 10 + (ch - '0') */
                ;
            } else if (ch == ' ') {
                /* flags_end <- p - 1 */
                /* r->token = NULL; */
                state = SW_SPACES_BEFORE_VLEN;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_VLEN:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1; /* go back by 1 byte */
                state = SW_VLEN;
                r->vlen = 0;
            }

            break;

        case SW_VLEN:
            if (isdigit(ch)) {
                r->vlen = r->vlen * 10 + (uint32_t)(ch - '0');
            } else if (ch == ' ' || ch == CR) {
                /* vlen_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                /* r->token = NULL; */
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RUNTO_VAL:
            switch (ch) {
            case LF:
                /* val_start <- p + 1 */
                state = SW_VAL;
                r->token = NULL;
#if 1 //shenzheng 2015-3-31 replication pool
				//token = NULL;
				r->res_key_token = NULL;
#endif //shenzheng 2015-4-3 replication pool
                break;

            default:
                goto error;
            }

            break;

        case SW_VAL:
            m = p + r->vlen;
            if (m >= b->last) {
                ASSERT(r->vlen >= (uint32_t)(b->last - p));
                r->vlen -= (uint32_t)(b->last - p);
                m = b->last - 1;
                p = m; /* move forward by vlen bytes */
                break;
            }
            switch (*m) {
            case CR:
                /* val_end <- p - 1 */
                p = m; /* move forward by vlen bytes */
                state = SW_VAL_LF;
                break;

            default:
                goto error;
            }

            break;

        case SW_VAL_LF:
            switch (ch) {
            case LF:
                /* state = SW_END; */
                state = SW_RSP_STR;
                break;

            default:
                goto error;
            }

            break;

        case SW_END:
            if (r->token == NULL) {
                if (ch != 'E') {
                    goto error;
                }
                /* end_start <- p */
                r->token = p;
            } else if (ch == CR) {
                /* end_end <- p */
                m = r->token;
                r->token = NULL;

                switch (p - m) {
                case 3:
                    if (str4cmp(m, 'E', 'N', 'D', '\r')) {
                        r->end = m;
                        state = SW_ALMOST_DONE;
                    }
                    break;

                default:
                    goto error;
                }
            }

            break;

        case SW_RUNTO_CRLF:
            switch (ch) {
            case CR:
                if (r->type == MSG_RSP_MC_VALUE) {
                    state = SW_RUNTO_VAL;
                } else {
                    state = SW_ALMOST_DONE;
                }

                break;

            default:
                break;
            }

            break;

        case SW_CRLF:
            switch (ch) {
            case ' ':
                break;

            case CR:
                state = SW_ALMOST_DONE;
                break;

            default:
                goto error;
            }

            break;

        case SW_ALMOST_DONE:
            switch (ch) {
            case LF:
                /* rsp_end <- p */
                goto done;

            default:
                goto error;
            }

            break;

        case SW_SENTINEL:
        default:
            NOT_REACHED();
            break;

        }
    }

    ASSERT(p == b->last);
    r->pos = p;
    r->state = state;

    if (b->last == b->end && r->token != NULL) {
        if (state <= SW_RUNTO_VAL || state == SW_CRLF || state == SW_ALMOST_DONE) {
            r->state = SW_START;

#if 1 //shenzheng 2015-2-4 replication pool
			//if (token != NULL)
			if(r->res_key_token != NULL)
			{
				ASSERT(array_n(r->keys) > 0);
				struct keypos *kpos;
				kpos = array_pop(r->keys);
				ASSERT(kpos != NULL);
				r->res_key_token = NULL;
				//nc_free(kpos);
				//kpos = NULL;
			}
#endif //shenzheng 2015-4-3 replication pool
        }

        r->pos = r->token;
        r->token = NULL;

        r->result = MSG_PARSE_REPAIR;
    } else {
        r->result = MSG_PARSE_AGAIN;
    }

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed rsp %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

done:
    ASSERT(r->type > MSG_UNKNOWN && r->type < MSG_SENTINEL);
    r->pos = p + 1;
    ASSERT(r->pos <= b->last);
    r->state = SW_START;
    r->token = NULL;
    r->result = MSG_PARSE_OK;

#if 1 //shenzheng 2015-4-3 replication pool
	r->res_key_token = NULL;
#endif //shenzheng 2015-4-3 replication pool

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed rsp %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

error:
    r->result = MSG_PARSE_ERROR;
    r->state = state;
    errno = EINVAL;

    log_hexdump(LOG_INFO, b->pos, mbuf_length(b), "parsed bad rsp %"PRIu64" "
                "res %d type %d state %d", r->id, r->result, r->type,
                r->state);
}

static rstatus_t
memcache_append_key(struct msg *r, uint8_t *key, uint32_t keylen)
{
    struct mbuf *mbuf;
    struct keypos *kpos;

    mbuf = msg_ensure_mbuf(r, keylen + 2);
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }

    kpos = array_push(r->keys);
    if (kpos == NULL) {
        return NC_ENOMEM;
    }

    kpos->start = mbuf->last;
    kpos->end = mbuf->last + keylen;
    mbuf_copy(mbuf, key, keylen);
    r->mlen += keylen;

    mbuf_copy(mbuf, (uint8_t *)" ", 1);
    r->mlen += 1;
    return NC_OK;
}

/*
 * read the comment in proto/nc_redis.c
 */
static rstatus_t
memcache_fragment_retrieval(struct msg *r, uint32_t ncontinuum,
                            struct msg_tqh *frag_msgq,
                            uint32_t key_step)
{
    struct mbuf *mbuf;
    struct msg **sub_msgs;
    uint32_t i;
    rstatus_t status;

    sub_msgs = nc_zalloc(ncontinuum * sizeof(*sub_msgs));
    if (sub_msgs == NULL) {
        return NC_ENOMEM;
    }

    ASSERT(r->frag_seq == NULL);
    r->frag_seq = nc_alloc(array_n(r->keys) * sizeof(*r->frag_seq));
    if (r->frag_seq == NULL) {
        nc_free(sub_msgs);
        return NC_ENOMEM;
    }

    mbuf = STAILQ_FIRST(&r->mhdr);
    mbuf->pos = mbuf->start;

    /*
     * This code is based on the assumption that 'gets ' is located
     * in a contiguous location.
     * This is always true because we have capped our MBUF_MIN_SIZE at 512 and
     * whenever we have multiple messages, we copy the tail message into a new mbuf
     */
    for (; *(mbuf->pos) != ' ';) {          /* eat get/gets  */
        mbuf->pos++;
    }
    mbuf->pos++;

    r->frag_id = msg_gen_frag_id();
    r->nfrag = 0;
    r->frag_owner = r;

    for (i = 0; i < array_n(r->keys); i++) {        /* for each  key */
        struct msg *sub_msg;
        struct keypos *kpos = array_get(r->keys, i);
        uint32_t idx = msg_backend_idx(r, kpos->start, kpos->end - kpos->start);

        if (sub_msgs[idx] == NULL) {
            sub_msgs[idx] = msg_get(r->owner, r->request, r->redis);
            if (sub_msgs[idx] == NULL) {
                nc_free(sub_msgs);
                return NC_ENOMEM;
            }
        }
        r->frag_seq[i] = sub_msg = sub_msgs[idx];

        sub_msg->narg++;
        status = memcache_append_key(sub_msg, kpos->start, kpos->end - kpos->start);
        if (status != NC_OK) {
            nc_free(sub_msgs);
            return status;
        }
    }

    for (i = 0; i < ncontinuum; i++) {     /* prepend mget header, and forward it */
        struct msg *sub_msg = sub_msgs[i];
        if (sub_msg == NULL) {
            continue;
        }

        /* prepend get/gets */
        if (r->type == MSG_REQ_MC_GET) {
            status = msg_prepend(sub_msg, (uint8_t *)"get ", 4);
        } else if (r->type == MSG_REQ_MC_GETS) {
            status = msg_prepend(sub_msg, (uint8_t *)"gets ", 5);
        }
        if (status != NC_OK) {
            nc_free(sub_msgs);
            return status;
        }

        /* append \r\n */
        status = msg_append(sub_msg, (uint8_t *)CRLF, CRLF_LEN);
        if (status != NC_OK) {
            nc_free(sub_msgs);
            return status;
        }

        sub_msg->type = r->type;
        sub_msg->frag_id = r->frag_id;
        sub_msg->frag_owner = r->frag_owner;

        TAILQ_INSERT_TAIL(frag_msgq, sub_msg, m_tqe);
        r->nfrag++;
    }

    nc_free(sub_msgs);
    return NC_OK;
}

rstatus_t
memcache_fragment(struct msg *r, uint32_t ncontinuum, struct msg_tqh *frag_msgq)
{
    if (memcache_retrieval(r)) {
        return memcache_fragment_retrieval(r, ncontinuum, frag_msgq, 1);
    }
    return NC_OK;
}

/*
 * Pre-coalesce handler is invoked when the message is a response to
 * the fragmented multi vector request - 'get' or 'gets' and all the
 * responses to the fragmented request vector hasn't been received
 */
void
memcache_pre_coalesce(struct msg *r)
{
    struct msg *pr = r->peer; /* peer request */
    struct mbuf *mbuf;

    ASSERT(!r->request);
    ASSERT(pr->request);

    if (pr->frag_id == 0) {
        /* do nothing, if not a response to a fragmented request */
        return;
    }

    pr->frag_owner->nfrag_done++;
    switch (r->type) {

    case MSG_RSP_MC_VALUE:
    case MSG_RSP_MC_END:

        /*
         * Readjust responses of the fragmented message vector by not
         * including the end marker for all
         */

        ASSERT(r->end != NULL);

        for (;;) {
            mbuf = STAILQ_LAST(&r->mhdr, mbuf, next);
            ASSERT(mbuf != NULL);

            /*
             * We cannot assert that end marker points to the last mbuf
             * Consider a scenario where end marker points to the
             * penultimate mbuf and the last mbuf only contains spaces
             * and CRLF: mhdr -> [...END] -> [\r\n]
             */

            if (r->end >= mbuf->pos && r->end < mbuf->last) {
                /* end marker is within this mbuf */
                r->mlen -= (uint32_t)(mbuf->last - r->end);
                mbuf->last = r->end;
                break;
            }

            /* end marker is not in this mbuf */
            r->mlen -= mbuf_length(mbuf);
            mbuf_remove(&r->mhdr, mbuf);
            mbuf_put(mbuf);
        }

        break;

    default:
        /*
         * Valid responses for a fragmented requests are MSG_RSP_MC_VALUE or,
         * MSG_RSP_MC_END. For an invalid response, we send out SERVER_ERRROR
         * with EINVAL errno
         */
        mbuf = STAILQ_FIRST(&r->mhdr);
        log_hexdump(LOG_ERR, mbuf->pos, mbuf_length(mbuf), "rsp fragment "
                    "with unknown type %d", r->type);
        pr->error = 1;
        pr->err = EINVAL;
        break;
    }
}

/*
 * copy one response from src to dst
 * return bytes copied
 * */
static rstatus_t
memcache_copy_bulk(struct msg *dst, struct msg *src)
{
    struct mbuf *mbuf, *nbuf;
    uint8_t *p;
    uint32_t len = 0;
    uint32_t bytes = 0;
    uint32_t i = 0;

    for (mbuf = STAILQ_FIRST(&src->mhdr);
         mbuf && mbuf_empty(mbuf);
         mbuf = STAILQ_FIRST(&src->mhdr)) {

        mbuf_remove(&src->mhdr, mbuf);
        mbuf_put(mbuf);
    }

    mbuf = STAILQ_FIRST(&src->mhdr);
    if (mbuf == NULL) {
        return NC_OK;           /* key not exists */
    }
    p = mbuf->pos;

#if 1 //shenzheng 2015-2-3 replication pool
	msg_print(src, LOG_DEBUG);
	//log_debug(LOG_DEBUG, "p: %s", p);
#endif //shenzheng 2015-2-3 replication pool

    /* get : VALUE key 0 len\r\nval\r\n */
    /* gets: VALUE key 0 len cas\r\nval\r\n */

    ASSERT(*p == 'V');
    for (i = 0; i < 3; i++) {                 /*  eat 'VALUE key 0 '  */
        for (; *p != ' ';) {
            p++;
        }
        p++;
    }

    len = 0;
    for (; p < mbuf->last && isdigit(*p); p++) {
        len = len * 10 + (uint32_t)(*p - '0');
    }

    for (; p < mbuf->last && ('\r' != *p); p++) { /* eat cas for gets */
        ;
    }

    len += CRLF_LEN * 2;
    len += (p - mbuf->pos);

    bytes = len;

    /* copy len bytes to dst */
    for (; mbuf;) {
        if (mbuf_length(mbuf) <= len) {   /* steal this mbuf from src to dst */
            nbuf = STAILQ_NEXT(mbuf, next);
            mbuf_remove(&src->mhdr, mbuf);
            mbuf_insert(&dst->mhdr, mbuf);
            len -= mbuf_length(mbuf);
            mbuf = nbuf;
        } else {                        /* split it */
            nbuf = mbuf_get();
            if (nbuf == NULL) {
                return NC_ENOMEM;
            }
            mbuf_copy(nbuf, mbuf->pos, len);
            mbuf_insert(&dst->mhdr, nbuf);
            mbuf->pos += len;
            break;
        }
    }

    dst->mlen += bytes;
    src->mlen -= bytes;
    log_debug(LOG_VVERB, "memcache_copy_bulk copy bytes: %d", bytes);
    return NC_OK;
}

/*
 * Post-coalesce handler is invoked when the message is a response to
 * the fragmented multi vector request - 'get' or 'gets' and all the
 * responses to the fragmented request vector has been received and
 * the fragmented request is consider to be done
 */
void
memcache_post_coalesce(struct msg *request)
{
    struct msg *response = request->peer;
    struct msg *sub_msg;
    uint32_t i;
    rstatus_t status;

    ASSERT(!response->request);
    ASSERT(request->request && (request->frag_owner == request));
    if (request->error || request->ferror) {
        response->owner->err = 1;
        return;
    }

    for (i = 0; i < array_n(request->keys); i++) {      /* for each  key */
        sub_msg = request->frag_seq[i]->peer;           /* get it's peer response */
        if (sub_msg == NULL) {
            response->owner->err = 1;
            return;
        }
        status = memcache_copy_bulk(response, sub_msg);
        if (status != NC_OK) {
            response->owner->err = 1;
            return;
        }
    }

    /* append END\r\n */
    status = msg_append(response, (uint8_t *)"END\r\n", 5);
    if (status != NC_OK) {
        response->owner->err = 1;
        return;
    }
}

rstatus_t
memcache_add_auth_packet(struct context *ctx, struct conn *c_conn, struct conn *s_conn)
{
    NOT_REACHED();
    return NC_OK;
}

rstatus_t
memcache_reply(struct msg *r)
{
    NOT_REACHED();
    return NC_OK;
}

#if 1 //shenzheng 2015-1-15 replication pool
rstatus_t
memcache_handle_result_old(struct msg *r)
{	

	struct msg *msg_m, *pmsg_m;	/* master msg and peer msg*/
	msg_m = r;
	ASSERT(msg_m->request);
	ASSERT(msg_m->frag_id == 0);
	pmsg_m = msg_m->peer;
	if(r->server_pool_id == -2)
	{
		ASSERT(r->type == MSG_REQ_MC_SET);
		ASSERT(r->replication_mode != 0);
		struct mbuf *mbuf_elem;
		int i;
		uint8_t ch;
		char command_msg[300], result_msg[100];
		int command_msg_len = 0;
		uint32_t result_msg_len = 0;
		if(msg_m->error)
		{
			char *err = strerror(msg_m->err);
			result_msg_len = strlen(err);
			if(result_msg_len >= 100)
			{
				result_msg_len = 99;
			}

			strncpy(result_msg, err, result_msg_len);
		}
		else
		{
			if(pmsg_m->type == MSG_RSP_MC_STORED)
			{
				return NC_OK;
			}
			STAILQ_FOREACH(mbuf_elem, &pmsg_m->mhdr, next) {
				if(result_msg_len >= 100)
				{
					result_msg[99] = '\0';
					break;
				}
				for(i = 0; i < mbuf_elem->last - mbuf_elem->start; i++)
				{	
					if(result_msg_len > 100)
					{
						result_msg[99] = '\0';
						break;
					}
					ch = *(mbuf_elem->start + i);
					if(ch == LF || ch == CR)
					{
						result_msg[result_msg_len ++] = ' ';
					}
					else
					{
						result_msg[result_msg_len ++] = ch;
					}
				}
			}
		}

		if(result_msg_len < 100)
		{
			result_msg[result_msg_len] = '\0';
		}
		for(i = result_msg_len - 1; i >0; i --)
		{
			if(result_msg[i] == ' ')
			{
				result_msg[i] = '\0';
			}
			else
			{
				break;
			}
		}

		
		STAILQ_FOREACH(mbuf_elem, &msg_m->mhdr, next) {
			if(command_msg_len >= 300)
			{
				command_msg[299] = '\0';
				break;
			}
			for(i = 0; i < mbuf_elem->last - mbuf_elem->start; i++)
			{	
				if(command_msg_len > 300)
				{
					command_msg[299] = '\0';
					break;
				}
				ch = *(mbuf_elem->start + i);
				if(ch == LF || ch == CR)
				{
					command_msg[command_msg_len ++] = ' ';
				}
				else
				{
					command_msg[command_msg_len ++] = ch;
				}
			}
		}
		if(command_msg_len < 300)
		{
			command_msg[command_msg_len] = '\0';
		}
		for(i = command_msg_len - 1; i >0; i --)
		{
			if(command_msg[i] == ' ')
			{
				command_msg[i] = '\0';
			}
			else
			{
				break;
			}
		}
		
		loga("warning: command '%.*s' for write back to master pool error: '%.*s'.", 
			command_msg_len, command_msg, result_msg_len, result_msg);		
		return NC_OK;
	}
	
	ASSERT(msg_m->nreplication_msgs >= 0);
	if(msg_m->nreplication_msgs == 0)
	{
		return NC_OK;
	}
	if(msg_m->frag_id)
	{
		return NC_OK;
	}
	if(msg_m->replication_mode == 0)
	{
		return NC_OK;
	}
	else if(msg_m->replication_mode == 1)
	{
		if(!msg_m->self_done || !replication_msgs_done(msg_m))
		{
			return NC_OK;
		}
	}
	struct msg *msg_s, *pmsg_s;	/* slave msg, peer msg */
	struct msg *msg_tmp;
	
	ASSERT(msg_m->server_pool_id == -1);
	ASSERT(msg_m->nreplication_msgs > 0);
	ASSERT(msg_m->replication_msgs != NULL);
	msg_s = msg_m->replication_msgs[0];
	ASSERT(msg_s != NULL);
	pmsg_s = msg_s->peer;
	ASSERT(msg_m->type == msg_s->type);
	
	log_debug(LOG_DEBUG, "memcache_handle_result real execute!");
	if(msg_s->error && !msg_m->error)
	{
		
	}
	else if(!msg_s->error && !msg_m->error)
	{
		ASSERT(pmsg_m != NULL);
		ASSERT(pmsg_s != NULL);
		if(pmsg_m->type != pmsg_s->type)
		{
			switch(msg_m->type)
			{
				case MSG_REQ_MC_SET:
				case MSG_REQ_MC_CAS:
				case MSG_REQ_MC_ADD:
				case MSG_REQ_MC_REPLACE:
				case MSG_REQ_MC_APPEND:
				case MSG_REQ_MC_PREPEND:
					if(pmsg_s->type == MSG_RSP_MC_STORED)
					{
						return NC_OK;
					}
					break;
				case MSG_REQ_MC_DELETE:
					if(pmsg_s->type == MSG_RSP_MC_DELETED)
					{
						return NC_OK;
					}
					break;
				case MSG_REQ_MC_INCR:
				case MSG_REQ_MC_DECR:
					if(pmsg_s->type == MSG_RSP_MC_NUM)
					{
						return NC_OK;
					}
					break;
				case MSG_REQ_MC_QUIT:
					log_error("error: REQ_MC_QUIT can not be used here!");
					return NC_ERROR;
					break;
				default:
					return NC_OK;
					break;	
			}
		}
		else
		{
			return NC_OK;
		}
	}
	else
	{
		return NC_OK;
	}

	if(msg_m->replication_mode == 1)
	{

		struct mbuf *mbuf_elem;
		int i;
		uint8_t ch;
		char command_msg[300], result_msg[100];
		int command_msg_len = 0;
		uint32_t result_msg_len = 0;
		STAILQ_FOREACH(mbuf_elem, &msg_m->mhdr, next) {
			if(command_msg_len >= 300)
			{
				command_msg[299] = '\0';
				break;
			}
			for(i = 0; i < mbuf_elem->last - mbuf_elem->start; i++)
			{	
				if(command_msg_len > 300)
				{
					command_msg[299] = '\0';
					break;
				}
				ch = *(mbuf_elem->start + i);
				if(ch == LF || ch == CR)
				{
					command_msg[command_msg_len ++] = ' ';
				}
				else
				{
					command_msg[command_msg_len ++] = ch;
				}
			}
		}
		if(command_msg_len < 300)
		{
			command_msg[command_msg_len] = '\0';
		}
		for(i = command_msg_len - 1; i >0; i --)
		{
			if(command_msg[i] == ' ')
			{
				command_msg[i] = '\0';
			}
			else
			{
				break;
			}
		}

		if(msg_s->error)
		{
			char *err = strerror(msg_s->err);
			result_msg_len = strlen(err);
			if(result_msg_len >= 100)
			{
				result_msg_len = 99;
			}

			strncpy(result_msg, err, result_msg_len);
		}
		else
		{
			STAILQ_FOREACH(mbuf_elem, &pmsg_s->mhdr, next) {
				if(result_msg_len >= 100)
				{
					result_msg[99] = '\0';
					break;
				}
				for(i = 0; i < mbuf_elem->last - mbuf_elem->start; i++)
				{	
					if(result_msg_len > 100)
					{
						result_msg[99] = '\0';
						break;
					}
					ch = *(mbuf_elem->start + i);
					if(ch == LF || ch == CR)
					{
						result_msg[result_msg_len ++] = ' ';
					}
					else
					{
						result_msg[result_msg_len ++] = ch;
					}
				}
			}
		}
		if(result_msg_len < 100)
		{
			result_msg[result_msg_len] = '\0';
		}
		for(i = result_msg_len - 1; i >0; i --)
		{
			if(result_msg[i] == ' ')
			{
				result_msg[i] = '\0';
			}
			else
			{
				break;
			}
		}
		
		loga("warning: response of command '%.*s' is '%.*s' in slave pool, but not same response in master.", 
			command_msg_len, command_msg, result_msg_len, result_msg);
		return NC_OK;
	}
	else if(msg_m->replication_mode == 2)
	{
		log_debug(LOG_DEBUG, "move slave msg to master.");
		struct conn *conn;
		conn = msg_m->owner;
		msg_tmp = TAILQ_PREV(msg_m, msg_tqh, c_tqe);
		TAILQ_REMOVE(&conn->omsg_q, msg_m, c_tqe);
		if(msg_tmp == NULL)
		{
			TAILQ_INSERT_HEAD(&conn->omsg_q, msg_s, c_tqe);
		}
		else
		{
			TAILQ_INSERT_AFTER(&conn->omsg_q, msg_tmp, msg_s, c_tqe);
		}
		ASSERT(msg_m->master_msg == NULL);
		ASSERT(msg_s->nreplication_msgs == -1);
		ASSERT(msg_s->replication_msgs == NULL);
		msg_s->replication_msgs = msg_m->replication_msgs;
		msg_m->replication_msgs = NULL;
		msg_s->replication_msgs[0] = msg_m;
		msg_s->nreplication_msgs = msg_m->nreplication_msgs;
		msg_m->nreplication_msgs = -1;
		msg_m->master_msg = msg_s;
		msg_s->master_msg = NULL;
		msg_s->server_pool_id = -1;
		msg_m->server_pool_id = 0;
		return NC_OK;
	}
	else
	{
		loga("error: can not reach here.");
	}
	return NC_OK;
}

rstatus_t
memcache_handle_result(struct context *ctx, struct conn *conn, struct msg *r)
{	

	struct msg *msg_m, *pmsg_m;	/* master msg and peer msg*/
	msg_m = r;
	ASSERT(msg_m->request);
	ASSERT(msg_m->frag_id == 0);
	pmsg_m = msg_m->peer;
	if(r->server_pool_id == -2)
	{
		ASSERT(r->type == MSG_REQ_MC_SET);
		ASSERT(r->replication_mode != 0);
		struct mbuf *mbuf_elem;
		int i;
		uint8_t ch;
		char command_msg[300], result_msg[100];
		int command_msg_len = 0;
		uint32_t result_msg_len = 0;
		if(msg_m->error)
		{
			char *err = strerror(msg_m->err);
			result_msg_len = strlen(err);
			if(result_msg_len >= 100)
			{
				result_msg_len = 99;
			}

			strncpy(result_msg, err, result_msg_len);
		}
		else
		{
			if(pmsg_m->type == MSG_RSP_MC_STORED)
			{
				return NC_OK;
			}
			STAILQ_FOREACH(mbuf_elem, &pmsg_m->mhdr, next) {
				if(result_msg_len >= 100)
				{
					result_msg[99] = '\0';
					break;
				}
				for(i = 0; i < mbuf_elem->last - mbuf_elem->start; i++)
				{	
					if(result_msg_len > 100)
					{
						result_msg[99] = '\0';
						break;
					}
					ch = *(mbuf_elem->start + i);
					if(ch == LF || ch == CR)
					{
						result_msg[result_msg_len ++] = ' ';
					}
					else
					{
						result_msg[result_msg_len ++] = ch;
					}
				}
			}
		}

		if(result_msg_len < 100)
		{
			result_msg[result_msg_len] = '\0';
		}
		for(i = result_msg_len - 1; i >0; i --)
		{
			if(result_msg[i] == ' ')
			{
				result_msg[i] = '\0';
			}
			else
			{
				break;
			}
		}

		
		STAILQ_FOREACH(mbuf_elem, &msg_m->mhdr, next) {
			if(command_msg_len >= 300)
			{
				command_msg[299] = '\0';
				break;
			}
			for(i = 0; i < mbuf_elem->last - mbuf_elem->start; i++)
			{	
				if(command_msg_len > 300)
				{
					command_msg[299] = '\0';
					break;
				}
				ch = *(mbuf_elem->start + i);
				if(ch == LF || ch == CR)
				{
					command_msg[command_msg_len ++] = ' ';
				}
				else
				{
					command_msg[command_msg_len ++] = ch;
				}
			}
		}
		if(command_msg_len < 300)
		{
			command_msg[command_msg_len] = '\0';
		}
		for(i = command_msg_len - 1; i >0; i --)
		{
			if(command_msg[i] == ' ')
			{
				command_msg[i] = '\0';
			}
			else
			{
				break;
			}
		}
		
		loga("warning: command '%.*s' for write back to master pool error: '%.*s'.", 
			command_msg_len, command_msg, result_msg_len, result_msg);		
		return NC_OK;
	}
	
	ASSERT(msg_m->nreplication_msgs >= 0);
	if(msg_m->nreplication_msgs == 0)
	{
		return NC_OK;
	}
	if(msg_m->frag_id)
	{
		return NC_OK;
	}
	if(msg_m->replication_mode == 0)
	{
		return NC_OK;
	}
	else if(msg_m->replication_mode == 1)
	{
		if(!msg_m->self_done || !replication_msgs_done(msg_m))
		{
			return NC_OK;
		}
	}
	struct msg *msg_s, *pmsg_s;	/* slave msg, peer msg */
	struct msg *msg_tmp;
	
	ASSERT(msg_m->server_pool_id == -1);
	ASSERT(msg_m->nreplication_msgs > 0);
	ASSERT(msg_m->replication_msgs != NULL);
	msg_s = msg_m->replication_msgs[0];
	ASSERT(msg_s != NULL);
	pmsg_s = msg_s->peer;
	ASSERT(msg_m->type == msg_s->type);
	
	log_debug(LOG_DEBUG, "memcache_handle_result real execute!");

	msg_print(pmsg_m, LOG_DEBUG);
	msg_print(pmsg_s, LOG_DEBUG);
	if(!msg_content_cmp(pmsg_m, pmsg_s))
	{
		log_debug(LOG_DEBUG, "pmsg_m != pmsg_s");
	}
	else
	{
		log_debug(LOG_DEBUG, "pmsg_m == pmsg_s");
	}

	if(msg_m->replication_mode == 1)
	{
		uint16_t type;
		if(!msg_m->error && !msg_s->error)	//all ok
		{
			if(pmsg_m->type != pmsg_s->type)
			{
				incon_m_s_svd_in_f[26] = '0';

				ASSERT(pmsg_m->type < 999);
				ASSERT(pmsg_s->type < 999);
				type = pmsg_m->type;
				incon_m_s_svd_in_f[28] = type/100 + '0';
				type = type%100;
				incon_m_s_svd_in_f[29] = type/10 + '0';
				incon_m_s_svd_in_f[30] = type%10 + '0';

				type = pmsg_s->type;
				incon_m_s_svd_in_f[32] = type/100 + '0';
				type = type%100;
				incon_m_s_svd_in_f[33] = type/10 + '0';
				incon_m_s_svd_in_f[34] = type%10 + '0';
				
			}
			else if((msg_m->type == MSG_REQ_MC_INCR || msg_m->type == MSG_REQ_MC_DECR) 
				&& pmsg_m->type == MSG_RSP_MC_NUM && !msg_content_cmp(pmsg_m, pmsg_s))
			{
				incon_m_s_svd_in_f[26] = '0';

				ASSERT(pmsg_m->type < 999);
				ASSERT(pmsg_s->type < 999);
				type = pmsg_m->type;
				incon_m_s_svd_in_f[28] = type/100 + '0';
				type = type%100;
				incon_m_s_svd_in_f[29] = type/10 + '0';
				incon_m_s_svd_in_f[30] = type%10 + '0';

				type = pmsg_s->type;
				incon_m_s_svd_in_f[32] = type/100 + '0';
				type = type%100;
				incon_m_s_svd_in_f[33] = type/10 + '0';
				incon_m_s_svd_in_f[34] = type%10 + '0';
			}
			else
			{
				return NC_OK;
			}
		}
		else if(!msg_m->error && msg_s->error)	//master ok; slave err
		{
			incon_m_s_svd_in_f[26] = '1';

			ASSERT(pmsg_m->type < 999);
			ASSERT(msg_s->err < 999);
			type = pmsg_m->type;
			incon_m_s_svd_in_f[28] = type/100 + '0';
			type = type%100;
			incon_m_s_svd_in_f[29] = type/10 + '0';
			incon_m_s_svd_in_f[30] = type%10 + '0';

			type = msg_s->err;
			incon_m_s_svd_in_f[32] = type/100 + '0';
			type = type%100;
			incon_m_s_svd_in_f[33] = type/10 + '0';
			incon_m_s_svd_in_f[34] = type%10 + '0';
		}
		else if(msg_m->error && !msg_s->error)	//master err; slave ok
		{
			incon_m_s_svd_in_f[26] = '2';

			ASSERT(msg_m->err < 999);
			ASSERT(pmsg_s->type < 999);
			type = msg_m->err;
			incon_m_s_svd_in_f[28] = type/100 + '0';
			type = type%100;
			incon_m_s_svd_in_f[29] = type/10 + '0';
			incon_m_s_svd_in_f[30] = type%10 + '0';

			type = pmsg_s->type;
			incon_m_s_svd_in_f[32] = type/100 + '0';
			type = type%100;
			incon_m_s_svd_in_f[33] = type/10 + '0';
			incon_m_s_svd_in_f[34] = type%10 + '0';
		}
		else if(msg_m->err != msg_s->err)	//all err and errs are different
		{
			incon_m_s_svd_in_f[26] = '3';

			ASSERT(msg_m->err < 999);
			ASSERT(msg_s->err < 999);
			type = msg_m->err;
			incon_m_s_svd_in_f[28] = type/100 + '0';
			type = type%100;
			incon_m_s_svd_in_f[29] = type/10 + '0';
			incon_m_s_svd_in_f[30] = type%10 + '0';

			type = msg_s->err;
			incon_m_s_svd_in_f[32] = type/100 + '0';
			type = type%100;
			incon_m_s_svd_in_f[33] = type/10 + '0';
			incon_m_s_svd_in_f[34] = type%10 + '0';
		}
		else
		{
			return NC_OK;
		}

		ASSERT(conn != NULL && !conn->proxy);
		struct server_pool *sp;
		if(conn->client)
		{
			sp = conn->owner;
#if 1 //shenzheng 2015-5-15 config-reload
			//if this conn move from replication pool to none replication pool,
			//sp->inconsistent_log will become NULL.
			if(sp->inconsistent_log == NULL)
			{
				return NC_OK;
			}
#endif //shenzheng 2015-5-15 config-reload
		}
		else
		{
			struct server *server = conn->owner;
			ASSERT(server != NULL);
			sp = server->owner;
			ASSERT(sp != NULL);
			if(sp->sp_master != NULL)
			{
				sp = sp->sp_master;
			}
		}
		ASSERT(sp != NULL);
		struct array *keys = msg_m->keys;
		ASSERT(keys != NULL && array_n(keys) == 1);
		struct keypos *key = array_get(keys, 0);
		ASSERT(key != NULL);
		uint32_t key_len = key->end - key->start;
		ASSERT(key_len >= 0);
		struct string string_tmp;
		struct timeval tv;
		int len;
		gettimeofday(&tv, NULL);
		len = nc_strftime(incon_m_s_svd_in_f, incon_m_s_svd_in_f_len, "%Y-%m-%d_%H:%M:%S ", localtime(&tv.tv_sec));
		ASSERT(len == 20);

		string_init(&string_tmp);
		nc_itos(&string_tmp, msg_m->replication_mode);

		if(msg_m->replication_mode == 0)
		{
			incon_m_s_svd_in_f[len++] = '0';
		}
		else if(msg_m->replication_mode == 1)
		{
			incon_m_s_svd_in_f[len++] = '1';
		}
		else if(msg_m->replication_mode == 2)
		{
			incon_m_s_svd_in_f[len++] = '2';
		}
		else
		{
			incon_m_s_svd_in_f[len++] = 'n';
		}

		incon_m_s_svd_in_f[len++] = ' ';

		ASSERT(msg_m->type < 999);

		type = msg_m->type;
		incon_m_s_svd_in_f[len++] = type/100 + '0';
		type = type%100;
		incon_m_s_svd_in_f[len++] = type/10 + '0';
		incon_m_s_svd_in_f[len++] = type%10 + '0';

		incon_m_s_svd_in_f[len++] = ' ';

		ASSERT(len == 26);

		incon_m_s_svd_in_f[27] = ' ';
		incon_m_s_svd_in_f[31] = ' ';
		incon_m_s_svd_in_f[35] = ' ';
		len = 36;
		ASSERT(key_len < incon_m_s_svd_in_f_len - 36);
		nc_memcpy(incon_m_s_svd_in_f + len, key->start, key_len);

		incon_m_s_svd_in_f[len + key_len] = '\n';
		incon_m_s_svd_in_f[len + key_len + 1] = '\0';

		//log_debug(LOG_WARN, "incon_m_s_svd_in_f : %s", incon_m_s_svd_in_f);
		storage_in_inconsistent_log(sp, incon_m_s_svd_in_f);
		stats_pool_incr(ctx, sp, inconsistent);
		return NC_OK;
	}
	else if(msg_m->replication_mode == 2)
	{
		if(msg_s->error && !msg_m->error)
		{
			
		}
		else if(!msg_s->error && !msg_m->error)
		{
			ASSERT(pmsg_m != NULL);
			ASSERT(pmsg_s != NULL);
			if(pmsg_m->type != pmsg_s->type)
			{
				switch(msg_m->type)
				{
					case MSG_REQ_MC_SET:
					case MSG_REQ_MC_CAS:
					case MSG_REQ_MC_ADD:
					case MSG_REQ_MC_REPLACE:
					case MSG_REQ_MC_APPEND:
					case MSG_REQ_MC_PREPEND:
						if(pmsg_s->type == MSG_RSP_MC_STORED)
						{
							return NC_OK;
						}
						break;
					case MSG_REQ_MC_DELETE:
						if(pmsg_s->type == MSG_RSP_MC_DELETED)
						{
							return NC_OK;
						}
						break;
					case MSG_REQ_MC_INCR:
					case MSG_REQ_MC_DECR:
						if(pmsg_s->type == MSG_RSP_MC_NUM)
						{
							return NC_OK;
						}
						break;
					case MSG_REQ_MC_QUIT:
						log_error("error: REQ_MC_QUIT can not be used here!");
						return NC_ERROR;
						break;
					default:
						return NC_OK;
						break;	
				}
			}
			else
			{
				return NC_OK;
			}
		}
		else
		{
			return NC_OK;
		}
		
		log_debug(LOG_DEBUG, "move slave msg to master.");
		struct conn *conn;
		conn = msg_m->owner;
		msg_tmp = TAILQ_PREV(msg_m, msg_tqh, c_tqe);
		TAILQ_REMOVE(&conn->omsg_q, msg_m, c_tqe);
		if(msg_tmp == NULL)
		{
			TAILQ_INSERT_HEAD(&conn->omsg_q, msg_s, c_tqe);
		}
		else
		{
			TAILQ_INSERT_AFTER(&conn->omsg_q, msg_tmp, msg_s, c_tqe);
		}
		ASSERT(msg_m->master_msg == NULL);
		ASSERT(msg_s->nreplication_msgs == -1);
		ASSERT(msg_s->replication_msgs == NULL);
		msg_s->replication_msgs = msg_m->replication_msgs;
		msg_m->replication_msgs = NULL;
		msg_s->replication_msgs[0] = msg_m;
		msg_s->nreplication_msgs = msg_m->nreplication_msgs;
		msg_m->nreplication_msgs = -1;
		msg_m->master_msg = msg_s;
		msg_s->master_msg = NULL;
		msg_s->server_pool_id = -1;
		msg_m->server_pool_id = 0;
		return NC_OK;
	}
	else
	{
		loga("error: can not reach here.");
	}
	return NC_OK;
}


rstatus_t
memcache_replication_penetrate(struct msg *pmsg, 
				struct msg *msg, struct msg_tqh *frag_msgq)
{
	rstatus_t status;
	struct conn *c_conn;
	struct server_pool *sp, **replication_sp;
	struct keypos *kpos_pmsg, *kpos_msg;
	size_t klen_pmsg, klen_msg;
	bool equal_flag;
	uint32_t i, j, k;
	uint32_t array_len;
	uint32_t idx_s;		/* slave pool idx */
	struct msg *frag_owner;
	struct msg **sub_msgs;
	struct msg *sub_msg;
	//struct msg *pre_msg;
	struct array *keys_rep;
	uint32_t keys_rep_num;
	int server_pool_id;
	

	c_conn = pmsg->owner;
	ASSERT(pmsg->request);
	ASSERT(c_conn != NULL);
	ASSERT(c_conn->client && !c_conn->proxy);

	
	if(msg != NULL)
	{
		ASSERT(!msg->request);
		keys_rep = msg->keys;
		ASSERT(keys_rep != NULL);
		keys_rep_num = array_n(keys_rep);
	}
	else
	{
		keys_rep_num = 0;
		keys_rep = NULL;
	}
	
	sp = c_conn->owner;
	frag_owner = pmsg->frag_owner;
	ASSERT(frag_owner != NULL);

	server_pool_id = pmsg->server_pool_id;
	
	replication_sp = array_get(&sp->replication_pools, server_pool_id);
	ASSERT(*replication_sp != NULL);
	sub_msgs = nc_zalloc((*replication_sp)->ncontinuum * sizeof(*sub_msgs));
	if (sub_msgs == NULL) {
		return NC_ENOMEM;
	}
	msg_print(pmsg, LOG_DEBUG);
	//pre_msg = pmsg;
	array_len = array_n(pmsg->keys);
	for(i = 0; i < array_len; i ++)
	{
		kpos_pmsg = array_get(pmsg->keys, i);

		if(kpos_pmsg->start)
		
		klen_pmsg = kpos_pmsg->end - kpos_pmsg->start;
		equal_flag = false;
		for(j = 0; j < keys_rep_num; j ++)
		{
			kpos_msg = array_get(keys_rep, j);
			klen_msg = kpos_msg->end - kpos_msg->start;
			if(klen_pmsg != klen_msg)
			{
				continue;
			}
			if(memcmp(kpos_pmsg->start, kpos_msg->start, klen_pmsg) == 0)
			{
				equal_flag = true;
				break;
			}
		}
		if(!equal_flag)
		{
			idx_s = server_pool_idx(*replication_sp, kpos_pmsg->start, klen_pmsg);

			if (sub_msgs[idx_s] == NULL) {
				sub_msgs[idx_s] = msg_get(pmsg->owner, pmsg->request, pmsg->redis);
				if (sub_msgs[idx_s] == NULL) {
					status = NC_ENOMEM;
					goto error;
				}
			}
			ASSERT(frag_owner->frag_seq != NULL);

			for(k = 0; k < array_n(frag_owner->keys); k ++)
			{
				kpos_msg = array_get(frag_owner->keys, k);
				klen_msg = kpos_msg->end - kpos_msg->start;
				if(klen_pmsg != klen_msg)
				{
					continue;
				}
				if(memcmp(kpos_pmsg->start, kpos_msg->start, klen_pmsg) == 0)
				{
					equal_flag = true;
					break;
				}
			}
			
			ASSERT(equal_flag);

			sub_msg = frag_owner->frag_seq[k];
			ASSERT(sub_msg != NULL);
			ASSERT(sub_msg == pmsg);
			sub_msg->narg --;
			if(sub_msg->narg == 0)
			{	
				/*
				pre_msg = TAILQ_PREV(sub_msg, msg_tqh, c_tqe);
				TAILQ_REMOVE(&c_conn->omsg_q, sub_msg, c_tqe);

				if(sub_msg == pmsg)
				{
					ASSERT( i == (array_len - 1) );
				}
				else
				{
					req_put(sub_msg);
				}
				*/
				ASSERT(i == (array_len - 1));
				
			}
			
			frag_owner->frag_seq[k] = sub_msg = sub_msgs[idx_s];

			sub_msg->narg++;
			msg_print(sub_msg, LOG_DEBUG);
			status = memcache_append_key(sub_msg, kpos_pmsg->start, klen_pmsg);
			if (status != NC_OK) {
				goto error;
			}
		}
		
	}

	for (i = 0; i < (*replication_sp)->ncontinuum; i++) {	   /* prepend mget header, and forward it */
		sub_msg = sub_msgs[i];
		if (sub_msg == NULL) {
			continue;
		}

		/* prepend get/gets */
		if (frag_owner->type == MSG_REQ_MC_GET) {
			status = msg_prepend(sub_msg, (uint8_t *)"get ", 4);
		} else if (frag_owner->type == MSG_REQ_MC_GETS) {
			status = msg_prepend(sub_msg, (uint8_t *)"gets ", 5);
		}
		
		if (status != NC_OK) {
			goto error;
		}

		/* append \r\n */
		status = msg_append(sub_msg, (uint8_t *)CRLF, CRLF_LEN);
		if (status != NC_OK) {
			goto error;
		}

		sub_msg->type = frag_owner->type;
		sub_msg->frag_id = frag_owner->frag_id;
		sub_msg->frag_owner = frag_owner;
		sub_msg->server_pool_id = server_pool_id;

		TAILQ_INSERT_TAIL(frag_msgq, sub_msg, m_tqe);
		//TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);

		/*
		ASSERT(pre_msg != NULL);
		ASSERT(pre_msg->frag_id == sub_msg->frag_id);
		
		if(pre_msg == NULL)
		{
			TAILQ_INSERT_HEAD(&c_conn->omsg_q, sub_msg, c_tqe);
		}
		else
		{
			TAILQ_INSERT_AFTER(&c_conn->omsg_q, pre_msg, sub_msg, c_tqe);
		}
		*/
		TAILQ_INSERT_AFTER(&c_conn->omsg_q, pmsg, sub_msg, c_tqe);
		frag_owner->nfrag ++;
	}

	nc_free(sub_msgs);
	return NC_OK;

error:
	for (i = 0; i < (*replication_sp)->ncontinuum; i++) 
	{
		sub_msg = sub_msgs[i];
		if (sub_msg == NULL) {
			continue;
		}
		for(k = 0; k < array_n(frag_owner->keys); k ++)
		{
			if(frag_owner->frag_seq[k] == sub_msg)
			{
				frag_owner->frag_seq[k] = NULL;
			}
		}
		msg_put(sub_msg);
	}
	nc_free(sub_msgs);
	return status;
	
}

rstatus_t
memcache_replication_write_back_old(struct context *ctx, struct msg *pmsg, struct msg *msg)
{
	if(array_n(msg->keys) == 0)
	{
		return NC_OK;
	}

	rstatus_t status;

	struct conn *c_conn;
	uint8_t *ch_from;
	struct msg * msg_wb;	/* msg to write back  */
	struct mbuf *mbuf_from;
	struct mbuf *mbuf_to;
	bool noreply_append_flag = false;
	bool msg_copy_done = false;
	bool space_flag = false;
	uint8_t k = 0;
	uint8_t str_noreply[8];
	uint8_t str_space_zero[3] = {' ', '0', ' '};
	uint32_t count_msg_wb = 0;	/* total count of msgs to write back */
	uint8_t	crlf_counter = 0;	/* used for split muti value msg */
	uint8_t space_counter = 0;
	bool msg_begin = false;		/* true:at the begin of a msg */
	
	c_conn = pmsg->owner;
	
	ASSERT(pmsg->server_pool_id >= 0);
	ASSERT(pmsg->request);
	ASSERT(!msg->request);
	ASSERT(msg->type == MSG_RSP_MC_END);
	ASSERT(c_conn->client && !c_conn->proxy);


	if(pmsg->replication_mode == 0)
	{
		str_noreply[0] = ' ';
		str_noreply[1] = 'n';
		str_noreply[2] = 'o';
		str_noreply[3] = 'r';
		str_noreply[4] = 'e';
		str_noreply[5] = 'p';
		str_noreply[6] = 'l';
		str_noreply[7] = 'y';
	}
	else
	{
		noreply_append_flag = true;
	}
	
	msg_wb = req_get(c_conn);
	msg_wb->owner = c_conn;
	//msg_wb->server_pool_id = pmsg->server_pool_id;
	msg_wb->server_pool_id = -2;
	/* prepend set */
    status = msg_append(msg_wb, (uint8_t *)"set", 3);
    if (status != NC_OK) {
        nc_free(msg_wb);
        return status;
    }
	mbuf_to = STAILQ_FIRST(&msg_wb->mhdr);
	msg_wb->pos =  mbuf_to->pos;
	
	mbuf_from = STAILQ_FIRST(&msg->mhdr);
	ch_from = mbuf_from->start;
	ASSERT(*ch_from == 'V');
	msg_begin = true;
	do{
		mbuf_to = STAILQ_LAST(&msg_wb->mhdr, mbuf, next);
		if (mbuf_to == NULL || mbuf_full(mbuf_to)) {
			mbuf_to = mbuf_get();
			if (mbuf_to == NULL) {
				req_put(msg_wb);
				return NC_ENOMEM;
			}
			log_debug(LOG_DEBUG, "get new mbuf for msg_t");
			mbuf_insert(&msg_wb->mhdr, mbuf_to);
			msg_wb->pos = mbuf_to->pos;
		}
		ASSERT(mbuf_to->end - mbuf_to->last > 0);

		for(; mbuf_to->last < mbuf_to->end;)
		{
			if(ch_from >= mbuf_from->last)
			{
				mbuf_from = STAILQ_NEXT(mbuf_from, next);
				if(mbuf_from == NULL)
				{
					msg_copy_done = true;
					break;
				}
				ch_from = mbuf_from->start;
			}

			if(crlf_counter == 0)
			{
				if(*ch_from == ' ' && space_flag == false)
				{
					space_flag = true;
					space_counter ++;
				}
				else if(*ch_from != ' ' && space_flag == true)
				{
					space_flag = false;
				}
				else if(*ch_from == ' ' && space_flag == true)
				{
					ch_from ++;
					continue;
				}


				switch(space_counter)
				{
				case 0:
					ch_from ++;
					break;
				case 1:
				case 2:
					*mbuf_to->last = *ch_from;
					ch_from ++;
					mbuf_to->last ++;
					break;
				case 3:
					if(k < 3)
					{
						*mbuf_to->last = str_space_zero[k];
						k ++;
						mbuf_to->last ++;
					}
					else
					{
						*mbuf_to->last = *ch_from;
						ch_from ++;
						mbuf_to->last ++;
					}
					break;
				case 4:
					ch_from ++;
					break;
				default:
					log_error("error: program can not reach here!");
					req_put(msg_wb);
					NOT_REACHED();
					return NC_ERROR;
					break;
				}

				if(*ch_from == CR)
				{
					crlf_counter ++;	
					k = 0;
				}
			}
			else
			{				
				if(*ch_from == CR && !noreply_append_flag)
				{
					*mbuf_to->last = str_noreply[k];
					k ++;
					mbuf_to->last ++;
					if(k >= 8)
					{
						noreply_append_flag = true;
						k = 0;
					}
				}
				else
				{					
					*mbuf_to->last = *ch_from;
					ch_from ++;
					mbuf_to->last ++;

					if(*ch_from == CR)
					{
						crlf_counter ++;	
					}
				}
				
				if(*ch_from == LF && crlf_counter == 2)
				{
					*mbuf_to->last = *ch_from;
					ch_from ++;
					mbuf_to->last ++;
					break;
				}	
			}
		}
		//log_debug(LOG_DEBUG, "msg_wb->pos : %s", msg_wb->pos);
		msg_wb->parser(msg_wb);
		switch (msg_wb->result) {
		case MSG_PARSE_OK:
			log_debug(LOG_DEBUG, "MSG_PARSE_OK and crlf_counter is %d", crlf_counter);
			ASSERT(crlf_counter == 2);
			crlf_counter = 0;
			space_counter = 0;
			space_flag = false;
			if(pmsg->replication_mode == 0)
			{
				noreply_append_flag = false;
			}

			count_msg_wb ++;
			req_forward(ctx, c_conn, msg_wb);
			
			if(count_msg_wb < array_n(msg->keys))
			{			
				msg_wb = req_get(c_conn);
				msg_wb->owner = c_conn;
				//msg_wb->server_pool_id = pmsg->server_pool_id;
				msg_wb->server_pool_id = -2;
				/* prepend set */
    			status = msg_append(msg_wb, (uint8_t *)"set", 3);
			    if (status != NC_OK) {
			        nc_free(msg_wb);
			        return status;
			    }
				mbuf_to = STAILQ_FIRST(&msg_wb->mhdr);
				msg_wb->pos =  mbuf_to->pos;
			}
			else
			{
				//ASSERT(msg_copy_done);
				msg_copy_done = true;
			}
		break;

		case MSG_PARSE_REPAIR:
			log_debug(LOG_DEBUG, "MSG_PARSE_REPAIR and crlf_counter is %d", crlf_counter);
			ASSERT(crlf_counter < 2);
			//status = msg_repair(NULL, c_conn, msg_wb);
			struct mbuf *nbuf;
		    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
		    if (nbuf == NULL) {
				req_put(msg_wb);
		        return NC_ENOMEM;
		    }
		    mbuf_insert(&msg->mhdr, nbuf);
		    msg->pos = nbuf->pos;
		break;

		case MSG_PARSE_AGAIN:
			log_debug(LOG_DEBUG, "MSG_PARSE_AGAIN and crlf_counter is %d", crlf_counter);
			ASSERT(crlf_counter <= 2);
		break;

		default:
			req_put(msg_wb);
			log_error("error: parse the replication msg error!");
			return NC_ERROR;
		break;
		}
	}
	while(!msg_copy_done);

	ASSERT(count_msg_wb == array_n(msg->keys));
	
	return NC_OK;
}

rstatus_t
memcache_replication_write_back1(struct context *ctx, struct msg *pmsg, struct msg *msg)
{
	if(array_n(msg->keys) == 0)
	{
		return NC_OK;
	}

	rstatus_t status;

	struct conn *c_conn;
	uint8_t *ch_from;
	struct msg * msg_wb;	/* msg to write back  */
	struct mbuf *mbuf_from;
	struct mbuf *mbuf_to;
	bool noreply_append_flag = false;
	bool msg_copy_done = false;
	bool space_flag = false;
	uint8_t k = 0;
	uint8_t str_noreply[8];
	uint8_t str_space_zero[3] = {' ', '0', ' '};
	uint32_t count_msg_wb = 0;	/* total count of msgs to write back */
	uint8_t	crlf_counter = 0;	/* used for split muti value msg */
	uint8_t space_counter = 0;
	bool msg_begin = false;		/* true:at the begin of a msg */
	uint32_t value_len = 0;
	
	c_conn = pmsg->owner;
	
	ASSERT(pmsg->server_pool_id >= 0);
	ASSERT(pmsg->request);
	ASSERT(!msg->request);
	ASSERT(msg->type == MSG_RSP_MC_END);
	ASSERT(c_conn->client && !c_conn->proxy);


	if(pmsg->replication_mode == 0)
	{
		str_noreply[0] = ' ';
		str_noreply[1] = 'n';
		str_noreply[2] = 'o';
		str_noreply[3] = 'r';
		str_noreply[4] = 'e';
		str_noreply[5] = 'p';
		str_noreply[6] = 'l';
		str_noreply[7] = 'y';
	}
	else
	{
		noreply_append_flag = true;
	}
	
	msg_wb = req_get(c_conn);
	msg_wb->owner = c_conn;
	//msg_wb->server_pool_id = pmsg->server_pool_id;
	msg_wb->server_pool_id = -2;
	/* prepend set */
    status = msg_append(msg_wb, (uint8_t *)"set", 3);
    if (status != NC_OK) {
        nc_free(msg_wb);
        return status;
    }
	mbuf_to = STAILQ_FIRST(&msg_wb->mhdr);
	msg_wb->pos =  mbuf_to->pos;
	
	mbuf_from = STAILQ_FIRST(&msg->mhdr);
	ch_from = mbuf_from->start;
	ASSERT(*ch_from == 'V');
	msg_begin = true;
	do{
		mbuf_to = STAILQ_LAST(&msg_wb->mhdr, mbuf, next);
		if (mbuf_to == NULL || mbuf_full(mbuf_to)) {
			mbuf_to = mbuf_get();
			if (mbuf_to == NULL) {
				req_put(msg_wb);
				return NC_ENOMEM;
			}
			log_debug(LOG_DEBUG, "get new mbuf for msg_t");
			mbuf_insert(&msg_wb->mhdr, mbuf_to);
			msg_wb->pos = mbuf_to->pos;
		}
		ASSERT(mbuf_to->end - mbuf_to->last > 0);

		for(; mbuf_to->last < mbuf_to->end;)
		{
			if(ch_from >= mbuf_from->last)
			{
				mbuf_from = STAILQ_NEXT(mbuf_from, next);
				if(mbuf_from == NULL)
				{
					msg_copy_done = true;
					break;
				}
				ch_from = mbuf_from->start;
			}

			if(crlf_counter == 0)
			{
				if(*ch_from == ' ' && space_flag == false)
				{
					space_flag = true;
					space_counter ++;
				}
				else if(*ch_from != ' ' && space_flag == true)
				{
					space_flag = false;
				}
				else if(*ch_from == ' ' && space_flag == true)
				{
					ch_from ++;
					continue;
				}


				switch(space_counter)
				{
				case 0:
					ch_from ++;
					break;
				case 1:
				case 2:
					*mbuf_to->last = *ch_from;
					ch_from ++;
					mbuf_to->last ++;
					break;
				case 3:
					if(k < 3)
					{
						*mbuf_to->last = str_space_zero[k];
						k ++;
						mbuf_to->last ++;
					}
					else
					{
						*mbuf_to->last = *ch_from;
						ch_from ++;
						mbuf_to->last ++;
					}
					break;
				case 4:
					ch_from ++;
					break;
				default:
					log_error("error: program can not reach here!");
					req_put(msg_wb);
					NOT_REACHED();
					return NC_ERROR;
					break;
				}

				if(*(ch_from-1) == LF)
				{
					ASSERT(*(ch_from-2) == CR);
					crlf_counter ++;
					k = 0;
					log_debug(LOG_DEBUG, "ch_from:%s",ch_from);
					break;
				}
			}
			else
			{	

				if(!noreply_append_flag)
				{
					*mbuf_to->last = str_noreply[k];
					k ++;
					mbuf_to->last ++;
					if(k >= 8)
					{
						noreply_append_flag = true;
						k = 0;
					}
				}
				else if(value_len > 0)
				{					
					*mbuf_to->last = *ch_from;
					ch_from ++;
					mbuf_to->last ++;
					value_len --;
				}
				else
				{
					ASSERT(value_len == 0);
					if(*ch_from == LF)
					{
						ASSERT(*(ch_from-1) == CR);
						*mbuf_to->last = *ch_from;
						ch_from ++;
						mbuf_to->last ++;
						crlf_counter ++;
						log_debug(LOG_DEBUG, "crlf_counter : %d", crlf_counter);
						break;
					}
					log_debug(LOG_DEBUG, "ch_from:%d", *ch_from);
					*mbuf_to->last = *ch_from;
					ch_from ++;
					mbuf_to->last ++;
				}
			}
		}
		//log_debug(LOG_DEBUG, "msg_wb->pos : %s", msg_wb->pos);
		msg_wb->parser(msg_wb);
		switch (msg_wb->result) {
		case MSG_PARSE_OK:
			log_debug(LOG_DEBUG, "MSG_PARSE_OK and crlf_counter is %d", crlf_counter);
			msg_print(msg_wb, LOG_DEBUG);
			ASSERT(crlf_counter == 2);
			crlf_counter = 0;
			space_counter = 0;
			space_flag = false;
			if(pmsg->replication_mode == 0)
			{
				noreply_append_flag = false;
			}

			count_msg_wb ++;
			req_forward(ctx, c_conn, msg_wb);
			
			if(count_msg_wb < array_n(msg->keys))
			{			
				msg_wb = req_get(c_conn);
				msg_wb->owner = c_conn;
				//msg_wb->server_pool_id = pmsg->server_pool_id;
				msg_wb->server_pool_id = -2;
				/* prepend set */
    			status = msg_append(msg_wb, (uint8_t *)"set", 3);
			    if (status != NC_OK) {
			        nc_free(msg_wb);
			        return status;
			    }
				mbuf_to = STAILQ_FIRST(&msg_wb->mhdr);
				msg_wb->pos =  mbuf_to->pos;
			}
			else
			{
				//ASSERT(msg_copy_done);
				msg_copy_done = true;
			}
		break;

		case MSG_PARSE_REPAIR:
			log_debug(LOG_DEBUG, "MSG_PARSE_REPAIR and crlf_counter is %d", crlf_counter);
			ASSERT(crlf_counter < 2);
			//status = msg_repair(NULL, c_conn, msg_wb);
			struct mbuf *nbuf;
		    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
		    if (nbuf == NULL) {
				req_put(msg_wb);
		        return NC_ENOMEM;
		    }
		    mbuf_insert(&msg->mhdr, nbuf);
		    msg->pos = nbuf->pos;
		break;

		case MSG_PARSE_AGAIN:
			log_debug(LOG_DEBUG, "MSG_PARSE_AGAIN and crlf_counter is %d", crlf_counter);
			ASSERT(crlf_counter <= 2);
		break;

		default:
			msg_print(msg_wb, LOG_DEBUG);
			req_put(msg_wb);
			log_error("error: parse the replication msg error!");
			return NC_ERROR;
		break;
		}
	}
	while(!msg_copy_done);

	ASSERT(count_msg_wb == array_n(msg->keys));
	
	return NC_OK;
}


rstatus_t
memcache_replication_write_back(struct context *ctx, struct msg *pmsg, struct msg *msg)
{
	if(array_n(msg->keys) == 0)
	{
		return NC_OK;
	}

	rstatus_t status;

	struct conn *c_conn;
	uint8_t *ch_from;
	struct msg * msg_wb;	/* msg to write back  */
	struct mbuf *mbuf_from;
	struct mbuf *mbuf_to;
	bool noreply_append_flag = false;
	bool msg_copy_done = false;
	bool space_flag = false;
	uint8_t extra_len = 0;
	uint8_t k = 0;
	uint8_t str_noreply[8];
	uint8_t str_space_zero[3] = {' ', '0', ' '};
	uint32_t count_msg_wb = 0;	/* total count of msgs to write back */
	uint8_t	crlf_counter = 0;	/* used for split muti value msg */
	uint8_t space_counter = 0;
	bool msg_begin = false;		/* true:at the begin of a msg */
	uint32_t vlen = 0;
	uint32_t vlen_copied = 0;

	
	c_conn = pmsg->owner;
	
	ASSERT(pmsg->server_pool_id >= 0);
	ASSERT(pmsg->request);
	ASSERT(!msg->request);
	ASSERT(msg->type == MSG_RSP_MC_END);
	ASSERT(c_conn->client && !c_conn->proxy);


	if(pmsg->replication_mode == 0)
	{
		str_noreply[0] = ' ';
		str_noreply[1] = 'n';
		str_noreply[2] = 'o';
		str_noreply[3] = 'r';
		str_noreply[4] = 'e';
		str_noreply[5] = 'p';
		str_noreply[6] = 'l';
		str_noreply[7] = 'y';

		//extra_len = 10;
		extra_len = 4;
	}
	else
	{
		extra_len = 4;
		noreply_append_flag = true;
	}
	
	msg_wb = req_get(c_conn);
	msg_wb->owner = c_conn;
	//msg_wb->server_pool_id = pmsg->server_pool_id;
	msg_wb->server_pool_id = -2;
	/* prepend set */
    status = msg_append(msg_wb, (uint8_t *)"set", 3);
    if (status != NC_OK) {
        nc_free(msg_wb);
        return status;
    }
	mbuf_to = STAILQ_FIRST(&msg_wb->mhdr);
	msg_wb->pos =  mbuf_to->pos;
	
	mbuf_from = STAILQ_FIRST(&msg->mhdr);
	ch_from = mbuf_from->start;
	ASSERT(*ch_from == 'V');
	msg_begin = true;
	do{
		mbuf_to = STAILQ_LAST(&msg_wb->mhdr, mbuf, next);
		if (mbuf_to == NULL || mbuf_full(mbuf_to)) {
			mbuf_to = mbuf_get();
			if (mbuf_to == NULL) {
				req_put(msg_wb);
				return NC_ENOMEM;
			}
			log_debug(LOG_DEBUG, "get new mbuf for msg_t");
			mbuf_insert(&msg_wb->mhdr, mbuf_to);
			//msg_wb->mlen += mbuf_length(mbuf_to);
			msg_wb->pos = mbuf_to->pos;
		}
		ASSERT(mbuf_to->end - mbuf_to->last > 0);

		for(; mbuf_to->last < mbuf_to->end;)
		{
			if(ch_from >= mbuf_from->last)
			{
				mbuf_from = STAILQ_NEXT(mbuf_from, next);
				if(mbuf_from == NULL)
				{
					msg_copy_done = true;
					break;
				}
				ch_from = mbuf_from->start;
			}

			if(crlf_counter == 0)
			{
				if(*ch_from == ' ' && space_flag == false)
				{
					space_flag = true;
					space_counter ++;
				}
				else if(*ch_from != ' ' && space_flag == true)
				{
					space_flag = false;
				}
				else if(*ch_from == ' ' && space_flag == true)
				{
					ch_from ++;
					continue;
				}


				switch(space_counter)
				{
				case 0:
					ch_from ++;
					break;
				case 1:
				case 2:
					*mbuf_to->last = *ch_from;
					ch_from ++;
					mbuf_to->last ++;
					msg_wb->mlen ++;
					break;
				case 3:
					if(k < 3)
					{
						*mbuf_to->last = str_space_zero[k];
						k ++;
						mbuf_to->last ++;
						msg_wb->mlen ++;
					}
					else
					{
						*mbuf_to->last = *ch_from;
						ch_from ++;
						mbuf_to->last ++;
						msg_wb->mlen ++;
					}
					break;
				case 4:
					ch_from ++;
					break;
				default:
					log_error("error: program can not reach here!");
					req_put(msg_wb);
					NOT_REACHED();
					return NC_ERROR;
					break;
				}

				if(*ch_from == CR)
				{
					crlf_counter ++;	
					k = 0;
					msg_print(msg_wb, LOG_DEBUG);
					break;
				}
			}
			else
			{			
				log_debug(LOG_DEBUG, "vlen : %d", vlen);
				if(vlen == 0)
				{
					
					vlen = msg_wb->vlen;
					/*	memcached allows value len is zero
					if(vlen <= 0)
					{
						msg_print(msg, LOG_ERR);
						msg_print(msg_wb, LOG_ERR);
					}
					ASSERT(vlen > 0);
					*/
				}
				msg_print(msg_wb, LOG_DEBUG);
				log_debug(LOG_DEBUG, "vlen : %d", vlen);
				if(!noreply_append_flag)
				{
					*mbuf_to->last = str_noreply[k];
					k ++;
					mbuf_to->last ++;
					msg_wb->mlen ++;
					if(k >= 8)
					{
						noreply_append_flag = true;
						k = 0;
					}
				}
				else if(vlen_copied < vlen + extra_len)
				{					
					*mbuf_to->last = *ch_from;
					ch_from ++;
					mbuf_to->last ++;
					msg_wb->mlen ++;
					vlen_copied ++;
				}
				else
				{
					break;
				}
				
			}
		}
		msg_wb->parser(msg_wb);
		switch (msg_wb->result) {
		case MSG_PARSE_OK:
			log_debug(LOG_DEBUG, "MSG_PARSE_OK and crlf_counter is %d", crlf_counter);
			msg_print(msg_wb, LOG_DEBUG);
			crlf_counter = 0;
			space_counter = 0;
			space_flag = false;
			vlen = 0;
			vlen_copied = 0;
			if(pmsg->replication_mode == 0)
			{
				noreply_append_flag = false;
			}

			count_msg_wb ++;
			req_forward(ctx, c_conn, msg_wb);
			
			if(count_msg_wb < array_n(msg->keys))
			{			
				msg_wb = req_get(c_conn);
				msg_wb->owner = c_conn;
				//msg_wb->server_pool_id = pmsg->server_pool_id;
				msg_wb->server_pool_id = -2;
				/* prepend set */
    			status = msg_append(msg_wb, (uint8_t *)"set", 3);
			    if (status != NC_OK) {
			        nc_free(msg_wb);
			        return status;
			    }
				mbuf_to = STAILQ_FIRST(&msg_wb->mhdr);
				//msg_wb->mlen += mbuf_length(mbuf_to);
				msg_wb->pos =  mbuf_to->pos;
			}
			else
			{
				//ASSERT(msg_copy_done);
				msg_copy_done = true;
			}
		break;

		case MSG_PARSE_REPAIR:
			log_debug(LOG_DEBUG, "MSG_PARSE_REPAIR and crlf_counter is %d", crlf_counter);
			ASSERT(crlf_counter < 2);
			//status = msg_repair(NULL, c_conn, msg_wb);
			/*
			struct mbuf *nbuf;
		    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
		    if (nbuf == NULL) {
				req_put(msg_wb);
		        return NC_ENOMEM;
		    }
		    mbuf_insert(&msg->mhdr, nbuf);
		    msg->pos = nbuf->pos;
			*/
			struct mbuf *nbuf;
		    nbuf = mbuf_split(&msg_wb->mhdr, msg_wb->pos, NULL, NULL);
		    if (nbuf == NULL) {
				req_put(msg_wb);
		        return NC_ENOMEM;
		    }
		    mbuf_insert(&msg_wb->mhdr, nbuf);
		    msg_wb->pos = nbuf->pos;
		break;

		case MSG_PARSE_AGAIN:
			log_debug(LOG_DEBUG, "MSG_PARSE_AGAIN and crlf_counter is %d", crlf_counter);
			ASSERT(crlf_counter <= 2);
		break;

		default:
			req_put(msg_wb);
			log_error("error: parse the replication msg error!");
			return NC_ERROR;
		break;
		}
	}
	while(!msg_copy_done);

	ASSERT(count_msg_wb == array_n(msg->keys));
	
	return NC_OK;
}


#endif //shenzheng 2015-2-27 replication pool
