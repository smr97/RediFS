/*
 * RediFS
 *
 * Redis File System based on FUSE
 * Copyright (C) 2011 Dave van Soest <dave@thebinarykid.nl>
 *
 * This program can be distributed under the terms of the GNU GPL.
 * See the file COPYING.
*/


// ---- Includes:
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <hiredis/hiredis.h>

#include "connection.h"


struct redis_connection_info
{
    const char* host;
    redifs_port_t port;
};


// ---- Redis globals:
static struct redis_connection_info redis1_info = { NULL, 0 };
static redisContext* redis1 = NULL;


#define MAX_OPEN_REPLIES 400
static redisReply* openReplies[MAX_OPEN_REPLIES];
#define OPEN_REPLIES_BITMAP_SIZE ((MAX_OPEN_REPLIES - 1) / sizeof(unsigned int)) + 1
static unsigned int openRepliesBitmap[OPEN_REPLIES_BITMAP_SIZE];


static void clearOpenReplies()
{
    int i;
    for (i = 0; i < OPEN_REPLIES_BITMAP_SIZE; ++i)
    {
        openRepliesBitmap[i] = 0;
    }
}


static int getReplyHandle(redisReply* reply)
{
    // TODO: More efficient implementation.
    
    int i;

    for (i = 0; i < MAX_OPEN_REPLIES; ++i)
    {
        if (0 == (openRepliesBitmap[i / sizeof(unsigned int)] & (1 << (i % sizeof(unsigned int)))))
        {
            openRepliesBitmap[i / sizeof(unsigned int)] |= 1 << (i % sizeof(unsigned int));
            openReplies[i] = reply;
            return i + 2;
        }
    }

    assert(0);
    return 0; // Failure.
}


void releaseReplyHandle(int handle)
{
    if (handle == 1)
    {
        return;
    }

    handle -= 2;
    assert(handle >= 0 && handle < MAX_OPEN_REPLIES);

    freeReplyObject(openReplies[handle]);
    openRepliesBitmap[handle / sizeof(unsigned int)] &= ~(1 << (handle % sizeof(unsigned int)));
}


// ---- Command formatting:
#define ARGC_MAX 16

struct command_format
{
    const char* cmd;
    int argc;
    int arg_types[ARGC_MAX];
    int reply_type_count;
    int reply_types[2];
};

enum {
    REDIS_CMD_HGET,
    REDIS_CMD_HKEYS,
    REDIS_CMD_HSET_INT,
    REDIS_CMD_INCR,
    REDIS_CMD_LINDEX,
    REDIS_CMD_LSET_INT,
    REDIS_CMD_SET,
    REDIS_CMD_RPUSH_INT,
    REDIS_CMD_HSET_STR,
};

enum {
    ARG_STR,
    ARG_INT,
    ARG_STRS,
    ARG_INTS,
};

static struct command_format commandFormats[] = {
    /* HGET      */ { "HGET",   2, { ARG_STR, ARG_STR }, 2, { REDIS_REPLY_STRING, REDIS_REPLY_NIL } },
    /* HKEYS     */ { "HKEYS",  1, { ARG_STR },          2, { REDIS_REPLY_ARRAY, REDIS_REPLY_NIL } },
    /* HSET_INT  */ { "HSET",   3, { ARG_STR, ARG_STR, ARG_INT }, 1, { REDIS_REPLY_INTEGER } },
    /* INCR      */ { "INCR",   1, { ARG_STR },          1, { REDIS_REPLY_INTEGER } },
    /* LINDEX    */ { "LINDEX", 2, { ARG_STR, ARG_INT }, 2, { REDIS_REPLY_STRING, REDIS_REPLY_NIL } },
    /* LSET_INT  */ { "LSET",   3, { ARG_STR, ARG_INT, ARG_INT }, 1, { REDIS_REPLY_STATUS } },
    /* SET       */ { "SET",    2, { ARG_STR, ARG_STR }, 1, { REDIS_REPLY_STATUS } },
    /* RPUSH_INT */ { "RPUSH",  2, { ARG_STR, ARG_INTS }, 1, { REDIS_REPLY_INTEGER } },
    /* HSET_STR */ { "HSET",  3, { ARG_STR, ARG_STR, ARG_STR }, 1, { REDIS_REPLY_INTEGER } },
};


// ---- Number conversion buffers:
#define NUM_CONV_BUF_LEN 32
#define NUM_CONV_BUF_COUNT 16
static char num_conv_bufs[NUM_CONV_BUF_LEN][NUM_CONV_BUF_COUNT];


// ---- Util functions:
static char* redifs_lltoa(long long ll, char* a, int len)
{
    int sign = ll < 0;
    ll = sign ? -ll : ll;

    a += len;

    *(--a) = '\0';

    if (ll == 0)
    {
        *(--a) = '0';
        return a;
    }

    do
    {
        *(--a) = '0' + (ll % 10);
        ll /= 10;
    } while (ll != 0);

    if (sign)
    {
        *(--a) = '-';
    }

    return a;
}


static int handleStringReply(redisReply* reply, char** result)
{
    switch (reply->type)
    {
        case REDIS_REPLY_STRING:
            *result = reply->str;
            return getReplyHandle(reply); // Success.

        case REDIS_REPLY_NIL:
            *result = NULL;
            return 1; // Success.

        default:
            assert(0);
            return 0; // Failure.
    }
}


static int handleStringArrayReply(redisReply* reply, int* result)
{
    switch (reply->type)
    {
        case REDIS_REPLY_ARRAY:
            *result = reply->elements;
            return getReplyHandle(reply); // Success.

        case REDIS_REPLY_NIL:
            *result = 0;
            return 1; // Success.

        default:
            assert(0);
            return 0; // Failure.
    }
}


void retrieveStringArrayElements(int handle, int offset, int count, char* array[])
{
    redisReply* reply;
    redisReply* strReply;
    int i;

    handle -= 2;
    assert(handle >= 0 && handle < MAX_OPEN_REPLIES);
    assert(openRepliesBitmap[handle / sizeof(unsigned int)] & (1 << (handle % sizeof(unsigned int))));

    reply = openReplies[handle];

    for (i = 0; i < count; ++i)
    {
        strReply = reply->element[i + offset];
        assert(strReply->type == REDIS_REPLY_STRING);
        array[i] = strReply->str;
    }
}


// ---- Interface functions:

// Reconnect to Redis server:
static int connectToRedisServer()
{
    // Connect to Redis server:
    redis1 = redisConnect(redis1_info.host, redis1_info.port);
    if (redis1->err)
    {
        fprintf(stderr, "Error: %s\n", redis1->errstr);
        closeRedisConnection();
        return -1; // Failure.
    }

    return 0; // Success.
}


// Connect to Redis server:
int openRedisConnection(const char* host, redifs_port_t port)
{
    int result;

    // Apply default settings if needed:
    redis1_info.host = host && host[0] != '\0' ? host : DEFAULT_HOST;
    redis1_info.port = port ? port : DEFAULT_PORT;

    // Actually open the connection:
    result = connectToRedisServer();

    // Clear open replies:
    clearOpenReplies();

    return result;
}


// Close Redis connection:
void closeRedisConnection()
{
    redisFree(redis1);
    redis1 = NULL;
}


// Execute a Redis command:
redisReply* execRedisCommand(int cmd, const char* strArgs[], long long intArgs[])
{
    redisReply* reply;
    struct command_format* commandFormat;
    const char* argv[ARGC_MAX+1];
    const char** strArg_ptr = strArgs;
    long long* intArg_ptr = intArgs;
    int numConvBufIndex = 0;
    int replyTypeOk;
    int retries;
    long long numArgs;
    long long j;
    int i;
    int argIndex;

    commandFormat = &commandFormats[cmd];

    // Check for Redis server connection:
    if (!redis1)
    {
        if (0 > connectToRedisServer())
        {
            fprintf(stderr, "Error: No connection to the Redis server.\n");
            return NULL;
        }
    }

    // Fill arguments:
    argv[0] = commandFormat->cmd;
    argIndex = 1;
    for (i = 0; i < commandFormat->argc; ++i)
    {
        switch (commandFormat->arg_types[i])
        {
            case ARG_STR:
                argv[argIndex++] = *(strArg_ptr++);
                break;

            case ARG_INT:
                argv[argIndex++] = redifs_lltoa(*(intArg_ptr++), num_conv_bufs[numConvBufIndex++], NUM_CONV_BUF_LEN);
                break;

            case ARG_STRS:
                numArgs = *(intArg_ptr++);
                for (j = 0; j < numArgs; ++j)
                {
                    argv[argIndex++] = *(strArg_ptr++);
                }
                break;

            case ARG_INTS:
                numArgs = *(intArg_ptr++);
                for (j = 0; j < numArgs; ++j)
                {
                    argv[argIndex++] = redifs_lltoa(*(intArg_ptr++), num_conv_bufs[numConvBufIndex++], NUM_CONV_BUF_LEN);
                }
                break;
        }
    }

    // Perform Redis command:
    retries = 2;
    while (1)
    {
        fprintf(stderr, "USED REDIS CALL: %s with args ", commandFormat->cmd);
        int i = 1;
        for(; i<argIndex; i++){
            fprintf(stderr, "%s ", argv[i]);
        }
        fprintf(stderr, "\n");

        reply = redisCommandArgv(redis1, argIndex, argv, NULL);
        if (!reply)
        {
            // Try to reconnect:
            if (redis1->err == REDIS_ERR_EOF && retries > 1)
            {
                closeRedisConnection();
                if (--retries > 0)
                {
                    fprintf(stderr, "Connection to Redis server lost. Trying to reconnect...\n");
                    if (0 == connectToRedisServer())
                    {
                        continue;
                    }
                    else
                    {
                        return NULL;
                    }
                }
            }

            fprintf(stderr, "XY Error: %s %d\n", redis1->errstr, redis1->err);
            return NULL; // Failure.
        }
        else if (reply->type == REDIS_REPLY_ERROR)
        {
            fprintf(stderr, "Error: %s\n", redis1->errstr);
            return NULL; // Failure.
        }

        break;
    }


    // Check reply type:
    replyTypeOk = 0;
    for (i = 0; i < commandFormat->reply_type_count; ++i)
    {
        if (commandFormat->reply_types[i] == reply->type)
        {
            replyTypeOk = 1;
            break;
        }
    }
    if (!replyTypeOk)
    {
        fprintf(stderr, "Error: Unexpected Redis reply type.\n");
        assert(0);
        return NULL;
    }

    // TODO: Retry once if connection is lost.

    return reply; // Success.
}


// ---- Redis command implementations:

// Redis HGET command:
int redisCommand_HGET(const char* key, const char* field, char** result)
{
    redisReply* reply;
    const char* args[] = { key, field };

    reply = execRedisCommand(REDIS_CMD_HGET, args, NULL);
    if (!reply)
    {
        return 0; // Failure.
    }

    return handleStringReply(reply, result);
}


// Redis HKEYS command:
int redisCommand_HKEYS(const char* key, int* result)
{
    redisReply* reply;
    const char* args[] = { key };

    reply = execRedisCommand(REDIS_CMD_HKEYS, args, NULL);
    if (!reply)
    {
        return 0; // Failure.
    }

    return handleStringArrayReply(reply, result);
}


// Redis HSET command with integer value:
extern int redisCommand_HSET_INT(const char* key, const char* field, long long value, int* result)
{
    redisReply* reply;
    const char* strArgs[] = { key, field };
    long long intArgs[] = { value };

    reply = execRedisCommand(REDIS_CMD_HSET_INT, strArgs, intArgs);
    if (!reply)
    {
        return 0; // Failure.
    }

    if (result)
    {
        *result = reply->integer;
    }

    freeReplyObject(reply);

    return 1; // Success.
}

int redisCommand_HSET_STR(const char* key, const char* field, char* value, int* result)
{
    redisReply* reply;
    const char* strArgs[] = { key, field, value };
    //long long intArgs[] = { field };

    reply = execRedisCommand(REDIS_CMD_HSET_STR, strArgs, NULL);
    if (!reply)
    {
        return 0; // Failure.
    }

    if (result)
    {
        *result = reply->integer;
    }

    freeReplyObject(reply);

    return 1; // Success.
}



// Redis INCR command:
int redisCommand_INCR(const char* key, long long* result)
{
    redisReply* reply;
    const char* args[] = { key };

    reply = execRedisCommand(REDIS_CMD_INCR, args, NULL);
    if (!reply)
    {
        return 0; // Failure.
    }

    *result = reply->integer;

    freeReplyObject(reply);

    return 1; // Success.
}


// Redis LINDEX command:
int redisCommand_LINDEX(const char* key, long long index, char** result)
{
    redisReply* reply;
    const char* strArgs[] = { key };
    long long intArgs[] = { index };

    reply = execRedisCommand(REDIS_CMD_LINDEX, strArgs, intArgs);
    if (!reply)
    {
        return 0; // Failure.
    }

    return handleStringReply(reply, result);
}


// Redis LSET command with integer value:
int redisCommand_LSET_INT(const char* key, long long index, long long value)
{
    redisReply* reply;
    const char* strArgs[] = { key };
    long long intArgs[] = { index, value };

    reply = execRedisCommand(REDIS_CMD_LSET_INT, strArgs, intArgs);
    if (!reply)
    {
        return 0; // Failure.
    }
    else if (0 != strcmp(reply->str, "OK"))
    {
        return 0; // Failure.
    }

    freeReplyObject(reply);

    return 1; // Success.
}


// Redis SET command:
int redisCommand_SET(const char* key, const char* value)
{
    redisReply* reply;
    const char* args[] = { key, value };

    reply = execRedisCommand(REDIS_CMD_SET, args, NULL);
    if (!reply)
    {
        return 0; // Failure.
    }
    else if (0 != strcmp(reply->str, "OK"))
    {
        return 0; // Failure.
    }

    freeReplyObject(reply);

    return 1; // Success.
}


// Redis RPUSH command with integer value:
int redisCommand_RPUSH_INT(const char* key, long long values[], long long value_count, int* result)
{
    redisReply* reply;
    const char* strArgs[] = { key };
    long long intArgs[value_count + 1];
    long long i;

    intArgs[0] = value_count;
    for (i = 0; i < value_count; ++i)
    {
        intArgs[i + 1] = values[i];
    }

    reply = execRedisCommand(REDIS_CMD_RPUSH_INT, strArgs, intArgs);
    if (!reply)
    {
        return 0; // Failure.
    }

    if (result)
    {
        *result = reply->integer;
    }

    freeReplyObject(reply);

    return 1; // Success.
}


