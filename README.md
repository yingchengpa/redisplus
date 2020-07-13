
# redisplus
hiredis c++ api


# interface 

## key
- DEL
- DUMP
- EXISTS
- EXPIRE
- EXPIREAT
- KEYS
- MIGRATE
- MOVE
- OBJECT
- PERSIST
- PEXPIRE
- PEXPIREAT
- PTTL
- RANDOMKEY
- RENAME
- RENAMENX
- RESTORE
- SORT
- TTL
- TYPE
- SCAN

## string
- APPEND
- BITCOUNT
- BITOP
- DECR
- DECRBY
- GET
- GETBIT
- GETRANGE
- GETSET
- INCR
- INCRBY
- INCRBYFLOAT
- MGET
- MSET
- MSETNX
- PSETEX
- SET
- SETBIT
- SETEX
- SETNX
- SETRANGE
- STRLEN

## hash
- HDEL
- HEXISTS
- HGET
- HGETALL
- HINCRBY
- HINCRBYFLOAT
- HKEYS
- HLEN
- HMGET
- HMSET
- HSET
- HSETNX
- HVALS
- HSCAN

## list
- BLPOP
- BRPOP
- BRPOPLPUSH
- LINDEX
- LINSERT
- LLEN
- LPOP
- LPUSH
- LPUSHX
- LRANGE
- LREM
- LSET
- LTRIM
- RPOP
- RPOPLPUSH
- RPUSH
- RPUSHX

## set
- SADD
- SCARD
- SDIFF
- SDIFFSTORE
- SINTER
- SINTERSTORE
- SISMEMBER
- SMEMBERS
- SMOVE
- SPOP
- SRANDMEMBER
- SREM
- SUNION
- SUNIONSTORE
- SSCAN

## sortedset
- ZADD
- ZCARD
- ZCOUNT
- ZINCRBY
- ZRANGE
- ZRANGEBYSCORE
- ZRANK
- ZREM
- ZREMRANGEBYRANK
- ZREMRANGEBYSCORE
- ZREVRANGE
- ZREVRANGEBYSCORE
- ZREVRANK
- ZSCORE
- ZUNIONSTORE
- ZINTERSTORE
- ZSCAN

# how to use ?

for example  ,call "string":"get" func :
~~~
#include "redis_ex.h"

redisContext *pContext = nullptr;
std::string strKey = "hello";

//..... get the pContext ;

auto value = tc_redis::redis_context(pContext).string().GET(strKey);

printf("the key(%s) value is %s \n",strKey.c_str(),*value.c_str());
~~~
