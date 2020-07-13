#pragma once

#ifndef __REDIS_CONTEXT_H__
#define __REDIS_CONTEXT_H__

#if _MSC_VER >= 1920
#   include <optional>
#else
#   include <memory>
#endif

#ifdef TC_REDIS
namespace TC_REDIS {
#endif

#if _MSC_VER >= 1920
#   define redis_optional std::optional
#   define redis_nullopt std::nullopt
#   define redis_make_optional(v) std::make_optional(v)
#else
#   define redis_optional std::unique_ptr
#   define redis_nullopt nullptr
#   define redis_make_optional(v) std::make_unique<decltype(v)>(v)
#endif


class redis_context
{
protected:
	redisContext* context;

    static const char* get_cmd(const char* func_name) {
        return strrchr(func_name, ':') + 1;
    }

	////////////////////////////////////////////////////////////////////////////////////////////
    class redis_key
    {
    protected:
        friend class redis_context;
        redisContext* context;

        redis_key(redisContext* _context) :context(_context) {
        }
    public:
        int64_t DEL(const std::vector<std::string>& keys) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), keys);
        }

        redis_optional<std::string> DUMP(const std::string& key)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key);

            return _reply.is_nil() ? redis_nullopt : 
                redis_make_optional((std::string)_reply);
        }

        bool EXISTS(const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key) != 0;
        }

        bool EXPIRE(const std::string& key, int seconds) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, seconds) != 0;
        }

        bool EXPIREAT(const std::string& key, int timestamp) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, timestamp) != 0;
        }

        std::vector<std::string> KEYS(const std::string& pattern) {
            return (std::vector<std::string>)redis_reply(context, get_cmd(__FUNCTION__), pattern);
        }

        bool MIGRATE(const std::string& host, int port, const std::string& key, int destination_db, int timeout,
            bool copy = false, bool replace = false)
        {
            std::vector<std::string> argv = {
                host,
                std::to_string(port),
                key,
                std::to_string(destination_db),
                std::to_string(timeout)
            };
            if (copy) {
                argv.push_back("COPY");
            }
            if (replace) {
                argv.push_back("REPLACE");
            }
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), argv);
            return _reply.is_ok();
        }

        bool MOVE(const std::string& key, int db) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, db) != 0;
        }

        enum { REFCOUNT };
        int64_t OBJECT(decltype(REFCOUNT) /*REFCOUNT*/, const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), "REFCOUNT", key);
        }
        enum { ENCODING };
        std::string OBJECT(decltype(ENCODING) /*ENCODING*/, const std::string& key) {
            return (std::string)redis_reply(context, get_cmd(__FUNCTION__), "ENCODING", key);
        }
        enum { IDLETIME };
        int64_t OBJECT(decltype(IDLETIME) /*IDLETIME*/, const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), "IDLETIME", key);
        }

        bool PERSIST(const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key) != 0;
        }

        bool PEXPIRE(const std::string& key, int64_t milliseconds) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, milliseconds) != 0;
        }

        bool PEXPIREAT(const std::string& key, int64_t milliseconds_timestamp) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, milliseconds_timestamp) != 0;
        }

        int64_t PTTL(const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key);
        }

        redis_optional<std::string> RANDOMKEY()
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__));

            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::string)_reply);
        }

        bool RENAME(const std::string& key, const std::string& newkey) {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, newkey);
            return _reply.is_ok();
        }

        bool RENAMENX(const std::string& key, const std::string& newkey) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, newkey) != 0;
        }

        bool RESTORE(const std::string& key, int64_t ttl, const std::string& serialized_value) 
        {
            std::vector<std::string> argv = {
                key,
                std::to_string(ttl),
                serialized_value
            };

            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), argv);
            return _reply.is_ok();
        }

        std::vector<std::string> SORT(const std::string& key,
            const std::string& by_pattern = "",
            int limit_offset = 0, unsigned int limit_count = -1,
            const std::vector<std::string>& get_pattern = {},
            bool desc = false, bool alpha = false)
        {
            std::vector<std::string> argv = { key };
            if (!by_pattern.empty()) {
                argv.push_back("BY");
                argv.push_back(by_pattern);
            }
            if (!(limit_offset == 0 && limit_count == -1)) {
                argv.push_back("LIMIT");
                argv.push_back(std::to_string(limit_offset));
                argv.push_back(std::to_string(limit_count));
            }
            for (auto& p : get_pattern) {
                argv.push_back("GET");
                argv.push_back(p);
            }
            if (desc) {
                argv.push_back("DESC");
            }
            if (alpha) {
                argv.push_back("ALPHA");
            }
            return (std::vector<std::string>)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        int64_t SORT(const std::string& key, const std::string& store,
            const std::string& by_pattern = "",
            int limit_offset = 0, unsigned int limit_count = -1,
            const std::vector<std::string>& get_pattern = {},
            bool desc = false, bool alpha = false)
        {
            std::vector<std::string> argv = { key };
            if (!by_pattern.empty()) {
                argv.push_back("BY");
                argv.push_back(by_pattern);
            }
            if (!(limit_offset == 0 && limit_count == -1)) {
                argv.push_back("LIMIT");
                argv.push_back(std::to_string(limit_offset));
                argv.push_back(std::to_string(limit_count));
            }
            for (auto& p : get_pattern) {
                argv.push_back("GET");
                argv.push_back(p);
            }
            if (desc) {
                argv.push_back("DESC");
            }
            if (alpha) {
                argv.push_back("ALPHA");
            }
            argv.push_back("STORE");
            argv.push_back(store);
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        int32_t TTL(const std::string& key) {
            return (int32_t)(int64_t)redis_reply(context, get_cmd(__FUNCTION__), key);
        }

        std::string TYPE(const std::string& key) {
            return (std::string)redis_reply(context, get_cmd(__FUNCTION__), key);
        }

        void SCAN(const std::string& match = "*", int count = 10,
            std::function<bool(const std::string&)> cb = [](const std::string& /*key*/) { return true; })
        {
            int64_t _pos = 0;
            std::set<std::string> _keys;
            do
            {
                auto _pair = (std::pair<tc_redis::redis_value, std::vector<std::string>>)
                    redis_reply(context, get_cmd(__FUNCTION__), _pos, "MATCH", match, "COUNT", count);

                _pos = _pair.first.as_int();

                for (auto& _key : _pair.second) {
                    if (_keys.insert(_key).second) {
                        if (!cb(_key)) {
                            break;
                        }
                    }
                }

            } while (_pos);

        }
    };
    ////////////////////////////////////////////////////////////////////////////////////////////
    class redis_string 
    {
    protected:
        friend class redis_context;
        redisContext* context;

        redis_string(redisContext* _context) :context(_context) {
        }
    public:
        int64_t APPEND(const std::string& key, const std::string& value) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, value);
        }

        int64_t BITCOUNT(const std::string& key, int start = 0, int end = -1) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, start, end);
        }

        enum { AND };
        int64_t BITOP(decltype(AND) /*AND*/, const std::string& destkey, const std::vector<std::string>& keys)
        {
            std::vector<std::string> argv = { "AND" , destkey };
            argv.insert(argv.end(), keys.begin(), keys.end());
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }
        enum { OR };
        int64_t BITOP(decltype(OR) /*OR*/, const std::string& destkey, const std::vector<std::string>& keys)
        {
            std::vector<std::string> argv = { "OR" , destkey };
            argv.insert(argv.end(), keys.begin(), keys.end());
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }
        enum { NOT };
        int64_t BITOP(decltype(NOT) /*NOT*/, const std::string& destkey, const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), "NOT", destkey, key);
        }
        enum { XOR };
        int64_t BITOP(decltype(XOR) /*XOR*/, const std::string& destkey, const std::vector<std::string>& keys)
        {
            std::vector<std::string> argv = { "XOR" , destkey };
            argv.insert(argv.end(), keys.begin(), keys.end());
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }



        int64_t DECR(const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key);
        }

        int64_t DECRBY(const std::string& key, int64_t decrement) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, decrement);
        }

        redis_optional<std::string> GET(const std::string& key)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key);

            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::string)_reply);
        }

        bool GETBIT(const std::string& key, int offset) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, offset) != 0;
        }

        std::string GETRANGE(const std::string& key, int start = 0, int end = -1) {
            return (std::string)redis_reply(context, get_cmd(__FUNCTION__), key, start, end);
        }

        std::string GETSET(const std::string& key, const std::string& value) {
            return (std::string)redis_reply(context, get_cmd(__FUNCTION__), key, value);
        }

        int64_t INCR(const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key);
        }

        int64_t INCRBY(const std::string& key, int64_t increment) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, increment);
        }

        double INCRBYFLOAT(const std::string& key, double increment) {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, increment);
            return ((redis_value)_reply).as_float();
        }

        std::vector<redis_optional<std::string>> MGET(const std::vector<std::string>& keys) {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), keys);
            std::vector<redis_optional<std::string>> _v;
            for (auto& _r : (std::vector<redis_reply>)_reply) {
                _v.emplace_back(
                    _r.is_nil() ? redis_nullopt :
                    redis_make_optional((std::string)_r));
            }
            return _v;
        }

        bool MSET(const std::map<std::string, std::string>& key_value_pairs) 
        {
            std::vector<std::string> argv;
            for (auto& p : key_value_pairs) {
                argv.push_back(p.first);
                argv.push_back(p.second);
            }
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), argv);
            return _reply.is_ok();
        }

        bool MSETNX(const std::map<std::string, std::string>& key_value_pairs)
        {
            std::vector<std::string> argv;
            for (auto& p : key_value_pairs) {
                argv.push_back(p.first);
                argv.push_back(p.second);
            }
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv) != 0;
        }

        bool PSETEX(const std::string& key, int64_t milliseconds, const std::string& value) {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, milliseconds, value);
            return _reply.is_ok();
        }

        bool SET(const std::string& key, const std::string& value, int seconds = -1, bool nx = false, bool xx = false)
        {
            std::vector<std::string> argv = { key, value };
            if (seconds != -1) {
                argv.push_back("EX");
                argv.push_back(std::to_string(seconds));
            }
            redis_test(!(nx && xx));
            if (nx) {
                argv.push_back("NX");
            }
            if (xx) {
                argv.push_back("XX");
            }
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), argv);
            return _reply.is_ok();
        }

        bool SET(const std::string& key, const std::string& value, int64_t milliseconds /*= -1*/, bool nx = false, bool xx = false)
        {
            std::vector<std::string> argv = { key, value };
            if (milliseconds != -1) {
                argv.push_back("PX");
                argv.push_back(std::to_string(milliseconds));
            }
            redis_test(!(nx && xx));
            if (nx) {
                argv.push_back("NX");
            }
            if (xx) {
                argv.push_back("XX");
            }
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), argv);
            return _reply.is_ok();
        }

        bool SETBIT(const std::string& key, int offset, bool value) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, offset, value) != 0;
        }

        bool SETEX(const std::string& key, int seconds, const std::string& value) {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, seconds, value);
            return _reply.is_ok();
        }

        bool SETNX(const std::string& key, const std::string& value) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, value) != 0;
        }

        int64_t SETRANGE(const std::string& key, int offset, const std::string& value) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, offset, value);
        }

        int64_t STRLEN(const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key);
        }
    };
    ////////////////////////////////////////////////////////////////////////////////////////////
    class redis_hash
    {
    protected:
        friend class redis_context;
        redisContext* context;

        redis_hash(redisContext* _context) :context(_context) {
        }
    public:
        int64_t HDEL(const std::string& key, const std::vector<std::string>& fields)
        {
            std::vector<std::string> argv = { key };
            argv.insert(argv.end(), fields.begin(), fields.end());

            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        bool HEXISTS(const std::string& key, const std::string& field) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, field) != 0;
        }
            
        redis_optional<std::string> HGET(const std::string& key, const std::string& field)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, field);

            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::string)_reply);
        }

        std::map<std::string, std::string> HGETALL(const std::string& key) {
            return (std::map<std::string, std::string>)redis_reply(context, get_cmd(__FUNCTION__), key);
        }
            
        int64_t HINCRBY(const std::string& key, const std::string& field, int64_t increment) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, field, increment);
        }

        double HINCRBYFLOAT(const std::string& key, const std::string& field, double increment) {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, field, increment);
            return ((redis_value)_reply).as_float();
        }

        std::vector<std::string> HKEYS(const std::string& key) {
            return (std::vector<std::string>)redis_reply(context, get_cmd(__FUNCTION__), key);
        }

        int64_t HLEN(const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key);
        }

        std::vector<redis_optional<std::string>> HMGET(const std::string& key, const std::vector<std::string>& fields)
        {
            std::vector<std::string> argv = { key };
            argv.insert(argv.end(), fields.begin(), fields.end());
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), argv);
            std::vector<redis_optional<std::string>> _v;
            for (auto& _r : (std::vector<redis_reply>)_reply) {
                _v.emplace_back(
                    _r.is_nil() ? redis_nullopt :
                    redis_make_optional((std::string)_r));
            }
            return _v;
        }

        bool HMSET(const std::string& key, const std::map<std::string, std::string>& field_value_pairs) 
        {
            std::vector<std::string> argv = { key };
            for (auto& p : field_value_pairs) {
                argv.push_back(p.first);
                argv.push_back(p.second);
            }
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), argv);
            return _reply.is_ok();
        }

        bool HSET(const std::string& key, const std::string& field, const std::string& value) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, field, value) != 0;
        }

        bool HSETNX(const std::string& key, const std::string& field, const std::string& value) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, field, value) != 0;
        }
            
        std::vector<std::string> HVALS(const std::string& key) {
            return (std::vector<std::string>)redis_reply(context, get_cmd(__FUNCTION__), key);
        }
            
        void HSCAN(const std::string& key, const std::string& match = "*", int count = 10,
            std::function<bool(const std::string&, const std::string&)> cb =
            [](const std::string& /*field*/, const std::string& /*value*/) { return true; })
        {
            int64_t _pos = 0;
            std::set<std::string> _fields;
            do
            {
                auto _pair = (std::pair<tc_redis::redis_value, std::map<std::string, std::string>>)
                    redis_reply(context, get_cmd(__FUNCTION__), key, _pos, "MATCH", match, "COUNT", count);

                _pos = _pair.first.as_int();

                for (auto& _field_value_pair : _pair.second) {
                    if (_fields.insert(_field_value_pair.first).second) {
                        if (!cb(_field_value_pair.first, _field_value_pair.second)) {
                            break;
                        }
                    }
                }

            } while (_pos);

        }

    };
    ////////////////////////////////////////////////////////////////////////////////////////////
    class redis_list
    {
    protected:
        friend class redis_context;
        redisContext* context;

        redis_list(redisContext* _context) :context(_context) {
        }
    public:
        redis_optional<std::pair<std::string, std::string>> BLPOP(const std::vector<std::string>& keys, int timeout) 
        {
            std::vector<std::string> argv = keys;
            argv.push_back(std::to_string(timeout));

            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), argv);
            
            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::pair<std::string, std::string>)_reply);
        }

        redis_optional<std::pair<std::string, std::string>> BRPOP(const std::vector<std::string>& keys, int timeout)
        {
            std::vector<std::string> argv = keys;
            argv.push_back(std::to_string(timeout));

            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), argv);

            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::pair<std::string, std::string>)_reply);
        }

        std::pair<redis_optional<std::string>, std::string> BRPOPLPUSH(const std::string& source, const std::string& destination, int timeout)
        {
            auto p = (std::vector<redis_reply>)redis_reply(context, get_cmd(__FUNCTION__), source, destination, timeout);

            return std::make_pair(
                (p[0].is_nil() ? redis_nullopt : redis_make_optional((std::string)p[0])),
                (std::string)p[1]);
        }

        redis_optional<std::string> LINDEX(const std::string& key, int index)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, index);

            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::string)_reply);
        }

        int64_t LINSERT(const std::string& key, const std::string& pivot, const std::string& value, bool after = false) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, (after ? "AFTER" : "BEFORE"), pivot, value);
        }

        int64_t LLEN(const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key);
        }

        redis_optional<std::string> LPOP(const std::string& key)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key);

            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::string)_reply);
        }

        int64_t LPUSH(const std::string& key, const std::vector<std::string>& values)
        {
            std::vector<std::string> argv = { key };
            argv.insert(argv.end(), values.begin(), values.end());

            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        int64_t LPUSHX(const std::string& key, const std::string& value) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, value);
        }

        std::vector<std::string> LRANGE(const std::string& key, int start, int stop) {
            return (std::vector<std::string>)redis_reply(context, get_cmd(__FUNCTION__), key, start, stop);
        }

        int64_t LREM(const std::string& key, int count, const std::string& value) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, count, value);
        }

        bool LSET(const std::string& key, int index, const std::string& value) {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, index, value);
            return _reply.is_ok();
        }

        bool LTRIM(const std::string& key, int start, int stop) {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, start, stop);
            return _reply.is_ok();
        }

        redis_optional<std::string> RPOP(const std::string& key)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key);

            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::string)_reply);
        }

        redis_optional<std::string> RPOPLPUSH(const std::string& source, const std::string& destination)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), source, destination);

            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::string)_reply);
        }

        int64_t RPUSH(const std::string& key, const std::vector<std::string>& values)
        {
            std::vector<std::string> argv = { key };
            argv.insert(argv.end(), values.begin(), values.end());

            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        int64_t RPUSHX(const std::string& key, const std::string& value) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, value);
        }
    };
    ////////////////////////////////////////////////////////////////////////////////////////////
    class redis_set
    {
    protected:
        friend class redis_context;
        redisContext* context;

        redis_set(redisContext* _context) :context(_context) {
        }
    public:

        int64_t SADD(const std::string& key, const std::vector<std::string>& members)
        {
            std::vector<std::string> argv = { key };
            argv.insert(argv.end(), members.begin(), members.end());

            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        int64_t SCARD(const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key);
        }

        std::set<std::string> SDIFF(const std::vector<std::string>& keys) {
            return (std::set<std::string>)redis_reply(context, get_cmd(__FUNCTION__), keys);
        }

        int64_t SDIFFSTORE(const std::string& destination ,const std::vector<std::string>& keys)
        {
            std::vector<std::string> argv = { destination };
            argv.insert(argv.end(), keys.begin(), keys.end());

            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        std::set<std::string> SINTER(const std::vector<std::string>& keys) {
            return (std::set<std::string>)redis_reply(context, get_cmd(__FUNCTION__), keys);
        }

        int64_t SINTERSTORE(const std::string& destination, const std::vector<std::string>& keys)
        {
            std::vector<std::string> argv = { destination };
            argv.insert(argv.end(), keys.begin(), keys.end());

            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        bool SISMEMBER(const std::string& key, const std::string& member) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, member) != 0;
        }

        std::set<std::string> SMEMBERS(const std::string& key) {
            return (std::set<std::string>)redis_reply(context, get_cmd(__FUNCTION__), key);
        }

        bool SMOVE(const std::string& source, const std::string& destination, const std::string& member) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), source, destination, member) != 0;
        }

        redis_optional<std::string> SPOP(const std::string& key)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key);

            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::string)_reply);
        }

        redis_optional<std::string> SRANDMEMBER(const std::string& key)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key);

            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::string)_reply);
        }
        std::vector<std::string> SRANDMEMBER(const std::string& key, int count) {
            return (std::vector<std::string>)redis_reply(context, get_cmd(__FUNCTION__), key, count);
        }

        int64_t SREM(const std::string& key, const std::vector<std::string>& members)
        {
            std::vector<std::string> argv = { key };
            argv.insert(argv.end(), members.begin(), members.end());

            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        std::set<std::string> SUNION(const std::vector<std::string>& keys) {
            return (std::set<std::string>)redis_reply(context, get_cmd(__FUNCTION__), keys);
        }

        int64_t SUNIONSTORE(const std::string& destination, const std::vector<std::string>& keys)
        {
            std::vector<std::string> argv = { destination };
            argv.insert(argv.end(), keys.begin(), keys.end());

            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        void SSCAN(const std::string& key, const std::string& match = "*", int count = 10,
            std::function<bool(const std::string& member)> cb = [](const std::string&) { return true; })
        {
            int64_t _pos = 0;
            std::set<std::string> _members;
            do
            {
                auto _pair = (std::pair<tc_redis::redis_value, std::vector<std::string>>)
                    redis_reply(context, get_cmd(__FUNCTION__), key, _pos, "MATCH", match, "COUNT", count);

                _pos = _pair.first.as_int();

                for (auto& _member : _pair.second) {
                    if (_members.insert(_member).second) {
                        if (!cb(_member)) {
                            break;
                        }
                    }
                }

            } while (_pos);

        }
    };
     
    ////////////////////////////////////////////////////////////////////////////////////////////
    class redis_sortedset
    {
    protected:
        friend class redis_context;
        redisContext* context;

        redis_sortedset(redisContext* _context) :context(_context) {
        }
    public:
        int64_t ZADD(const std::string& key, const std::map<std::string, std::string>& member_score_pairs)
        {
            std::vector<std::string> argv = { key };
            for (auto& p : member_score_pairs) {
                argv.push_back(p.second);
                argv.push_back(p.first);
            }
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        int64_t ZCARD(const std::string& key) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key);
        }

        int64_t ZCOUNT(const std::string& key, const std::string& min, const std::string& max) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, min, max);
        }

        std::string ZINCRBY(const std::string& key, const std::string& increment, const std::string& member) {
            return (std::string)redis_reply(context, get_cmd(__FUNCTION__), key, increment, member);
        }
     
        std::map<std::string, std::string> ZRANGE(const std::string& key, int start, int stop, bool with_scores = false)
        {
            if (with_scores) {
                return (std::map<std::string, std::string>)redis_reply(context, get_cmd(__FUNCTION__), key, start, stop, "WITHSCORES");
            }

            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, start, stop);

            std::map<std::string, std::string> _m;
            for (auto& _member : (std::set<std::string>)_reply) {
                _m[_member] = "0";
            }
            return _m;
        }

        std::map<std::string, std::string> ZRANGEBYSCORE(const std::string& key,
            const std::string& min, const std::string& max, bool with_scores = false,
            int limit_offset = 0, unsigned int limit_count = -1)
        {
            if (with_scores) {
                if (!(limit_offset == 0 && limit_count == -1)) {
                    return (std::map<std::string, std::string>)redis_reply(context, get_cmd(__FUNCTION__), key,
                        min, max, "WITHSCORES", "LIMIT", limit_offset, limit_count);
                }
                else {
                    return (std::map<std::string, std::string>)redis_reply(context, get_cmd(__FUNCTION__), key,
                        min, max, "WITHSCORES");
                }
            }

            redis_reply _reply = !(limit_offset == 0 && limit_count == -1) ?
                redis_reply(context, get_cmd(__FUNCTION__), key, min, max, "LIMIT", limit_offset, limit_count) :
                redis_reply(context, get_cmd(__FUNCTION__), key, min, max);

            std::map<std::string, std::string> _m;
            for (auto& _member : (std::set<std::string>)_reply) {
                _m[_member] = "0";
            }
            return _m;
        }

        int64_t ZRANK(const std::string& key, const std::string& member)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, member);

            return _reply.is_nil() ? -1 : (int64_t)_reply;
        }

        int64_t ZREM(const std::string& key, const std::vector<std::string>& members)
        {
            std::vector<std::string> argv = { key };
            argv.insert(argv.end(), members.begin(), members.end());

            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        int64_t ZREMRANGEBYRANK(const std::string& key, int start, int stop) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, start, stop);
        }

        int64_t ZREMRANGEBYSCORE(const std::string& key, const std::string& min, const std::string& max) {
            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), key, min, max);
        }

        std::map<std::string, std::string> ZREVRANGE(const std::string& key, int start, int stop, bool with_scores = false)
        {
            if (with_scores) {
                return (std::map<std::string, std::string>)redis_reply(context, get_cmd(__FUNCTION__), key, start, stop, "WITHSCORES");
            }

            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, start, stop);

            std::map<std::string, std::string> _m;
            for (auto& _member : (std::set<std::string>)_reply) {
                _m[_member] = "0";
            }
            return _m;
        }

        std::map<std::string, std::string> ZREVRANGEBYSCORE(const std::string& key,
            const std::string& min, const std::string& max, bool with_scores = false,
            int limit_offset = 0, unsigned int limit_count = -1)
        {
            if (with_scores) {
                if (!(limit_offset == 0 && limit_count == -1)) {
                    return (std::map<std::string, std::string>)redis_reply(context, get_cmd(__FUNCTION__), key,
                        min, max, "WITHSCORES", "LIMIT", limit_offset, limit_count);
                }
                else {
                    return (std::map<std::string, std::string>)redis_reply(context, get_cmd(__FUNCTION__), key,
                        min, max, "WITHSCORES");
                }
            }

            redis_reply _reply = !(limit_offset == 0 && limit_count == -1) ?
                redis_reply(context, get_cmd(__FUNCTION__), key, min, max, "LIMIT", limit_offset, limit_count) :
                redis_reply(context, get_cmd(__FUNCTION__), key, min, max);

            std::map<std::string, std::string> _m;
            for (auto& _member : (std::set<std::string>)_reply) {
                _m[_member] = "0";
            }
            return _m;
        }

        int64_t ZREVRANK(const std::string& key, const std::string& member)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, member);

            return _reply.is_nil() ? -1 : (int64_t)_reply;
        }

        redis_optional<std::string> ZSCORE(const std::string& key, const std::string& member)
        {
            redis_reply _reply = redis_reply(context, get_cmd(__FUNCTION__), key, member);

            return _reply.is_nil() ? redis_nullopt :
                redis_make_optional((std::string)_reply);
        }
        
        enum {
            SUM,
            MIN,
            MAX
        };
        int64_t ZUNIONSTORE(const std::string& destination, const std::vector<std::string>& keys,
            const std::vector<std::string>& weights = {}, decltype(SUM) aggregate = SUM)
        {
            std::vector<std::string> argv = { destination };

            argv.push_back(std::to_string(keys.size()));
            argv.insert(argv.end(), keys.begin(), keys.end());

            auto _w = weights;
            while (_w.size() < keys.size()) {
                _w.push_back("1");
            }
            _w.resize(keys.size());
            argv.insert(argv.end(), _w.begin(), _w.end());

            argv.push_back("AGGREGATE");
            switch (aggregate)
            {
            case SUM:
                argv.push_back("SUM");
                break;
            case MIN:
                argv.push_back("MIN");
                break;
            case MAX:
                argv.push_back("MAX");
                break;
            }

            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }

        int64_t ZINTERSTORE(const std::string& destination, const std::vector<std::string>& keys,
            const std::vector<std::string>& weights = {}, decltype(SUM) aggregate = SUM)
        {
            std::vector<std::string> argv = { destination };

            argv.push_back(std::to_string(keys.size()));
            argv.insert(argv.end(), keys.begin(), keys.end());

            auto _w = weights;
            while (_w.size() < keys.size()) {
                _w.push_back("1");
            }
            _w.resize(keys.size());
            argv.insert(argv.end(), _w.begin(), _w.end());

            argv.push_back("AGGREGATE");
            switch (aggregate)
            {
            case SUM:
                argv.push_back("SUM");
                break;
            case MIN:
                argv.push_back("MIN");
                break;
            case MAX:
                argv.push_back("MAX");
                break;
            }

            return (int64_t)redis_reply(context, get_cmd(__FUNCTION__), argv);
        }


        void ZSCAN(const std::string& key, const std::string& match = "*", int count = 10,
            std::function<bool(const std::string&, const std::string&)> cb =
            [](const std::string&, const std::string& /*member*/) { return true; })
        {
            int64_t _pos = 0;
            std::set<std::string> _fields;
            do
            {
                auto _pair = (std::pair<tc_redis::redis_value, std::map<std::string, std::string>>)
                    redis_reply(context, get_cmd(__FUNCTION__), key, _pos, "MATCH", match, "COUNT", count);

                _pos = _pair.first.as_int();

                for (auto& _member_score_pair : _pair.second) {
                    if (_fields.insert(_member_score_pair.first).second) {
                        if (!cb(_member_score_pair.first, _member_score_pair.second)) {
                            break;
                        }
                    }
                }

            } while (_pos);

        }
    };
public:
    redis_context(redisContext* _context) :context(_context) {
    }

    redis_key key() {
        return redis_key(context);
    }
    redis_string string() {
        return redis_string(context);
    }
    redis_hash hash() {
        return redis_hash(context);
    }
    redis_list list() {
        return redis_list(context);
    }
    redis_set set() {
        return redis_set(context);
    } 
    redis_sortedset sortedset() {
        return redis_sortedset(context);
    }
};


#ifdef TC_REDIS
}
#endif

#endif