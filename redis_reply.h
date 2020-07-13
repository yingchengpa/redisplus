#pragma once

#ifndef __REDIS_REPLY_H__
#define __REDIS_REPLY_H__

#ifdef TC_REDIS
namespace TC_REDIS {
#endif

//填充redis命令的参数类型
//整形一律使用(%lld)
//浮点一律使用(%f)
//字符串(%s)
class redis_reply_param_type {
public:
	template<typename T>
	typename std::enable_if<std::is_integral<typename std::decay<T>::type>::value 
		|| std::is_enum<typename std::decay<T>::type>::value, const char*>::type
		operator ()(T) { return "%lld"; }

	template<typename T>
	typename std::enable_if<std::is_floating_point<typename std::decay<T>::type>::value,
		const char*>::type
		operator ()(T) { return "%f"; }

	const char* operator ()(const char*) { return "%s"; }
	const char* operator ()(const std::string&) { return "%s"; }
};

//转换redis命令的参数类型
//整形一律转(int64_t)
//浮点一律转(double)
//字符串转(const char*)
class redis_reply_param_convert {
public:
	template<typename T>
	typename std::enable_if<std::is_integral<typename std::decay<T>::type>::value
		|| std::is_enum<typename std::decay<T>::type>::value, int64_t>::type
		operator ()(T v) { return v; }

	template<typename T>
	typename std::enable_if<std::is_floating_point<typename std::decay<T>::type>::value, 
		double>::type
		operator ()(T v) { return v; }

	const char* operator ()(const char* v) { return v; }
	const char* operator ()(const std::string& v) { return v.c_str(); }
};


//判断参数类型是否std::vector<std::string>
template<typename... ARGS>
class is_redis_command_argv {
public:
	enum { value = false };
};

template<>
class is_redis_command_argv<std::vector<std::string>> {
public:
	enum { value = true };
};

//使用参数传递构造redis命令
template<typename... ARGS, typename = std::enable_if<!is_redis_command_argv<typename std::decay<ARGS>::type...>::value>::type>
std::string redis_append_command(redisContext* _context, const std::string& _cmd, ARGS&&... _args)
{
	std::ostringstream _sout;
	_sout << _cmd;
	std::ostream* tmp[] = { &_sout, &(_sout << ' ' << redis_reply_param_type()(_args))... };
	(void)tmp;//for warning
	int ret = redisAppendCommand(_context, _sout.str().c_str(), redis_reply_param_convert()(_args)...);
	std::string cmd = strprintf(_sout.str(), redis_reply_param_convert()(_args)...);
	redis_test(ret == REDIS_OK, redis_error_code::command_error, cmd);
	return cmd;
}

//使用std::vector<std::string>构造redis命令
inline std::string redis_append_command(redisContext* _context, const std::string& _cmd, const std::vector<std::string>& argv)
{
	std::ostringstream _sout;
	std::vector<const char*> _argv;
	std::vector<size_t> _argvlen;
	_sout << _cmd;
	_argv.push_back(_cmd.c_str());
	_argvlen.push_back(_cmd.size());
	for (auto& _arg : argv) {
		_sout << ' ' << _arg;
		_argv.push_back(_arg.c_str());
		_argvlen.push_back(_arg.size());
	}
	int ret = redisAppendCommandArgv(_context, _argv.size(), _argv.data(), _argvlen.data());
	std::string cmd = _sout.str();
	redis_test(ret == REDIS_OK, redis_error_code::command_error, cmd);
	return cmd;
}

//redis值类型
//提供对std::string,int64_t,double的读写接口
class redis_value {
protected:
	std::string value;
public:
	template<typename T, typename = std::enable_if <
		!std::is_same<typename std::decay<T>::type, redis_reply>::value,
		std::result_of<redis_reply_param_convert(typename std::decay<T>::type)>::type
	>::type>
	explicit redis_value(const T& _v)
	{
		auto _v2 = redis_reply_param_convert()(_v);

		if (&typeid(int64_t) == &typeid(_v2)) {
			value = strprintf("%I64d", _v2);
		}
		else if (&typeid(double) == &typeid(_v2)){
			value = strprintf("%lf", _v2);
		}
		else if (&typeid(const char*) == &typeid(_v2)) {
			value = strprintf("%s", _v2);
		}
		else {
			redis_test(false);
		}
	}
	redis_value() {}
	redis_value(const redis_value& _v) { value = _v.value; }
	redis_value(redis_value&& _v) { std::swap(value, _v.value); }
	~redis_value() {}

	operator const std::string&()const { return value; }

	const std::string& as_string()const { return value; }
	int64_t as_int()const { return atoll(value.c_str()); }
	double as_float()const { return atof(value.c_str()); }

	template<typename T>
	redis_value& operator =(const T& _v) {
		return *this = redis_value(_v);
	}
	redis_value& operator =(const redis_value& _v) {
		value = _v.value;
		return *this;
	}
	redis_value& operator =(redis_value&& _v) {
		std::swap(value, _v.value);
		return *this;
	}
};

//redis回应类
//转换目标类型失败时抛出异常
class redis_reply
{
protected:
	redisReply* reply;
	std::string cmd;
	std::shared_ptr<redisReply> ref_reply;

	//校验是否是error
	void check_error()const
	{
		redis_test(reply != nullptr, redis_error_code::reply_is_null, cmd);

		if (reply->type == REDIS_REPLY_ERROR) {
			throw redis_error(redis_error_code::reply_is_error, (reply->str ? reply->str : ""), cmd);
		}
	}

	//转换vector的实现
	template<typename T>
	T convert_vector()const
	{
		check_error();
		//要求reply类型是数组
		redis_test(reply->type == REDIS_REPLY_ARRAY, redis_error_code::reply_type_incorrect, cmd);

		T _vector;
		for (size_t i = 0; i < reply->elements; i++) {
			redis_reply _redis_reply(reply->element[i], ref_reply);
			_vector.push_back((typename T::value_type)std::move(_redis_reply));
		}
		return _vector;
	}

	//转换set的实现
	template<typename T>
	T convert_set()const
	{
		check_error();
		//要求reply类型是数组
		redis_test(reply->type == REDIS_REPLY_ARRAY, redis_error_code::reply_type_incorrect, cmd);

		T _set;
		for (size_t i = 0; i < reply->elements; i++) {
			redis_reply _redis_reply(reply->element[i], ref_reply);
			_set.insert((typename T::key_type)std::move(_redis_reply));
		}
		return _set;
	}

	//转换map的实现
	template<typename T>
	T convert_map()const
	{
		check_error();
		//要求reply类型是数组,且元素个数是偶数
		redis_test(reply->type == REDIS_REPLY_ARRAY, redis_error_code::reply_type_incorrect, cmd);
		redis_test(reply->elements % 2 == 0, redis_error_code::reply_data_incorrect, cmd);

		T _map;
		for (size_t i = 0; i < reply->elements; i += 2)
		{
			typename T::key_type _field;
			{
				redis_reply _redis_reply(reply->element[i], ref_reply);
				_field = (typename T::key_type)std::move(_redis_reply);
			}
			{
				redis_reply _redis_reply(reply->element[i + 1], ref_reply);
				_map[_field] = (typename T::mapped_type)std::move(_redis_reply);
			}
		}
		return _map;
	}

	static void free_reply(redisReply* reply) {
		if (reply != nullptr) {
			freeReplyObject(reply);
		}
	}

	redis_reply(const redis_reply&) = delete;
	redis_reply& operator =(const redis_reply&) = delete;
public:
	redis_reply(redisReply* _reply, const std::string& _cmd = "") :
		reply(_reply), cmd(_cmd), ref_reply(reply, free_reply)
	{
	}
	redis_reply(redisReply* _reply, const std::shared_ptr<redisReply>& _ref_reply, const std::string& _cmd = "") :
		reply(_reply), cmd(_cmd), ref_reply(_ref_reply)
	{
	}
	redis_reply(redis_reply&& _reply) :
		reply(nullptr)
	{
		std::swap(reply, _reply.reply);
		std::swap(cmd, _reply.cmd);
		std::swap(ref_reply, _reply.ref_reply);
	}

	~redis_reply()
	{
	}

	redis_reply& operator =(redis_reply&& _reply)
	{
		std::swap(reply, _reply.reply);
		std::swap(cmd, _reply.cmd);
		std::swap(ref_reply, _reply.ref_reply);
		return *this;
	}

	const std::string& get_cmd() const { return cmd; }

	//通过参数传递方式redis命令构造
	template<typename... ARGS, typename = std::enable_if<!is_redis_command_argv<typename std::decay<ARGS>::type...>::value>::type>
	redis_reply(redisContext* _context, const std::string& _cmd, ARGS&&... _args) 
	{
		std::ostringstream _sout;
		_sout << _cmd;
		std::ostream* tmp[] = { &_sout, &(_sout << ' ' << redis_reply_param_type()(_args))... };
		(void)tmp;//for warning
		reply = (redisReply*)redisCommand(_context, _sout.str().c_str(), redis_reply_param_convert()(_args)...);
		ref_reply.reset(reply, free_reply);
		cmd = strprintf(_sout.str(), redis_reply_param_convert()(_args)...);
	}

	//通过std::vector<std::string>方式redis命令构造
	redis_reply(redisContext* _context, const std::string& _cmd, const std::vector<std::string>& argv)
	{
		std::ostringstream _sout;
		std::vector<const char*> _argv;
		std::vector<size_t> _argvlen;
		_sout << _cmd;
		_argv.push_back(_cmd.c_str());
		_argvlen.push_back(_cmd.size());
		for (auto& _arg : argv) {
			_sout << ' ' << _arg;
			_argv.push_back(_arg.c_str());
			_argvlen.push_back(_arg.size());
		}
		reply = (redisReply*)redisCommandArgv(_context, _argv.size(), _argv.data(), _argvlen.data());
		ref_reply.reset(reply, free_reply);
		cmd = _sout.str();
	}

	redisReply** operator &() { return &reply; }
	redisReply*& operator ->() { return reply; }

	bool is_nil()const
	{
		check_error();
		return reply->type == REDIS_REPLY_NIL;
	}
	bool is_ok()const
	{
		check_error();
		return (reply->type == REDIS_REPLY_STATUS && _stricmp(reply->str, "OK") == 0);
	}
	bool is_queued()const
	{
		check_error();
		return (reply->type == REDIS_REPLY_STATUS && _stricmp(reply->str, "QUEUED") == 0);
	}

	operator redisReply*()const { return reply; }
	explicit operator int64_t()const
	{
		check_error();
		redis_test(reply->type == REDIS_REPLY_INTEGER, redis_error_code::reply_type_incorrect, cmd);
		return reply->integer;
	}
	explicit operator std::string()const
	{
		check_error();
		redis_test(reply->type == REDIS_REPLY_STRING || reply->type == REDIS_REPLY_STATUS,
			redis_error_code::reply_type_incorrect, cmd);
		return std::string(reply->str, reply->len);
	}
	explicit operator redis_value()const
	{
		check_error();
		if (reply->type == REDIS_REPLY_INTEGER) {
			return redis_value(reply->integer);
		}
		if (reply->type == REDIS_REPLY_STRING) {
			return redis_value(reply->str);
		}
		redis_test(false, redis_error_code::reply_type_incorrect, cmd);
		return redis_value();
	}

	template<typename T>
	explicit operator std::deque<T>()const {
		return convert_vector<std::deque<T>>();
	}

	template<typename T>
	explicit operator std::vector<T>()const {
		return convert_vector<std::vector<T>>();
	}

	template<typename T>
	explicit operator std::list<T>()const {
		return convert_vector<std::list<T>>();
	}

	template<typename T>
	explicit operator std::set<T>()const{
		return convert_set<std::set<T>>();
	}

	template<typename T>
	explicit operator std::unordered_set<T>()const {
		return convert_set<std::unordered_set<T>>();
	}

	template<typename F, typename V>
	explicit operator std::map<F, V>()const {
		return convert_map<std::map<F, V>>();
	}
	
	template<typename F, typename V>
	explicit operator std::unordered_map<F, V>()const {
		return convert_map<std::unordered_map<F, V>>();
	}
	
	template<typename F, typename V>
	explicit operator std::pair<F, V>()const
	{
		check_error();
		redis_test(reply->type == REDIS_REPLY_ARRAY, redis_error_code::reply_type_incorrect, cmd);
		redis_test(reply->elements == 2, redis_error_code::reply_data_incorrect, cmd);

		std::pair<F, V> _pair;
		{
			redis_reply _redis_reply(reply->element[0], ref_reply);
			_pair.first = (F)std::move(_redis_reply);
		}
		{
			redis_reply _redis_reply(reply->element[1], ref_reply);
			_pair.second = (V)std::move(_redis_reply);
		}

		return _pair;
	}
};


inline redis_reply redis_get_reply(redisContext* _context, const std::string& _cmd = "")
{
	redisReply* _reply = nullptr;
	int ret = redisGetReply(_context, (void**)&_reply);
	redis_reply _reply2(_reply, _cmd);
	redis_test(ret == REDIS_OK, redis_error_code::command_error, _cmd);
	return std::move(_reply2);
}


#ifdef TC_REDIS
}
#endif

#endif