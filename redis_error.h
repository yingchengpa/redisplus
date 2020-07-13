#pragma once

#ifndef __REDIS_ERROR_H__
#define __REDIS_ERROR_H__

#ifdef TC_REDIS
namespace TC_REDIS {
#endif

#define DECLARE_REDIS_ERROR_CODE(code) \
	static const char* code() { return #code; }

//redis异常码类
class redis_error_code {
protected:
	const char* code;
public:
	redis_error_code(const char* (*_func)()) :code((*_func)()) { }
	std::string get_describe() { return code; }

	void* get_value() { return (void*)code; }
	//定义异常码
	DECLARE_REDIS_ERROR_CODE(reply_is_null);		//reply为null
	DECLARE_REDIS_ERROR_CODE(reply_type_incorrect);	//reply类型不正确
	DECLARE_REDIS_ERROR_CODE(reply_data_incorrect);	//reply数据不正确
	DECLARE_REDIS_ERROR_CODE(reply_is_error);		//reply返回的是错误
	DECLARE_REDIS_ERROR_CODE(test_failed);			//测试为false
	DECLARE_REDIS_ERROR_CODE(command_error);		//命令错误
	DECLARE_REDIS_ERROR_CODE(exceeded_retry_times);	//超出重试上限
	//DECLARE_REDIS_ERROR_CODE(index_out_of_range);	//索引越界
	//DECLARE_REDIS_ERROR_CODE(object_locked);		//对象被锁定
};

#undef DECLARE_REDIS_ERROR_CODE

//redis异常类
class redis_error {
public:
	redis_error_code code;
	std::string describe;
	std::string cmd;

	redis_error(redis_error_code _code,
		const std::string& _describe = "",
		const std::string& _cmd = "") :code(_code), describe(_describe), cmd(_cmd)
	{
	}
	~redis_error() {}
};

//测试一个表达式
//若为false,抛出一个指定的redis异常
inline void redis_test(bool _exp, redis_error_code _code = redis_error_code::test_failed, const std::string& _cmd = "")
{
	if (!_exp) {
		throw redis_error(_code, _code.get_describe(), _cmd);
	}
}

#ifdef TC_REDIS
}
#endif

#endif