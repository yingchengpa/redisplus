#pragma once

#ifndef __REDIS_ERROR_H__
#define __REDIS_ERROR_H__

#ifdef TC_REDIS
namespace TC_REDIS {
#endif

#define DECLARE_REDIS_ERROR_CODE(code) \
	static const char* code() { return #code; }

//redis�쳣����
class redis_error_code {
protected:
	const char* code;
public:
	redis_error_code(const char* (*_func)()) :code((*_func)()) { }
	std::string get_describe() { return code; }

	void* get_value() { return (void*)code; }
	//�����쳣��
	DECLARE_REDIS_ERROR_CODE(reply_is_null);		//replyΪnull
	DECLARE_REDIS_ERROR_CODE(reply_type_incorrect);	//reply���Ͳ���ȷ
	DECLARE_REDIS_ERROR_CODE(reply_data_incorrect);	//reply���ݲ���ȷ
	DECLARE_REDIS_ERROR_CODE(reply_is_error);		//reply���ص��Ǵ���
	DECLARE_REDIS_ERROR_CODE(test_failed);			//����Ϊfalse
	DECLARE_REDIS_ERROR_CODE(command_error);		//�������
	DECLARE_REDIS_ERROR_CODE(exceeded_retry_times);	//������������
	//DECLARE_REDIS_ERROR_CODE(index_out_of_range);	//����Խ��
	//DECLARE_REDIS_ERROR_CODE(object_locked);		//��������
};

#undef DECLARE_REDIS_ERROR_CODE

//redis�쳣��
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

//����һ�����ʽ
//��Ϊfalse,�׳�һ��ָ����redis�쳣
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