#pragma once

#ifndef __REDIS_TRANSACTION_H__
#define __REDIS_TRANSACTION_H__

#ifdef TC_REDIS
namespace TC_REDIS {
#endif

//redis������
//�ύ������Ỻ�浽������,execʱһ�����ύ
class redis_transaction
{
protected:
	redisContext* context;
	std::deque<std::function<std::string()>> commands;

	//ת������Ĳ�������
	class redis_transaction_param_convert {
	public:
		template<typename T>
		typename std::enable_if<std::is_integral<typename std::decay<T>::type>::value 
			|| std::is_enum<typename std::decay<T>::type>::value, int64_t>::type
			operator ()(T v) { return v; }

		template<typename T>
		typename std::enable_if<std::is_floating_point<typename std::decay<T>::type>::value,
			double>::type
			operator ()(T v) { return v; }

		std::string operator ()(const char* v) { return v; }
		const std::string& operator ()(const std::string& v) { return v; }
	};

	//��ֵ������ʽ����lambda���ʽ
	//�������еĲ��������Կ����ķ�ʽ����
	template<typename... ARGS>
	void append_command_converted(const std::string& _cmd, ARGS&&... _args) {
		commands.push_back([=]() {
			return redis_append_command(context, _cmd.c_str(), _args...);
		});
	}
public:
	redis_transaction(redisContext* _context) :
		context(_context)
	{
	}
	~redis_transaction() {}

	//ʹ�ò������ݹ�����������
	template<typename... ARGS, typename = std::enable_if<!is_redis_command_argv<typename std::decay<ARGS>::type...>::value>::type>
	void append_command(const std::string& _cmd, ARGS&&... _args) {
		append_command_converted(_cmd, redis_transaction_param_convert()(_args)...);
	}

	//ʹ��std::vector<std::string>�ݹ�����������
	void append_command(const std::string& _cmd, const std::vector<std::string>& _argv) {
		append_command_converted(_cmd, _argv);
	}


	//ִ������
	//�Զ��ڶ���ͷ����� MULTI ����
	//�Զ��ڶ���β����� EXEC ����
	//��������һ�����ύ
	redis_reply exec()
	{
		std::vector<std::string> _appends;
		_appends.push_back(redis_append_command(context, "MULTI"));
		for (auto& _cmd : commands) {
			_appends.push_back(_cmd());
		}
		commands.clear();
		_appends.push_back(redis_append_command(context, "EXEC"));

		std::vector<redis_reply> _replys;
		for (auto& _cmd : _appends) {
			redis_reply _reply = redis_get_reply(context, _cmd);
			_replys.push_back(std::move(_reply));
		}

		redis_test(_replys[0].is_ok(),
			redis_error_code::reply_is_error, _appends[0]);
		for (size_t i = 1; i < _replys.size() - 1; i++) {
			redis_test(_replys[i].is_queued(),
				redis_error_code::reply_is_error, _appends[i]);
		}
		return std::move(_replys.back());
	}

	//ȡ������
	//�鹹һ��OK�ķ���
	//ʲô��������ύ
	redis_reply discard()
	{
		commands.clear();

		auto _reply = new redisReply{ 0 };
		_reply->type = REDIS_REPLY_STATUS;
		_reply->str = "OK";
		_reply->len = strlen(_reply->str);

		return redis_reply(_reply, std::shared_ptr<redisReply>(_reply), "DISCARD");
	}

};

//redis watch����
//�������һ��ʹ��
inline redis_reply redis_watch(redisContext* _context,
	const std::vector<std::string>& _keys,	//��Ҫwatch��key
	uint32_t _retry_times,					//���Դ���
	std::function<redis_reply()> _func)		//ִ������
{
	do
	{
		redis_test(
			redis_reply(_context, "WATCH", _keys).is_ok(),
			redis_error_code::command_error);

		auto _reply = _func();

		std::string _cmd;
		redis_test(1 == strscanf(_reply.get_cmd(), "%s", _cmd),
			redis_error_code::command_error, _reply.get_cmd());

		if (_stricmp(_cmd.c_str(), "UNWATCH") == 0) {
			redis_test(_reply.is_ok(), redis_error_code::command_error);
			return std::move(_reply);
		}
		else if (_stricmp(_cmd.c_str(), "DISCARD") == 0) {
			redis_test(_reply.is_ok(), redis_error_code::command_error);
			return std::move(_reply);
		}
		else if (_stricmp(_cmd.c_str(), "EXEC") == 0)
		{
			if (!_reply.is_nil()) {
				return std::move(_reply);
			}
			if (0 == _retry_times--) {
				return std::move(_reply);
			}
		}
		else {
			redis_test(false, redis_error_code::command_error, _reply.get_cmd());
		}
	} while (true);
}


//�ж������Ƿ�ִ��
inline bool is_transaction_executed(const redis_reply& _reply){
	return _stricmp(_reply.get_cmd().c_str(), "EXEC") == 0;
}

//�ж������Ƿ�ɹ�
inline bool is_transaction_succeed(const redis_reply& _reply){
	return is_transaction_executed(_reply) && !_reply.is_nil();
}

/*
	һ���򵥵�����

	redis_reply _reply = redis_watch(_context, { _key }, 3, [&]()
	{
		auto value = (redis_value)redis_reply(_context, "GET", _key);

		redis_transaction _trans(_context);
		_trans.append_command("SET", _key, ...);
		return _trans.exec();
	});

	if (is_transaction_succeed(_reply)) {
		...
	}
*/

#ifdef TC_REDIS
}
#endif

#endif