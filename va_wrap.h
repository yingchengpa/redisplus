#pragma once

#ifndef __VA_WRAP_H__
#define __VA_WRAP_H__

#include <stdio.h>
#include <stdint.h>
#include <string>
#include <type_traits>
#include <memory>

class va_in_convert {
public:
	template<typename T>
	const typename std::enable_if<std::is_pod<typename std::decay<T>::type>::value, T>::type *
		operator ()(const T* p) { return p; }
};

class va_out_convert {
public:
	const char* operator ()(const std::string& s) { return s.c_str(); }
	const wchar_t* operator ()(const std::wstring& s) { return s.c_str(); }
#ifdef _AFX
	LPCTSTR operator ()(const CString& s) { return (LPCTSTR)s; }
#endif
	template<typename T>
	const typename std::enable_if<std::is_pod<typename std::decay<T>::type>::value, T>::type &
		operator ()(const T& p) { return p; }
};

template < typename CONVERT, typename RET, typename... ARGS >
class va_wrapper
{
protected:
	RET( *func)(ARGS..., ...);
public:
	va_wrapper(RET( *_func)(ARGS..., ...)) :
		func(_func) {}

	template < typename... ARGS2 >
	RET operator ()(ARGS... args, ARGS2&&... args2) {
		return (*func)(std::forward<ARGS>(args)..., CONVERT()(args2)...);
	}
};

template < typename CONVERT, typename CLS, typename RET, typename... ARGS >
class va_wrapper2
{
protected:
	CLS* cls;
	RET( CLS::*func)(ARGS..., ...);
public:
	va_wrapper2(CLS* _cls, RET( CLS::*_func)(ARGS..., ...)) :
		cls(_cls), func(_func) {}

	template < typename... ARGS2 >
	RET operator ()(ARGS... args, ARGS2&&... args2) {
		return (cls->*func)(std::forward<ARGS>(args)..., CONVERT()(args2)...);
	}
};

template < typename CONVERT, typename RET, typename... ARGS >
va_wrapper<CONVERT, RET, ARGS...> va_analyzer(RET( *func)(ARGS..., ...)) {
	return va_wrapper<CONVERT, RET, ARGS...>(func);
}

template < typename CONVERT, typename CLS, typename RET, typename... ARGS >
va_wrapper2<CONVERT, CLS, RET, ARGS...> va_analyzer(CLS* cls, RET( CLS::*func)(ARGS..., ...)) {
	return va_wrapper2<CONVERT, CLS, RET, ARGS...>(cls, func);
}


#define va_in_wrap(func)		va_analyzer<va_in_convert>(func)
#define va_in_wrap2(cls, func)	va_analyzer<va_in_convert>(cls, func)

#define va_out_wrap(func)		va_analyzer<va_out_convert>(func)
#define va_out_wrap2(cls, func)	va_analyzer<va_out_convert>(cls, func)


class strscanf_helper
{
protected:
	size_t sz;
	
	template < typename T >
	class strscanf_pointer {
	protected:
		T** p;
	public:
		strscanf_pointer(T*& _v) :p(&_v) {}
		~strscanf_pointer() {}
		void* operator ()() { return p; }
	};


	template < typename T >
	class strscanf_integer {
	protected:
		T& v;
		intmax_t im;
	public:
		strscanf_integer(T& _v) :v(_v) {}
		~strscanf_integer() { v = (T)im; }
		void* operator ()() { return &im; }
	};

	template < typename T >
	class strscanf_float {
	protected:
		T& v;
		long double ld;
	public:
		strscanf_float(T& _v) :v(_v), ld(0) {}
		~strscanf_float() { v = (T)ld; }
		void* operator ()() { return &ld; }
	};

	template < typename T >
	class strscanf_string {
	protected:
		std::basic_string<T>& v;
		std::unique_ptr<T[]> p;
	public:
		strscanf_string(std::basic_string<T>& _v, size_t sz) :v(_v), p(new T[sz + 1]) { p.get()[0] = 0; }
		strscanf_string(strscanf_string&&) = default;
		~strscanf_string() { v = p.get(); }
		void* operator ()() { return p.get(); }
	};
public:
	strscanf_helper(size_t _sz) :sz(_sz) { }
	~strscanf_helper() { }

	template<typename T>
	typename std::enable_if<std::is_same<T, char>::value || std::is_same<T, wchar_t>::value, strscanf_string<T>>::type
		operator ()(std::basic_string<T>& v) { return strscanf_string<T>(v, sz); }

	template<typename T>
	typename std::enable_if<std::is_integral<typename std::decay<T>::type>::value, strscanf_integer<T>>::type
		operator ()(T& v) { return strscanf_integer<T>(v); }

	template<typename T>
	typename std::enable_if<std::is_floating_point<typename std::decay<T>::type>::value, strscanf_float<T>>::type
		operator ()(T& v) { return strscanf_float<T>(v); }

	template<typename T>
	strscanf_pointer<T>	operator ()(T*& v) { return strscanf_pointer<T>(v); }
};

template < typename... ARGS >
int strscanf(const std::string& src, const std::string& fmt, ARGS&... args)
{
	return sscanf(src.c_str(), fmt.c_str(), strscanf_helper(src.size())(args)()...);
}


typedef va_out_convert strprintf_helper;

template < typename... ARGS >
std::string strprintf(const std::string& fmt, const ARGS&... args)
{
	std::string s;

	int l = _snprintf(nullptr, 0, fmt.c_str(), strprintf_helper()(args)...);
	if (l > 0) {
		std::unique_ptr<char[]> tmp(new char[l + 1]);
		_snprintf(tmp.get(), l + 1, fmt.c_str(), strprintf_helper()(args)...);
		s = tmp.get();
	}

	return s;
}


inline size_t strreplace(std::string& _str, const std::string& _old, const std::string& _new) 
{
	if (_old.size() == 0)
		return 0;

	size_t count = 0;
	for (size_t p = 0; (p = _str.find(_old, p)) != std::string::npos; p += _old.size()) {
		count++;
	}
	if (count > 0) 
	{
		std::string s2;
		s2.reserve(_str.size() + (_new.size() - _old.size()) * count);
		size_t q = 0;
		for (size_t p = 0; (p = _str.find(_old, q)) != std::string::npos; q = p + _old.size()) {
			s2.append(_str.begin() + q, _str.begin() + p);
			s2.append(_new);
		}
		s2.append(_str.begin() + q, _str.end());
		_str = std::move(s2);
	}
	return count;
}

#endif