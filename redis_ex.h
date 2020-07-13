#ifdef _DEBUG
#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif
#endif

#ifndef __REDIS_EX_H__
#define __REDIS_EX_H__


#include <hiredis.h>
#include <stdint.h>
#include <string>
#include <deque>
#include <vector>
#include <list>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <sstream>
#include <algorithm>
#include <functional>

#include "va_wrap.h"

#define TC_REDIS tc_redis
#include "redis_error.h"
#include "redis_reply.h"
#include "redis_transaction.h"
#include "redis_context.h"


#endif
