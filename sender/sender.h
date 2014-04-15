#ifndef _SENDER_H_
#define _SENDER_H_

#include <stdint.h>
#include <set>
#include <string>
using namespace std;

#define FORMAT_QUEUE_OP "%u # %u"

enum
{
    OK = 0,
    ROLL_BACK = 1,
    ERROR     = 2,
};

enum
{
    T_INT = 0,
    T_STR = 1,
    T_ARR = 2,
};

#ifndef TIME_US_COST
#define TIME_US_COST(pre, cur) (((cur.tv_sec)-(pre.tv_sec))*1000000 + \
                                (cur.tv_usec) - (pre.tv_usec))
#endif

struct sender_config_t
{
	uint32_t sender_id;
	char     mode[32];             ///> 支持xhead数据回放和mongo两种类型
	char     channel[64];          ///> 消息接受者的名字，它的进度文件为 $channel.offset
	char     host[32];             ///> 消息接受者ip
	uint32_t port;                 ///> 消息接受者port
	uint32_t long_connect;         ///> 是否长链接
	uint32_t send_toms;            ///> socket的写入超时 单位: ms
	uint32_t recv_toms;            ///> socket的读取超时 单位: ms
	uint32_t conn_toms;            ///> socket的连接超时 单位: ms
	uint32_t enable;               ///> 是否开启
	uint32_t sleep_interval_us;    ///> 休息时间
                                   
	set<string> events_set;        ///> 监听的事件列表
	char     qpath[128];           ///> 消息队列的路径 ./data/$qpath/
	char     qfile[128];           ///> 消息队列的文件名, $file.n, n为整数
	set<string> filter_set;        ///> 需要过滤的key
                                  
    // solrd类型关注              
	char     url_suffix[128];      ///> post的url后缀

    // mongo类型关注              
	char     database[32];         ///> 更新record的所在数据库
	char     collection[32];       ///> 更新record的所在数据库的collection
	char     indexkey[32];         ///> 更新record的主键字段名称
	uint32_t insert_mode;          ///> 使用insert更新，适合批量建库所用，置位1生效，默认为0
	set<string> int2date_set;      ///> 需要从int转化为date的数据类型
	map<string, uint32_t> need_default_map;  ///> 需要为这些key设置默认值
};
#endif
