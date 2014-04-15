#ifndef __EVCRAWLER_STRUCT_H__
#define __EVCRAWLER_STRUCT_H__

#include <event2/event_struct.h>
#include <event2/event.h>
#include <event2/event_compat.h>
#include <event2/dns.h>
#include <event2/util.h>
#include <event2/http.h>
#include <event2/buffer.h>
#include <event2/keyvalq_struct.h>

#include <string>
#include <json/json.h>

#include "config.h"
#include "xhead.h"
#include "mylog.h"
#include "host_links.h"

using namespace std;

#define LINK_FIELD_URL       "url"
#define LINK_FIELD_HOST      "host"
#define LINK_FIELD_REFER     "refer"
#define LINK_FIELD_URL_NO    "url_no"
#define LINK_FIELD_HOST_NO   "host_no"
#define LINK_FIELD_SHARD_NO  "shard_no"
#define LINK_FIELD_TIMESTAMP "timestamp"

#define HTTP_HEADER_FIELD_CONTENT_ENCODING "Content-Encoding"
#define HTTP_HEADER_FIELD_STATUS_CODE      "Status-Code"
#define HTTP_HEADER_FIELD_LOCATION         "Location"

//static const char* LINK_FIELD_URL;
//static const char* LINK_FIELD_REFER;
//static const char* LINK_FIELD_URL_NO;
//static const char* LINK_FIELD_SHARD_NO;

enum
{
    READING_HEAD      = 0,
    READING_CONTENT   = 1,
    READ_CONTENT_DONE = 2,
};

enum
{
    CRAWL_URL_LIST = 0,
    CRAWL_STAT,
};

enum {
    OK = 0,
    JSON_PARSE_ERROR,
    JSON_FORMAT_ERROR,
    JSON_ZERO_LENGTH,
};

struct socket_context_t
{
    xhead_t       src_head;
    char*         content;
    uint32_t      to_read_len;
    uint32_t      has_read_len;
    uint32_t      status;
    uint32_t      ret_code;
    uint32_t      cb_count;
};

socket_context_t* socket_context_new();
void socket_context_free(socket_context_t*);

struct thread_context_t
{
    uint32_t     thread_no;
    int32_t      notify_fd[2];
    event_base*  base;
};

struct timer_context_t
{
    uint32_t*         round_robin_step;
    uint32_t          thread_number;
    thread_context_t* thread_context_list;
    char*             host;
    event*            ev;
    uint32_t          last_dns_update;
    uint32_t          dns_update_interval;
    evdns_base*       dnsbase;
};

timer_context_t* timer_context_new(
        const char* host,
        thread_context_t* thread_context_list,
        uint32_t dns_update_interval,
        evdns_base* dnsbase);

void timer_context_free(timer_context_t* ptimer_context);


struct link_info_t
{
    char*        url;             // 需要抓取的url
    char*        host;            // 该url的域名
    char*        path;            // 该url请求的uri
    char*        refer;           // 该url的refer
    uint32_t     url_no;          // url的编号，从上游传输下来，并继续传下去
    uint32_t     host_no;         // host的编号，从上游传输下来，并继续传下去
    uint8_t      shard_no;        // 未来可能需要对linkbase进行分区
    uint8_t      retry_count;     // 重试次数
    uint16_t     port;            // 端口
};

struct crawl_stat_t
{
    uint32_t max_crawling_number;           // 并发抓取的最大链接数
    uint32_t crawling_number;               // 正在抓取的链接数
    uint32_t queue_length;                  // 队列中等待的链接数
    uint32_t pick_url_interval_ms;          // 该域名的唤醒间隔
    uint32_t last_minute;                   // 从1970-00-00以来的分钟序号, time(NULL)/60
    uint32_t crawling_number_in_minute;     // 该分钟内的抓取总数
    uint32_t max_crawling_number_in_minute; // 该分钟内的抓取总数

    uint32_t error_count;                   // 抓取时，发生错误的数目
    uint32_t last_error_timestamp;          // 最后一次错误的时间戳
    uint32_t ok_count;                      // 正常的数目
    uint32_t last_ok_timestamp;             // 最后一次正常的时间戳
};

link_info_t* link_info_new();
void link_info_free(link_info_t* plink_info);

typedef struct
{
    char* host;
    link_info_t* plink_info;
    evhttp_connection* evconn;
    event_base*  base;
    evdns_base*  dnsbase;
}request_context_t;

request_context_t* request_context_new( link_info_t* plink_info, evhttp_connection* evconn,
    event_base*  base, evdns_base*  dnsbase);
void request_context_free(request_context_t* my_request_context);

bool url_parser(const string& url, string& host, string& path, uint16_t& port);
bool process_link_item(const Json::Value& link_item, link_info_t* plink_info);

struct sigint_context_t
{
    evconnlistener* listener;
    event*          evtimeout_list;
    uint32_t        evtimeout_list_size;
};
//typedef struct
//{
//    Json::Value meta;
//    char* content;
//    uint32_t content_size;
//}page_info_t;
//
//page_info_t* page_info_new();
//void page_info_free(page_info_t* page_info);

#endif
