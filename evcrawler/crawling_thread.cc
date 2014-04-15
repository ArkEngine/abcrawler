#include <event2/event.h>
#include <event2/event_compat.h>
#include <event2/dns.h>
#include <event2/util.h>
#include <event2/http.h>
#include <event2/buffer.h>
#include <event2/keyvalq_struct.h>
#include <event2/http_struct.h>

#include <sys/socket.h>

#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "MyException.h"
#include "evcrawler_struct.h"
#include "page_info_queue.h"
#include "dns_cache.h"

using namespace std;

int crawl_the_page(event_base* base, evdns_base* dnsbase, link_info_t* plink_info);
void request_cb(struct evhttp_request * my_evhttp_request, void * arg)
{
    request_context_t* my_request_context = (request_context_t*)arg;
    host_links* my_host_links = host_links::getInstance();
    //struct event_base* base = evhttp_connection_get_base(my_request_context->my_evhttp_connection);

    if (my_evhttp_request == NULL)
    {
        my_host_links->link_finish_crawl(my_request_context->plink_info, false);
        if (my_request_context->plink_info->retry_count < 3)
        {
            my_request_context->plink_info->retry_count++;
            // try again
            host_links::getInstance()->put_link(my_request_context->plink_info);
            ALARM("request = NULL, url[%s] retry[%u] msg[%m]",
                    my_request_context->plink_info->url,
                    my_request_context->plink_info->retry_count);
        }
        else
        {
            ALARM("request finally fail, url[%s] retry[%u] msg[%m]",
                    my_request_context->plink_info->url,
                    my_request_context->plink_info->retry_count);
            link_info_free(my_request_context->plink_info);
        }
    }
    else
    {
        struct evbuffer* buff = evhttp_request_get_input_buffer(my_evhttp_request);
        MySuicideAssert(NULL != buff);
        size_t len = evbuffer_get_length(buff);

        page_info_t* page_info = new page_info_t();
        if (len > 0)
        {
            page_info->m_content_size = len;
            page_info->m_content = (char*) malloc(len);
            MySuicideAssert(len == (size_t)evbuffer_remove( buff, page_info->m_content, len));
        }
        struct evkeyvalq * head_list = evhttp_request_get_input_headers(my_evhttp_request);
        MySuicideAssert(head_list);
        // 需要识别的项目
        // -1- 是否压缩
        // -2- 链接是否保持
        // -3-
        //struct evkeyval* header = NULL;
        //for (header = head_list->tqh_first; header; header = header->next.tqe_next)
        //{
        //printf("%s: %s\n", header->key, header->value);
        //}

        uint32_t status_code = evhttp_request_get_response_code(my_evhttp_request);

        if (5 == status_code/100)
        {
            my_host_links->link_finish_crawl(my_request_context->plink_info, false);
            if (my_request_context->plink_info->retry_count < 3)
            {
                my_request_context->plink_info->retry_count++;
                // try again
                host_links::getInstance()->put_link(my_request_context->plink_info);
                ALARM("5XX error, url[%s] code[%u] retry[%u] msg[%m]",
                        my_request_context->plink_info->url,
                        status_code,
                        my_request_context->plink_info->retry_count);
            }
            else
            {
                link_info_free(my_request_context->plink_info);
                ALARM("5XX error, url[%s] code[%u] retry[%u] msg[%m]",
                        my_request_context->plink_info->url,
                        status_code,
                        my_request_context->plink_info->retry_count);
            }
        }
        else
        {
            // 表示该url抓取成功
            my_host_links->link_finish_crawl(my_request_context->plink_info, true);

            page_info->m_meta[HTTP_HEADER_FIELD_STATUS_CODE] = status_code;
            page_info->m_meta[LINK_FIELD_URL]       = my_request_context->plink_info->url;
            page_info->m_meta[LINK_FIELD_HOST]      = my_request_context->plink_info->host;
            page_info->m_meta[LINK_FIELD_URL_NO]    = my_request_context->plink_info->url_no;
            page_info->m_meta[LINK_FIELD_HOST_NO]   = my_request_context->plink_info->host_no;
            page_info->m_meta[LINK_FIELD_SHARD_NO]  = my_request_context->plink_info->shard_no;
            page_info->m_meta[LINK_FIELD_TIMESTAMP] = (uint32_t)time(NULL);

            const char* content_encoding = evhttp_find_header(head_list, HTTP_HEADER_FIELD_CONTENT_ENCODING);
            if (content_encoding != NULL) 
            {
                page_info->m_meta[HTTP_HEADER_FIELD_CONTENT_ENCODING] = content_encoding;
            }
            // 如果是302跳转的话，需要继续处理
            if (302 == status_code || 301 == status_code)
            {
                const char* new_location = evhttp_find_header(head_list, HTTP_HEADER_FIELD_LOCATION);
                if (new_location)
                {
                    // 跳转并不实时跳转，而是交给离线处理，优点:
                    // -1- 跳转的地址作为扩散出来的新地址，逻辑上统一。
                    // -2- 对跳转后的新地址，也要经过url去重过滤，避免大面积的跳转到首页，抓取资源浪费
                    // -3- 为了陷入爬虫陷阱，可以设置一个跳转ttl。TODO，优先级不高
                    page_info->m_meta[HTTP_HEADER_FIELD_LOCATION] = string(new_location);
                }
                else
                {
                    ALARM("url[%s] redirect to null", my_request_context->plink_info->url);
                }
            }
            // 把page信息写入dump队列中
            page_info_queue::getInstance()->put(page_info);
            link_info_free(my_request_context->plink_info);
        }
    }

    evhttp_connection_free (my_request_context->evconn);
    request_context_free(my_request_context);
}

int crawl_the_page(event_base* base, evdns_base* dnsbase, link_info_t* plink_info)
{
    //DEBUG("host:[%s] port:[%d] path:[%s]", plink_info->host, plink_info->port, plink_info->path);
    const char* proxyip = Config::getInstance()->ProxyIP();
    string host2ip = Config::getInstance()->IsProxyEnabled() ? proxyip : dns_cache::getInstance()->search(plink_info->host);
    const char* host2crawl = (0 == host2ip.length()) ? plink_info->host : host2ip.c_str();
    uint32_t port = Config::getInstance()->IsProxyEnabled() ? Config::getInstance()->ProxyPort() : plink_info->port;
    DEBUG("host[%s] port[%d]", host2crawl, port); 
    //DEBUG("host[%s] dns_cache_ip[%s]", plink_info->host, host2ip.c_str());
    struct evhttp_connection* my_evhttp_connection = evhttp_connection_base_new (base, dnsbase, host2crawl, port);
    MySuicideAssert(NULL != my_evhttp_connection);
    const uint32_t CONNECTION_MAX_RETRY = 3;
    evhttp_connection_set_retries(my_evhttp_connection, CONNECTION_MAX_RETRY);
    const uint32_t TIMEOUT_IN_SECONDS = 30;
    evhttp_connection_set_timeout (my_evhttp_connection, TIMEOUT_IN_SECONDS);

    request_context_t* my_request_context = request_context_new(plink_info, my_evhttp_connection, base, dnsbase);
    MySuicideAssert(NULL != my_request_context);

    struct evhttp_request * my_evhttp_request = evhttp_request_new (
            request_cb,
            my_request_context);
    MySuicideAssert(NULL != my_evhttp_request);

    const char* UA = "Mozilla/5.0 (Windows NT 6.1; rv:12.0) Gecko/20100101 Firefox/12.0";
    MySuicideAssert(0 == evhttp_add_header(my_evhttp_request->output_headers, "User-Agent",      UA));
    MySuicideAssert(0 == evhttp_add_header(my_evhttp_request->output_headers, "Accept",          "*/*"));
    MySuicideAssert(0 == evhttp_add_header(my_evhttp_request->output_headers, "Host",            plink_info->host));
    //MySuicideAssert(0 == evhttp_add_header(my_evhttp_request->output_headers, "Connection",      "Keep-Alive"));
    MySuicideAssert(0 == evhttp_add_header(my_evhttp_request->output_headers, "Accept-Encoding", "gzip"));
    if (plink_info->refer)
    {
        int iret = 0;
        if(0 != (iret = evhttp_add_header(my_evhttp_request->output_headers, "Referer", plink_info->refer)))
        {
            ALARM("add refer fail[%d] refer[%s]", iret, plink_info->refer);
        }
    }
    
    DEBUG("path[%s]", plink_info->path);
    int ret = evhttp_make_request (my_evhttp_connection, my_evhttp_request, EVHTTP_REQ_GET, plink_info->url);
    //int ret = evhttp_make_request (my_evhttp_connection, my_evhttp_request, EVHTTP_REQ_GET, plink_info->path);
    if (0 != ret)
    {
        ALARM("evhttp_make_request fail. ret[%d]", ret);
        host_links::getInstance()->put_link(plink_info);
        evhttp_connection_free(my_evhttp_connection);
        request_context_free(my_request_context);
    }
    return ret;
}

enum
{
    READ_NOTIFICATION_LENGTH = 0,
    READ_NOTIFICATION_CONTENT,
};
struct notify_context_t
{
    thread_context_t* thread_context;
    event_base*       base;
    evdns_base*       dnsbase;
    char              host[256];
    uint8_t           host_length;
    uint8_t           read_length;
    uint8_t           status;
    uint8_t           hold_place;
};

    static void
start_crawling(int fd, short _event, void *arg)
{
    // 回调函数被唤醒，某个站点需要抓取了
    MySuicideAssert(_event & EV_READ);
    notify_context_t* notify_context = (notify_context_t*)arg;
    MySuicideAssert(notify_context != NULL);
    uint8_t read_len = 0;

    switch(notify_context->status)
    {
        case READ_NOTIFICATION_LENGTH:
            MySuicideAssert(sizeof(notify_context->host_length) 
                    == read(fd, &(notify_context->host_length), sizeof(notify_context->host_length)));
            notify_context->status = READ_NOTIFICATION_CONTENT;
            // break; // 直接进入读取host的阶段
        case READ_NOTIFICATION_CONTENT:
            read_len = read(fd, &(notify_context->host[notify_context->read_length]), notify_context->host_length);
            notify_context->read_length += read_len;
            if (notify_context->host_length == notify_context->read_length)
            {
                notify_context->host[notify_context->host_length] = '\0';
                notify_context->status = READ_NOTIFICATION_LENGTH;
                notify_context->read_length = 0;
                notify_context->host_length = 0;
                // 从全局的链接池中获取一个url
                // 抓取的压力控制策略将由每分钟抓取数目和并发数来同时控制
                // -1- 根据配置host_visit_interval.config.json中的每分钟抓取数，timer定时器平均分配时间，定时唤醒抓取线程开始抓取
                // -2- 在此过程中，并发抓取数不能超过上限
                link_info_t* plink_info = host_links::getInstance()->get_link(notify_context->host);
                if (NULL == plink_info)
                {
                    //ROUTN("thread[%u]  host[%s]'s url_list is empty.",
                    //        notify_context->thread_context->thread_no, notify_context->host);
                    break;
                }
                else
                {
                    //ROUTN("thread[%u] need to crawl host[%s], url[%s]",
                    //        notify_context->thread_context->thread_no, notify_context->host, plink_info->url);
                    crawl_the_page(notify_context->base, notify_context->dnsbase, plink_info);
                }
            }
            break;
        default:
            MySuicideAssert(0);
    }
}

void* crawling_thread(void* arg)
{
    thread_context_t* thread_context = (thread_context_t*)arg;
    MySuicideAssert(thread_context != NULL);

    event_base* base = event_base_new();
    MySuicideAssert(NULL != base);
    thread_context->base = base;

    struct evdns_base *dnsbase = evdns_base_new(base, 1);
    MySuicideAssert(NULL != dnsbase);

    notify_context_t* notify_context = (notify_context_t*)calloc(sizeof(notify_context_t), 1);
    MySuicideAssert(NULL != notify_context);
    notify_context->thread_context = thread_context;
    notify_context->status         = READ_NOTIFICATION_LENGTH;
    notify_context->base           = base;
    notify_context->dnsbase        = dnsbase;

    // 设置与主线程的沟通方式
    struct event ev;
    event_set(&ev, thread_context->notify_fd[1], EV_READ|EV_PERSIST, start_crawling, notify_context);
    event_base_set(base, &ev);
    event_add(&ev, 0);

    event_base_dispatch(base);

    evdns_base_free(dnsbase, 0);
    event_base_free(base);
    free(notify_context);
    return NULL;
}
