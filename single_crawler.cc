#include <event2/dns.h>
#include <event2/util.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/buffer.h>
#include <event2/keyvalq_struct.h>
#include <event2/http_struct.h>
#include <event2/bufferevent.h>
#include <event2/keyvalq_struct.h>

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

using namespace std;

#define TrueOrDie(expression) \
    do \
    { \
        if(!(expression)) \
        { \
            printf("TrueOrDie Failed. ["#expression"] EMSG[%m]\n"); \
            while(0 != raise(SIGKILL)){} \
            exit(1); \
        } \
    } \
    while(0)

typedef struct
{
    char* host;
    char* content;
    uint32_t total_size;
    uint32_t used_size;
    uint32_t read_count;
    struct evhttp_connection* my_evhttp_connection;
}connect_context_t;

uint32_t request_count = 0;

connect_context_t* connect_context_new(
        const char* host,
        struct evhttp_connection* myconn,
        const uint32_t content_size)
{
    TrueOrDie(NULL != host);
    TrueOrDie(content_size > 0);
    connect_context_t* my_connection_context = (connect_context_t*)malloc(sizeof(connect_context_t));
    my_connection_context->my_evhttp_connection = myconn;
    my_connection_context->host = strdup(host);

    my_connection_context->content = (char*)malloc(content_size);
    TrueOrDie(NULL != my_connection_context->content);
    my_connection_context->total_size = content_size;
    my_connection_context->used_size = 0;

    my_connection_context->read_count = 0;
    return my_connection_context;
}

connect_context_t* connect_context_free(connect_context_t* my_connection_context)
{
    free(my_connection_context->host);
    my_connection_context->host = NULL;

    free(my_connection_context->content);
    my_connection_context->content = NULL;

    my_connection_context->total_size = 0;
    my_connection_context->used_size = 0;
    my_connection_context->read_count = 0;
}

void request_cb(struct evhttp_request * my_evhttp_request, void * arg)
{
    printf("=========================\n");
    connect_context_t* my_connection_context = (connect_context_t*)arg;
    struct event_base* base = evhttp_connection_get_base(my_connection_context->my_evhttp_connection);

    if (my_evhttp_request == NULL)
    {
        printf("request = NULL, msg[%m]\n");
    }
    else
    {
        struct evbuffer* buff = evhttp_request_get_input_buffer(my_evhttp_request);
        TrueOrDie(NULL != buff);
        size_t len = evbuffer_get_length(buff);
        if (len > (my_connection_context->total_size - my_connection_context->used_size))
        {
            my_connection_context->content = (char*)realloc(my_connection_context->content, my_connection_context->total_size * 2);
            TrueOrDie(NULL != my_connection_context->content);
            my_connection_context->total_size *= 2;
        }
        int read_len = evbuffer_remove( buff, &my_connection_context->content[my_connection_context->used_size], len);
        TrueOrDie(read_len == len);
        my_connection_context->used_size += read_len;
        my_connection_context->content[my_connection_context->used_size] = 0;
        my_connection_context->read_count ++;
        printf("read %d byte\n", read_len);
        struct evkeyvalq * head_list = evhttp_request_get_input_headers(my_evhttp_request);
        TrueOrDie(head_list);
        struct evkeyval* header = NULL;
        // 需要识别的项目
        // -1- 是否压缩
        // -2- 链接是否保持
        // -3-
        for (header = head_list->tqh_first; header; header = header->next.tqe_next)
        {
            printf("%s: %s\n", header->key, header->value);
        }

        const char* content_encoding = evhttp_find_header(head_list, "Content-Encoding");
        const char* DUMP_FILE_NAME = "page";
        if (content_encoding != NULL) 
        {
            if (strstr(content_encoding, "gzip"))
            {
                DUMP_FILE_NAME = "page.gz";
            }
        }
        printf("--------------------\n");
        mode_t amode = (0 == access(DUMP_FILE_NAME, F_OK)) ? O_WRONLY|O_TRUNC : O_WRONLY|O_TRUNC|O_CREAT;
        int fd = open(DUMP_FILE_NAME, amode, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
        TrueOrDie (-1 != fd);
        TrueOrDie ( my_connection_context->used_size == write(fd, my_connection_context->content, my_connection_context->used_size));
        close(fd);
        printf("%u write into %s\n", request_count, DUMP_FILE_NAME);
        printf("--------------------\n");

        printf("Status-Code: %d\n", evhttp_request_get_response_code(my_evhttp_request));

        //char* ip = NULL;
        //uint16_t port = 0;
        //evhttp_connection_get_peer (my_connection_context->my_evhttp_connection, &ip, &port);
        //printf("IP: %s PORT: %d\n", ip, port);
    }

//    evhttp_connection_free (my_connection_context->my_evhttp_connection);
    connect_context_free(my_connection_context);

    if (--request_count == 0)
    {
        event_base_loopexit(base, NULL);
    }
}

    static void
event_cb(struct bufferevent *bev, short events, void *ctx)
{
    printf("------------------------------\n");
    printf("%20s 000 00000 %05u bufferevent shutdown. events: 0x%x\n", (char*)ctx, request_count, events);
    //bufferevent_free(bev);
}

bool url_parser(const string& url, string& host, string& path , uint16_t& port)
{
    char http_format[] = "http://";
    size_t http_start = url.find(http_format);
    if (http_start == string::npos)
    {
        return false;
    }

    size_t path_start = url.find('/', sizeof(http_format));
    if (path_start == string::npos)
    {
        path = "/";
        host = url.substr(http_start+sizeof(http_format)-1);
    }
    else
    {
        path = url.substr(path_start);

        host = url.substr(http_start+sizeof(http_format)-1, path_start-sizeof(http_format)+1);

        size_t port_start = host.find(':');
        if (port_start != string::npos)
        {
            string str_port = host.substr(port_start+1);
            port = atoi(str_port.c_str());
            host = host.substr(0, port_start);
        }
    }
    return true;
}

int page_crawler(event_base* base, evdns_base* dnsbase, const string& url)
{
    string host;
    string path;
    uint16_t port = 80;
    TrueOrDie(url_parser(url, host, path, port));
    printf("host:[%s] port:[%d] path:[%s]\n", host.c_str(), port, path.c_str());

    struct evhttp_connection* my_evhttp_connection = evhttp_connection_base_new (base, dnsbase, host.c_str(), port);
    TrueOrDie(NULL != my_evhttp_connection);
    struct bufferevent* bev = evhttp_connection_get_bufferevent(my_evhttp_connection);
    TrueOrDie(NULL != bev);
    bufferevent_setcb(bev, NULL, NULL, event_cb, strdup((char*)host.c_str()));

    const uint32_t TIMEOUT_IN_SECONDS = 1;
    struct timeval tv;
    tv.tv_sec  = TIMEOUT_IN_SECONDS * 10;
    tv.tv_usec = 0; 
    TrueOrDie(0 == bufferevent_set_timeouts(bev, &tv, &tv));
    const uint32_t CONNECTION_MAX_RETRY = 3;
    evhttp_connection_set_retries(my_evhttp_connection, CONNECTION_MAX_RETRY);
    evhttp_connection_set_timeout (my_evhttp_connection, TIMEOUT_IN_SECONDS);

    const uint32_t CONTENT_MAX_BUFFSIZE = 1024*1024;
    connect_context_t* my_connection_context = connect_context_new(
            host.c_str(),
            my_evhttp_connection,
            CONTENT_MAX_BUFFSIZE);
    TrueOrDie(NULL != my_connection_context);

    struct evhttp_request * my_evhttp_request = evhttp_request_new (
            request_cb,
            my_connection_context);
    TrueOrDie(NULL != my_evhttp_request);

    const char* UA = "Mozilla/5.0 (Windows NT 6.1; rv:12.0) Gecko/20100101 Firefox/12.0";
    TrueOrDie(0 == evhttp_add_header(my_evhttp_request->output_headers, "User-Agent",      UA));
    TrueOrDie(0 == evhttp_add_header(my_evhttp_request->output_headers, "Accept",          "*/*"));
    TrueOrDie(0 == evhttp_add_header(my_evhttp_request->output_headers, "Host",            host.c_str()));
    TrueOrDie(0 == evhttp_add_header(my_evhttp_request->output_headers, "Connection",      "Keep-Alive"));
    TrueOrDie(0 == evhttp_add_header(my_evhttp_request->output_headers, "Accept-Encoding", "compress, gzip"));

    return evhttp_make_request (my_evhttp_connection, my_evhttp_request, EVHTTP_REQ_GET, path.c_str());
}

/* Take a list of host names from the command line and resolve them in
 *  * parallel. */
int main(int argc, char **argv)
{
    if (argc != 2)
    {
        printf("%s url_file\n", argv[0]);
        return 1;
    }

    struct evdns_base *dnsbase = NULL;
    struct event_base *base    = NULL;

    base = event_base_new();
    TrueOrDie(NULL != base);
    dnsbase = evdns_base_new(base, 1);
    TrueOrDie(NULL != dnsbase);

    vector<string> url_list;

    FILE* fp = fopen(argv[1], "r");
    TrueOrDie(NULL != fp);

    char strurl[512];
    while(NULL != fgets(strurl, sizeof(strurl), fp))
    {
        strurl[strlen(strurl) - 1] = '\0';
        printf("%s\n", strurl);
        url_list.push_back(strurl);
    }
    request_count = url_list.size();
    for (uint32_t i=0; i<request_count; i++)
    {
        TrueOrDie(0 == page_crawler(base, dnsbase, url_list[i]));
    }


    // struct evhttp_connection * evhttp_connection_base_new (
    //                                   struct event_base *base,
    //                                   struct evdns_base *dnsbase,
    //                                   const char *address,
    //                                   unsigned short port)
    // void                       evhttp_connection_free (
    //                                   struct evhttp_connection *evcon)
    // void                       evhttp_connection_set_closecb (
    //                                   struct evhttp_connection *evcon,
    //                                   void(*)(struct evhttp_connection *, void *),
    //                                   void *)
    // void                       evhttp_connection_set_retries (
    //                                   struct evhttp_connection *evcon,
    //                                   int retry_max)
    // void                       evhttp_connection_set_timeout (
    //                                   struct evhttp_connection *evcon,
    //                                   int timeout_in_secs)
    // int                        evhttp_make_request (
    //                                   struct evhttp_connection *evcon,
    //                                   struct evhttp_request *req,
    //                                   enum evhttp_cmd_type type,
    //                                   const char *uri)
    // struct evbuffer *          evhttp_request_get_input_buffer (
    //                                   struct evhttp_request *req)
    // struct evbuffer *          evhttp_request_get_output_buffer (
    //                                   struct evhttp_request *req)
    // struct evkeyvalq *         evhttp_request_get_output_headers (
    //                                   struct evhttp_request *req)
    // struct evhttp_request *    evhttp_request_new (
    //                                   void(*cb)(struct evhttp_request *, void *),
    //                                   void *arg)

    event_base_dispatch(base);
    //evhttp_request_free (my_evhttp_request);

    evdns_base_free(dnsbase, 0);
    event_base_free(base);

    printf("exit.\n");

    return 0;
}
