#include <event2/listener.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/event.h>
#include <event2/event_struct.h>
#include <event2/event_compat.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <json/json.h>

#include "config.h"
#include "mylog.h"
#include "xhead.h"
#include "host_links.h"
#include "page_info_queue.h"
#include "MyException.h"
#include "evcrawler_struct.h"
#include "dns_cache.h"

void* crawling_thread(void*);
void* dump2disk_thread(void*);

void PrintHelp(void)
{
    printf("\nUsage:\n");
    printf("%s <options>\n", PROJNAME);
    printf("  options:\n");
    printf("    -c:  #conf path\n");
    printf("    -v:  #version\n");
    printf("    -h:  #This page\n");
    printf("\n\n");
}

void PrintVersion(void)
{
    printf("Project    :  %s\n", PROJNAME);
    printf("Version    :  %s\n", VERSION);
    printf("BuildDate  :  %s - %s\n", __DATE__, __TIME__);
}

void dns_query_cb(int errcode, struct evutil_addrinfo *addr, void* ctx)
{
    timer_context_t* timer_context = (timer_context_t*)ctx;
    if (errcode)
    {
        ALARM("host[%s] dns_query fail: %s", timer_context->host, evutil_gai_strerror(errcode));
    }
    else
    {
        struct evutil_addrinfo *ai;
        vector<string> ip_list;
        for (ai = addr; ai; ai = ai->ai_next) {
            if (ai->ai_family == AF_INET) {
                char ipbuf[20];
                struct sockaddr_in *sin = (struct sockaddr_in *)ai->ai_addr;
                const char* ip = evutil_inet_ntop(AF_INET, &sin->sin_addr, ipbuf, sizeof(ipbuf));
                if (ip)
                {
                    ip_list.push_back(ip);
                    ROUTN("host[%s] ip[%s]", timer_context->host, ip);
                }
            }
        }
        dns_cache::getInstance()->update(timer_context->host, ip_list);
        timer_context->last_dns_update = (uint32_t)time(NULL);
        evutil_freeaddrinfo(addr);
    }
}

void make_dns_query(struct evdns_base* dnsbase, timer_context_t* timer_context)
{
    struct evutil_addrinfo hints;
    
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_flags = EVUTIL_AI_CANONNAME;
    /* Unless we specify a socktype, we'll get at least two entries for
     *          * each address: one for TCP and one for UDP. That's not what we
     *                   * want. */
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    evdns_getaddrinfo( dnsbase, timer_context->host, NULL /* no service name given */,
            &hints, dns_query_cb, timer_context);
}

    static void
main_process_cb(struct bufferevent *bev, void *ctx)
{
    /* This callback is invoked when there is data to read on bev. */
    socket_context_t* psocket_context = (socket_context_t*) ctx;
    xhead_t& src_head = psocket_context->src_head;
    size_t read_len = 0;
    xhead_t dst_head;
    int ret = 0;
    uint32_t process_ok_count = 0;
    // 解析json
    Json::Features features;
    features.strictRoot_ = true;
    Json::Value root;
    Json::Reader reader(features);
    psocket_context->cb_count++;
    ROUTN("socket_ctx[%p] cb_count[%u]", psocket_context, psocket_context->cb_count);

    char* send_buf = NULL;
    uint32_t send_buff_size = 0;
    switch(psocket_context->status)
    {
        case READING_HEAD:
            // 有了 watermark的保证，仅当缓冲区有xhead_t的时候才被调用。
            read_len = bufferevent_read (bev, &src_head, sizeof(xhead_t)); 
            MySuicideAssert(read_len == sizeof(xhead_t));
            //MySuicideAssert(psocket_context->src_head.detail_len > 0);
            if (psocket_context->src_head.detail_len > 0)
            {
                psocket_context->content = (char*)malloc(psocket_context->src_head.detail_len + 1);
                psocket_context->content[psocket_context->src_head.detail_len] = '\0';
                psocket_context->to_read_len = psocket_context->src_head.detail_len;
                psocket_context->has_read_len = 0;
                //bufferevent_setwatermark(bev, EV_READ, psocket_context->src_head.detail_len, psocket_context->src_head.detail_len);
                // 重置watermark
                bufferevent_setwatermark(bev, EV_READ, 0, psocket_context->src_head.detail_len);
                ROUTN("log_id[%u] read %s head done, to_read_len: %d has_read_len: %d", psocket_context->src_head.log_id,
                        psocket_context->src_head.srvname, psocket_context->to_read_len, psocket_context->has_read_len);
            }
            else
            {
                ROUTN("log_id[%u] read head done. detail_len zero length. status[%u]", 
                        psocket_context->src_head.log_id, psocket_context->src_head.status);
            }
            psocket_context->status = READING_CONTENT;
            // 故意注释break, 让其继续进入读取content的状态
            // break;
        case READING_CONTENT:
            // 这里要考虑阻塞的情况，未必能一次读完的。
            if (psocket_context->src_head.detail_len > 0)
            {
                read_len = bufferevent_read (bev,
                        &(psocket_context->content[psocket_context->has_read_len]),
                        psocket_context->to_read_len);
                psocket_context->to_read_len -= read_len;
                psocket_context->has_read_len += read_len;
                DEBUG("read_len: %d has_read_len: %d to_read_len: %d",
                        read_len, psocket_context->has_read_len, psocket_context->to_read_len);
                if (psocket_context->to_read_len == 0)
                {
                    psocket_context->status = READ_CONTENT_DONE;
                    psocket_context->content[psocket_context->src_head.detail_len] = '\0';
                    bufferevent_enable(bev, EV_WRITE);
                    //DEBUG("[%s]", psocket_context->content);
                    // 无需break，直接进入 READ_CONTENT_DONE
                }
                else
                {
                    break;
                }
            }
            else
            {
                DEBUG("reading content. detail_len zero length");
                psocket_context->status = READ_CONTENT_DONE;
            }
        case READ_CONTENT_DONE:
            psocket_context->ret_code = OK;

            if (CRAWL_URL_LIST == psocket_context->src_head.status)
            {
                if (psocket_context->src_head.detail_len == 0)
                {
                    ALARM("log_id[%u] jsonstr zero-length.", psocket_context->src_head.log_id);
                    psocket_context->ret_code = JSON_ZERO_LENGTH;
                    goto exit_gate;
                }
                if (! reader.parse(psocket_context->content, root))
                {
                    ALARM("jsonstr parse error. [%s]", psocket_context->content);
                    // 是否需要通知该socket是异常，需要关闭？
                    psocket_context->ret_code = JSON_PARSE_ERROR;
                    goto exit_gate;
                }
                if (!root.isArray())
                {
                    ALARM("jsonstr format error. isArray[%u]", root.isArray());
                    // 是否需要通知该socket是异常，需要关闭？
                    psocket_context->ret_code = JSON_FORMAT_ERROR;
                    goto exit_gate;
                }
                // 调用处理函数
                for (uint32_t i=0; i<root.size(); i++)
                {
                    Json::Value link_item = root[i];
                    link_info_t* plink_info = link_info_new();
                    if (! link_item.isObject())
                    {
                        ALARM("Json Value is NOT a object. index[%u], skip", i);
                        link_info_free(plink_info);
                        continue;
                    }
                    if (false == process_link_item(link_item, plink_info))
                    {
                        link_info_free(plink_info);
                        ALARM("process_link_item error. index[%u]", i);
                    }
                    else
                    {
                        uint32_t put_ret = host_links::getInstance()->put_link(plink_info);
                        if (host_links::PUT_OK != put_ret)
                        {
                            ALARM("ret[%u] link_item-> url[%s] host[%s] path[%s] refer[%s] url_no[%u] shard_no[%u]",
                                    put_ret,
                                    plink_info->url,
                                    plink_info->host,
                                    plink_info->path,
                                    plink_info->refer == NULL ? "-":plink_info->refer,
                                    plink_info->url_no,
                                    plink_info->shard_no);
                            link_info_free(plink_info);
                        }
                        else
                        {
                            process_ok_count++;
                            DEBUG("link_item-> url[%s] host[%s] path[%s] refer[%s] url_no[%u] shard_no[%u]",
                                    plink_info->url,
                                    plink_info->host,
                                    plink_info->path,
                                    plink_info->refer == NULL ? "-":plink_info->refer,
                                    plink_info->url_no,
                                    plink_info->shard_no);
                        }
                    }
                }

                ROUTN("process all[%u] err[%u]", root.size(), root.size()-process_ok_count);
                Json::Value process_stat;
                process_stat["all_num"] = root.size();
                process_stat["err_num"] = root.size()-process_ok_count;
                Json::FastWriter fwriter;
                string jsonstr = fwriter.write(process_stat);
                send_buf = (char*)malloc(jsonstr.length());
                MySuicideAssert(NULL != send_buf);
                send_buff_size = jsonstr.length();
                memcpy(send_buf, jsonstr.c_str(), jsonstr.length());
            }
            // 把统计信息写回到链接选取器(selector)中
            else if (CRAWL_STAT == psocket_context->src_head.status)
            {
                DEBUG("prepare stat data for selector.");
                map<string, crawl_stat_t> stat_map;
                host_links::getInstance()->get_stat(stat_map);
                map<string, crawl_stat_t>:: iterator it_stat;

                Json::Value stat_root;
                for (it_stat = stat_map.begin(); it_stat != stat_map.end(); it_stat++)
                {
                    Json::Value item;
                    item["queue_length"]         = it_stat->second.queue_length;
                    item["crawling_number"]      = it_stat->second.crawling_number;
                    item["max_crawling_number"]  = it_stat->second.max_crawling_number;
                    item["pick_url_interval_ms"] = it_stat->second.pick_url_interval_ms;
                    stat_root[it_stat->first] = item;
                }
                Json::FastWriter fwriter;
                string jsonstr = fwriter.write(stat_root);
                send_buf = (char*)malloc(jsonstr.length());
                MySuicideAssert(NULL != send_buf);
                send_buff_size = jsonstr.length();
                memcpy(send_buf, jsonstr.c_str(), jsonstr.length());
                DEBUG("finish stat data for selector.");
            }

            break;
        default:
            MySuicideAssert(0);
    }

exit_gate:
    if (psocket_context->status == READ_CONTENT_DONE)
    {
        memset(&dst_head, 0, sizeof(dst_head));
        dst_head.log_id = psocket_context->src_head.log_id;
        snprintf(dst_head.srvname, sizeof(dst_head.srvname), "%s", PROJNAME);
        dst_head.detail_len = send_buff_size;
        dst_head.status = psocket_context->ret_code;
        ret = bufferevent_write(bev, &dst_head, sizeof(dst_head));
        int dret = 0;
        if (send_buf != NULL)
        {
            dret = bufferevent_write(bev, send_buf, send_buff_size);
            free(send_buf);
            send_buf = NULL;
            send_buff_size = 0;
        }
        DEBUG("log_id[%u] status[%u] ret[%d] dret[%d] write xhead done.",
                dst_head.log_id, psocket_context->src_head.status, ret, dret);
        psocket_context->status = READING_HEAD;
        bufferevent_setwatermark(bev, EV_READ, sizeof(xhead_t), Config::getInstance()->ReadBufferHighWaterMark());
        //bufferevent_flush(bev, EV_WRITE, BEV_NORMAL);
    }

    //if (OK != psocket_context->ret_code)
    //{
    //ALARM("error happened, code: %u, cleanup.", psocket_context->ret_code);
    // 可以设计个超时，来解决错误链接长期得不到释放的问题，不过在现实网络中，这个不必担心
    //bufferevent_free(bev);
    //socket_context_free((socket_context_t*)ctx);
    //}
}

    static void
main_event_cb(struct bufferevent *bev, short events, void *ctx)
{
    MySuicideAssert(ctx != NULL);

    //if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR | BEV_EVENT_READING | BEV_EVENT_WRITING))
    ROUTN("bufferevent shutdown. events: 0x%x", events);
    bufferevent_free(bev);
    socket_context_free((socket_context_t*)ctx);
}

    static void
accept_conn_cb(struct evconnlistener *listener,
        evutil_socket_t fd, struct sockaddr *address, int socklen,
        void *ctx)
{
    MySuicideAssert(ctx == NULL);
    MySuicideAssert(address != NULL);
    MySuicideAssert(0 != socklen);
    /* We got a new connection! Set up a bufferevent for it. */
    struct event_base *base = evconnlistener_get_base(listener);
    struct bufferevent *bev = bufferevent_socket_new(
            base, fd, BEV_OPT_CLOSE_ON_FREE|BEV_OPT_DEFER_CALLBACKS);
    bufferevent_setwatermark(bev, EV_READ, sizeof(xhead_t), Config::getInstance()->ReadBufferHighWaterMark());

    socket_context_t* psocket_context = socket_context_new();
    bufferevent_setcb(bev, main_process_cb, NULL, main_event_cb, psocket_context);

    bufferevent_enable (bev, EV_READ|EV_WRITE);
    //bufferevent_disable(bev, EV_WRITE);
    DEBUG("got new connection");
}

    static void
accept_error_cb(struct evconnlistener *listener, void *ctx)
{
    MySuicideAssert(ctx == NULL);
    struct event_base *base = evconnlistener_get_base(listener);
    int err = EVUTIL_SOCKET_ERROR();
    FATAL("Got an error %d (%s) on the listener. "
            "Shutting down.\n", err, evutil_socket_error_to_string(err));

    event_base_loopexit(base, NULL);
}

    static void
timeout_cb(int fd, short _event, void *arg)
{
    MySuicideAssert((fd != 0) || (fd == 0));
    MySuicideAssert(_event == EV_TIMEOUT);
    timer_context_t* timer_context = (timer_context_t*)arg;

    crawl_stat_t crawl_stat = host_links::getInstance()->get_stat(timer_context->host);
    struct timeval tv;
    evutil_timerclear(&tv);
    tv.tv_sec = crawl_stat.pick_url_interval_ms/1000;
    tv.tv_usec = 1000*(crawl_stat.pick_url_interval_ms%1000);
    event_add(timer_context->ev, &tv);

    // 检查是否需要dns更新
    uint32_t time_now = (uint32_t) time(NULL);
    if (time_now > (timer_context->last_dns_update+Config::getInstance()->DnsUpdateInterval()))
    {
        make_dns_query(timer_context->dnsbase, timer_context);
    }

    // round robin的方式通知crawling_thread
    // 负载控制，需要控制某个站点同时的抓取数
    // 当定期唤醒后，发现crawling_number并没有减少的话，则需要减速抓取
    uint32_t& round_robin_step = *(timer_context->round_robin_step);
    uint32_t cur_thread_no = round_robin_step % timer_context->thread_number;
    round_robin_step++;
    uint8_t host_length = strlen(timer_context->host);
    int32_t notify_fd = timer_context->thread_context_list[cur_thread_no].notify_fd[0];
    // 在异步编程里，这么做是危险的
    MySuicideAssert(sizeof(host_length) == write(notify_fd, &host_length, sizeof(host_length)));
    MySuicideAssert(host_length == write(notify_fd, timer_context->host, host_length));
    DEBUG("thread_no[%u] %u host[%s] interval[%ums]",
            cur_thread_no,
            round_robin_step,
            timer_context->host,
            crawl_stat.pick_url_interval_ms
         );
}

void sigexit_cb(int signo, short events, void* arg)
{
    MySuicideAssert(signo == SIGINT || signo == SIGTERM);
    MySuicideAssert(events & EV_SIGNAL);
    MySuicideAssert(NULL != arg);
    sigint_context_t* psigint_context = (sigint_context_t*)arg;
    ROUTN("sig[%u] happened.", signo);
    // 停止listener
    evconnlistener_disable(psigint_context->listener);
    // 删除timeout定时器，不再向crawling_thread发送任务
    for (uint32_t i=0; i<psigint_context->evtimeout_list_size; i++)
    {
        evtimer_del(&psigint_context->evtimeout_list[i]);
    }
    // host_links->dump_into_file()完成保存
    host_links::getInstance()->dump_into_file();
    // 等待host_links中的total_crawing_number降为0
    uint32_t last_count = 10;
    while(1)
    {
        uint32_t total_crawling_number = host_links::getInstance()->get_total_crawling_number();
        // 当剩下最后10个抓取任务的时候，最多给他们10s的时间。
        if (total_crawling_number < 10)
        {
            if (last_count == 0 || total_crawling_number == 0)
            {
                ROUTN("exit in sigexit_cb. left_crawling_number: %u", total_crawling_number);
                sleep(1);
                exit(0);
            }
            else
            {
                last_count --;
            }
        }
        ROUTN("total_crawling_number: %u", total_crawling_number);
        sleep(1);
    }
    // 退出
}

    int
main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    // 设置进程打开句柄限制
    //struct rlimit nofile_rc;
    //getrlimit(RLIMIT_STACK, &nofile_rc);
    //nofile_rc.rlim_cur = 4096;
    //nofile_rc.rlim_max = 4096;
    //MySuicideAssert(0 == setrlimit(RLIMIT_NOFILE, &nofile_rc));

    const char * strConfigPath = "./conf/"PROJNAME".config.json";
    char configPath[128];

    char c = '\0';
    while ((c = getopt(argc, argv, "c:vh?")) != -1)
    {
        switch (c)
        {
            case 'c':
                snprintf(configPath, sizeof(configPath), "%s", optarg);
                strConfigPath = configPath;
                break;
            case 'v':
                PrintVersion();
                return 0;
            case 'h':
            case '?':
                PrintHelp();
                return 0;
            default:
                break;
        }
    }

    Config* myConfig = Config::getInstance();
    myConfig->init(strConfigPath);

    ROUTN("-----------------------------------------");
    ROUTN("using config: %s", strConfigPath);

    host_links::getInstance()->init(myConfig->HostVisitIntervalConfigPath());

    dns_cache::getInstance()->load(Config::getInstance()->DnsCachePath());

    struct event_base *base = NULL;
    struct evconnlistener *listener = NULL;
    struct sockaddr_in sin;

    base = event_base_new();
    MySuicideAssert(NULL != base);
    struct evdns_base *dnsbase = evdns_base_new(base, 1);
    MySuicideAssert(NULL != dnsbase);

    // 启动多个worker_thread
    thread_context_t* thread_context_list = new thread_context_t[myConfig->WorkerThreadNum()];
    pthread_t* thread_id = new pthread_t[myConfig->WorkerThreadNum()];
    for (uint32_t i=0; i<myConfig->WorkerThreadNum(); i++)
    {
        thread_context_list[i].thread_no = i;
        MySuicideAssert(0 == socketpair(AF_LOCAL, SOCK_STREAM, 0, thread_context_list[i].notify_fd));
        MySuicideAssert(0 == pthread_create(&thread_id[i], NULL, crawling_thread, &thread_context_list[i]));
    }

    pthread_t dump2disk_thread_id;
    MySuicideAssert(0 == pthread_create(&dump2disk_thread_id, NULL, dump2disk_thread, NULL));
    // 设置各个host的抓取闹钟
    map<string, crawl_stat_t> stat_map;
    host_links::getInstance()->get_stat(stat_map);
    map<string, crawl_stat_t>:: iterator it_stat;
    event* evtimeout_list = (event*)calloc(sizeof(event), stat_map.size());
    MySuicideAssert(evtimeout_list != NULL);
    uint32_t step = 0;
    uint32_t round_robin_step = 0;
    for (it_stat = stat_map.begin(); it_stat != stat_map.end(); it_stat++)
    {
        timer_context_t* timer_context = timer_context_new(
                it_stat->first.c_str(),
                thread_context_list,
                Config::getInstance()->DnsUpdateInterval(),
                dnsbase);

        timer_context->round_robin_step = &round_robin_step;
        timer_context->thread_number = myConfig->WorkerThreadNum();
        timer_context->ev = &evtimeout_list[step];
        evtimer_set(&evtimeout_list[step], timeout_cb, timer_context);
        event_base_set(base, &evtimeout_list[step]);
        struct timeval tv;
        evutil_timerclear(&tv);
        tv.tv_sec = it_stat->second.pick_url_interval_ms/1000;
        tv.tv_usec = 1000*(it_stat->second.pick_url_interval_ms%1000);
        event_add(&evtimeout_list[step], &tv);
        step++;
        ROUTN("host[%s] pick_url_interval_ms[%u]", it_stat->first.c_str(), it_stat->second.pick_url_interval_ms);
    }

    // 设置监听服务
    /* Clear the sockaddr before using it, in case there are extra
     * platform-specific fields that can mess us up. */
    memset(&sin, 0, sizeof(sin));
    /* This is an INET address */
    sin.sin_family = AF_INET;
    /* Listen on 0.0.0.0 */
    sin.sin_addr.s_addr = htonl(0);
    /* Listen on the given port. */
    sin.sin_port = htons(myConfig->QueryPort());

    /* 似乎不支持 LEV_OPT_DEFERRED_ACCEPT */
    listener = evconnlistener_new_bind(base, accept_conn_cb, NULL,
            LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, -1,
            (struct sockaddr*)&sin, sizeof(sin));
    MySuicideAssert(NULL != listener);
    evconnlistener_set_error_cb(listener, accept_error_cb);

    // 拦截INT信号
    sigint_context_t* psigint_context = (sigint_context_t*)malloc(sizeof(sigint_context_t));
    psigint_context->listener       = listener;
    psigint_context->evtimeout_list = evtimeout_list;
    psigint_context->evtimeout_list_size = stat_map.size();
    event* sigint_ev  = evsignal_new(base, SIGINT,  sigexit_cb, psigint_context);
    event* sigterm_ev = evsignal_new(base, SIGTERM, sigexit_cb, psigint_context);
    MySuicideAssert( 0 == evsignal_add(sigint_ev, NULL) );
    MySuicideAssert( 0 == evsignal_add(sigterm_ev, NULL) );

    event_base_dispatch(base);

    for (uint32_t i=0; i<myConfig->WorkerThreadNum(); i++)
    {
        pthread_join(thread_id[i], NULL);
    }

    pthread_join(dump2disk_thread_id, NULL);

    evconnlistener_free(listener);
    event_base_free(base);
    evdns_base_free(dnsbase, 0);
    free(psigint_context);

    ROUTN("%s exit normally.", PROJNAME);

    return 0;
}
