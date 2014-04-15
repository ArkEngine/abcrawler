#include <json/json.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "mylog.h"
#include "creat_sign.h"

#include "host_links.h"
#include "MyException.h"
#include "config.h"

host_links* host_links::m_static_host_links = new host_links();

host_links* host_links::getInstance()
{
    return m_static_host_links;
}

host_links::host_links()
{
    pthread_mutex_init(&m_link_lock, NULL);
    pthread_mutex_init(&m_stat_lock, NULL);
    pthread_mutex_init(&m_set_lock,  NULL);

    m_total_crawling_number = 0;
    m_freeze = false;
}

void host_links::init(const char* visit_interval_config_path)
{
    Json::Value root;
    Json::Reader reader;
    ifstream in(visit_interval_config_path);
    MySuicideAssert (reader.parse(in, root));

    m_url_save_path = Config::getInstance()->UrlSavePath();

    //uint32_t pick_url_interval_ms = root["pick_url_interval_ms"].asUInt();
    //pick_url_interval_ms = pick_url_interval_ms > MIN_PICK_URL_INTERVAL_MS ? pick_url_interval_ms : MIN_PICK_URL_INTERVAL_MS;
    //pick_url_interval_ms = pick_url_interval_ms < MAX_PICK_URL_INTERVAL_MS ? pick_url_interval_ms : MAX_PICK_URL_INTERVAL_MS;

    // 初始化统计结构
    Json::Value::Members keynames = root["host_crawling_control"].getMemberNames();
    MySuicideAssert(keynames.size() > 0);
    for (uint32_t i = 0; i < keynames.size(); i++)
    {
        string key = keynames[i];
        crawl_stat_t crawl_stat;
        memset(&crawl_stat, 0, sizeof(crawl_stat_t));
        // 设置域名的访问间隔
        crawl_stat.max_crawling_number  = root["host_crawling_control"][key]["max_crawling_number"].asUInt();
        uint32_t crawling_number_by_minute = root["host_crawling_control"][key]["crawling_number_by_minute"].asUInt();
        MySuicideAssert(crawling_number_by_minute > 0);
        uint32_t pick_url_interval_ms = (uint32_t)(1000/(crawling_number_by_minute/60.0));
        pick_url_interval_ms = pick_url_interval_ms > MIN_PICK_URL_INTERVAL_MS ? pick_url_interval_ms : MIN_PICK_URL_INTERVAL_MS;
        pick_url_interval_ms = pick_url_interval_ms < MAX_PICK_URL_INTERVAL_MS ? pick_url_interval_ms : MAX_PICK_URL_INTERVAL_MS;
        crawl_stat.pick_url_interval_ms = pick_url_interval_ms;
        crawl_stat.max_crawling_number_in_minute = crawling_number_by_minute;
        crawl_stat.crawling_number_in_minute = 0;
        crawl_stat.last_minute = (uint32_t)time(NULL)/60;
        DEBUG("host[%s] max_crawling_number[%u] pick_url_interval_ms[%u]",
                key.c_str(), crawl_stat.max_crawling_number, pick_url_interval_ms);
        if (crawl_stat.max_crawling_number <= 0)
        {
            crawl_stat.max_crawling_number   = 1;
        }
        m_stat_map[key] = crawl_stat;
    }
    // 尝试load上次未完成的任务
    load_from_file();
}

host_links::~host_links()
{
}

link_info_t* host_links::get_link(const string& host)
{
    if (m_freeze)
    {
        return NULL;
    }

    link_info_t* plink_info = NULL;

    pthread_mutex_lock  (&m_stat_lock);
    crawl_stat_t& crawl_stat_pre = m_stat_map[host];
    pthread_mutex_unlock(&m_stat_lock);
    // TODO 加上对每分钟抓取数目的控制
    if (crawl_stat_pre.crawling_number >= crawl_stat_pre.max_crawling_number)
    {
        ROUTN("host[%s] need slow down, queue_length[%u] crawling_number[%u] max_crawling_num[%u]",
                host.c_str(), crawl_stat_pre.queue_length, crawl_stat_pre.crawling_number, crawl_stat_pre.max_crawling_number);
        return NULL;
    }
    if(((uint32_t)time(NULL)/60) == crawl_stat_pre.last_minute)
    {
        if (crawl_stat_pre.crawling_number_in_minute >= crawl_stat_pre.max_crawling_number_in_minute)
        {
            ROUTN("host[%s] need slow down, crawling_number_in_minute[%u] max[%u]",
                    host.c_str(), crawl_stat_pre.crawling_number_in_minute, crawl_stat_pre.max_crawling_number_in_minute);
            return NULL;
        }
    }
    
    pthread_mutex_lock  (&m_link_lock);
    if (m_host2links.end() != m_host2links.find(host))
    {
        if (m_host2links[host].size() > 0)
        {
            plink_info = m_host2links[host].front();
            m_host2links[host].pop();
        }
    }
    pthread_mutex_unlock(&m_link_lock);

    // 更新统计结构
    if (NULL != plink_info)
    {
        pthread_mutex_lock  (&m_stat_lock);
        // put_link的时候，已经保证m_stat_map中必然含有host这个站点
        MySuicideAssert (m_stat_map.end() != m_stat_map.find(host));
        crawl_stat_t& crawl_stat = m_stat_map[host];
        crawl_stat.crawling_number++;
        crawl_stat.crawling_number_in_minute++;
        crawl_stat.queue_length--;
        m_total_crawling_number ++;

        pthread_mutex_unlock(&m_stat_lock);
    }
    return plink_info;
}

uint32_t host_links::put_link(const link_info_t* plink_info)
{
    if (m_freeze)
    {
        return FREEZE;
    }

    bool got_new_host = false;
    const string& host = plink_info->host;

    if (plink_info->retry_count == 0)
    {
        uint64_t  url_sign_64 = 0;
        uint32_t* psign = (uint32_t*)&url_sign_64;
        creat_sign_64(plink_info->url, strlen(plink_info->url), &psign[0], &psign[1]);

        pthread_mutex_lock(&m_set_lock);
        bool already_in = (m_url_set.end() != m_url_set.find(url_sign_64));
        if (! already_in)
        {
            m_url_set.insert(url_sign_64);
        }
        pthread_mutex_unlock(&m_set_lock);
        if (already_in)
        {
            return ALREADY_IN;
        }
    }

    pthread_mutex_lock  (&m_stat_lock);
    if (m_stat_map.end() != m_stat_map.find(host))
    {
        // 对于配置中不存在的站点不予抓取
        got_new_host = false;
        crawl_stat_t& crawl_stat = m_stat_map[host];
        crawl_stat.queue_length++;
    }
    else
    {
        got_new_host = true;
    }
    pthread_mutex_unlock(&m_stat_lock);

    if (got_new_host)
    {
        return HOST_UNKNOWN;
    }
    else
    {
        pthread_mutex_lock  (&m_link_lock);
        if (m_host2links.end() != m_host2links.find(host))
        {
            m_host2links[host].push(const_cast<link_info_t*>(plink_info));
        }
        else
        {
            queue<link_info_t*> link_q;
            link_q.push(const_cast<link_info_t*>(plink_info));
            m_host2links[host] = link_q;
        }
        pthread_mutex_unlock(&m_link_lock);
        return PUT_OK;
    }

    return PUT_OK;
}

void host_links::link_finish_crawl(const link_info_t* plink_info, bool status )
{
    uint64_t  url_sign_64 = 0;
    uint32_t* psign = (uint32_t*)&url_sign_64;
    creat_sign_64(plink_info->url, strlen(plink_info->url), &psign[0], &psign[1]);
    pthread_mutex_lock(&m_set_lock);
    set<uint64_t>::iterator sit = m_url_set.find(url_sign_64);
    if (sit != m_url_set.end())
    {
        m_url_set.erase(sit);
    }
    pthread_mutex_unlock(&m_set_lock);

    string myhost(plink_info->host);
    pthread_mutex_lock  (&m_stat_lock);
    MySuicideAssert (m_stat_map.end() != m_stat_map.find(myhost));
    crawl_stat_t& crawl_stat = m_stat_map[myhost];
    crawl_stat.crawling_number--;
    m_total_crawling_number --;

    if(((uint32_t)time(NULL)/60) != crawl_stat.last_minute)
    {
        ROUTN("host[%s] crawled [%u] pages in last minute, max[%u]",
                myhost.c_str(), crawl_stat.crawling_number_in_minute, crawl_stat.max_crawling_number_in_minute);
        crawl_stat.crawling_number_in_minute = 0;
        crawl_stat.last_minute = (uint32_t)time(NULL)/60;
    }

    if (status == true)
    {
        crawl_stat.ok_count += 1;
        crawl_stat.last_ok_timestamp = (uint32_t)time(NULL);
        // 已经步入正常轨道
        // 可以用来动态的调整压力，如控制interval和并发数
        if (crawl_stat.ok_count > 10)
        {
            crawl_stat.error_count = 0;
        }
    }
    else
    {
        crawl_stat.error_count += 1;
        crawl_stat.last_error_timestamp = (uint32_t)time(NULL);
        if (crawl_stat.error_count > 10)
        {
            crawl_stat.ok_count = 0;
        }
    }

    pthread_mutex_unlock(&m_stat_lock);
}

void host_links::get_stat(map<string, crawl_stat_t>& stat_map)
{
    pthread_mutex_lock  (&m_stat_lock);
    stat_map = m_stat_map;
    pthread_mutex_unlock(&m_stat_lock);
}

crawl_stat_t host_links::get_stat(const string& host)
{
    pthread_mutex_lock  (&m_stat_lock);
    MySuicideAssert (m_stat_map.end() != m_stat_map.find(host));
    crawl_stat_t crawl_stat = m_stat_map[host];
    pthread_mutex_unlock(&m_stat_lock);
    return crawl_stat;
}

void host_links::log_stat()
{
    pthread_mutex_lock  (&m_stat_lock);
    map<string, crawl_stat_t>::iterator it_stat;
    for (it_stat = m_stat_map.begin(); it_stat != m_stat_map.end(); it_stat++)
    {
        DEBUG("host[%s]->crawling[%u] qlen[%u] vi_ms[%u] ok[%u] ok_tt[%u] error[%u] err_tt[%u]",
                it_stat->first.c_str(),
                it_stat->second.crawling_number,
                it_stat->second.queue_length,
                it_stat->second.pick_url_interval_ms,
                it_stat->second.ok_count,
                it_stat->second.last_ok_timestamp,
                it_stat->second.error_count,
                it_stat->second.last_error_timestamp);
    }
    pthread_mutex_unlock(&m_stat_lock);
}

void host_links::freeze()
{
    m_freeze = true;
}

bool host_links::is_freeze()
{
    return m_freeze == true;
}

void host_links::dump_into_file()
{
    map<string, queue<link_info_t*> >::iterator it;
    Json::Value link_list;
    pthread_mutex_lock  (&m_link_lock);
    for (it=m_host2links.begin(); it!=m_host2links.end(); it++)
    {
        while (it->second.size() > 0)
        {
            link_info_t* plink_info = it->second.front();
            it->second.pop();
            Json::Value link_item;
            link_item[LINK_FIELD_URL]      = plink_info->url;
            link_item[LINK_FIELD_URL_NO]   = plink_info->url_no;
            link_item[LINK_FIELD_HOST]     = plink_info->host;
            link_item[LINK_FIELD_HOST_NO]  = plink_info->host_no;
            link_item[LINK_FIELD_SHARD_NO] = plink_info->shard_no;
            if (NULL != plink_info->refer)
            {
                link_item[LINK_FIELD_REFER] = plink_info->refer;
            }
            link_list.append(link_item);
            link_info_free(plink_info);
        }
    }
    pthread_mutex_unlock  (&m_link_lock);

    Json::FastWriter fwriter;
    string jsonstr = fwriter.write(link_list);
    if (jsonstr[jsonstr.length() - 1] != '\n')
    {
        jsonstr += '\n';
    }

    mode_t amode = (0 == access(m_url_save_path.c_str(), F_OK)) ? O_WRONLY|O_APPEND|O_TRUNC : O_WRONLY|O_CREAT|O_APPEND|O_TRUNC;
    int fd = open(m_url_save_path.c_str(), amode, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    MySuicideAssert (fd != -1);
    const char* buffer2write = jsonstr.c_str();
    uint32_t write_size = jsonstr.length();
    uint32_t has_write_size = 0;
    while(write_size > has_write_size)
    {
        int ret = write(fd, &buffer2write[has_write_size], write_size - has_write_size);
        MySuicideAssert(ret >= 0);
        has_write_size += ret;
    }
    close(fd);
}

bool host_links::load_from_file()
{
    Json::Value root;
    Json::Reader reader;
    if (0 != access(m_url_save_path.c_str(), F_OK))
    {
        ALARM("load url json file FAIL, can't access path[%s]", m_url_save_path.c_str());
        return false;
    }
    ifstream in(m_url_save_path.c_str());
    if (! reader.parse(in, root))
    {
        ALARM("load url json file FAIL, not valid json format. path[%s]", m_url_save_path.c_str());
        return false;
    }

    uint32_t error_count = 0;
    for (uint32_t i=0; i<root.size(); i++)
    {
        link_info_t* plink_info = link_info_new();
        if (false == process_link_item(root[i], plink_info))
        {
            link_info_free(plink_info);
            continue;
        }
        uint32_t put_ret = put_link(plink_info);
        if (PUT_OK != put_ret)
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
            error_count++;
        }
    }

    ROUTN("load url from [%s] all[%u] err[%u]", m_url_save_path.c_str(), root.size(), error_count);

    return true;
}
