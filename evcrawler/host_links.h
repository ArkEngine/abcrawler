#ifndef __HOST_LINKS__
#define __HOST_LINKS__

#include <map>
#include <queue>
#include <set>
#include <string>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

#include "evcrawler_struct.h"

using namespace std;

struct link_info_t;
struct crawl_stat_t;

class host_links
{
    private:
        static const uint32_t MIN_PICK_URL_INTERVAL_MS = 50;
        static const uint32_t MAX_PICK_URL_INTERVAL_MS = 2000;
        map<string, queue<link_info_t*> >  m_host2links;
        map<string, crawl_stat_t> m_stat_map;
        set<uint64_t> m_url_set;

        string m_url_save_path;

        uint32_t m_total_crawling_number;

        pthread_mutex_t m_set_lock;
        pthread_mutex_t m_link_lock;
        pthread_mutex_t m_stat_lock;
        host_links(const host_links&);
        host_links();

        static host_links* m_static_host_links;

        bool m_freeze;
    public:
        static host_links* getInstance();
        enum { CRAWL_OK = 0, CRAWL_ERROR,};
        enum { PUT_OK = 0, FREEZE, HOST_UNKNOWN, ALREADY_IN};
        void init(const char*);
        ~host_links();
        /*
         * ** get_link **
         * -1- 在host对应的队列中，获取一个链接，准备用于抓取。
         * -2- 更新host对应的crawl_stat_t, crawling_number +1, 更新queue_length
         */
        link_info_t* get_link(const string& host);
        /*
         * ** put_link **
         * -1- 在host对应的队列中，增加一个链接，准备用于抓取。
         * -2- 更新host对应的crawl_stat_t, 更新queue_length
         */
        uint32_t put_link(const link_info_t* plink_info);
        /*
         * ** link_finish_crawl **
         * -1- 更新host对应的crawl_stat_t，crawling_number -1
         */
        void link_finish_crawl(const link_info_t* plink_info, bool status );
        crawl_stat_t get_stat(const string& host);
        void get_stat(map<string, crawl_stat_t>& stat_map);
        void freeze();
        bool is_freeze();
        void log_stat();

        void dump_into_file();
        bool load_from_file();

        uint32_t get_total_crawling_number() { return m_total_crawling_number; }
};
#endif
