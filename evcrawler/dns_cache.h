#ifndef __DNS_CACHE_H__
#define __DNS_CACHE_H__
#include <pthread.h>
#include <vector>
#include <string>
#include <map>

using namespace std;

class ip_list_manager
{
    public:
        uint32_t        m_step;
        vector<string>  ip_list;
    public:
        ip_list_manager(){m_step = 0;}
        ~ip_list_manager(){}
};

class dns_cache
{
    private:
        map<string, ip_list_manager > m_dns_cache_pool;

        pthread_rwlock_t m_rwlock;
        string m_dns_cache_path;
        uint32_t m_last_save_timestmp;

        static dns_cache* m_static_dns_cache;
        dns_cache();
        bool save();
    public:
        static dns_cache* getInstance();
        ~dns_cache();
        void update(const string& host, const vector<string>&);
        string search(const string& host);
        bool is_valid_ipv4_address (const char *str);
        bool load(const char* dns_cache_path);
};

#endif
