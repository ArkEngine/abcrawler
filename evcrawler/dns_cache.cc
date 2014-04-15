#include <unistd.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <fstream>
#include <json/json.h>
#include "mylog.h"
#include "dns_cache.h"
#include "MyException.h"
#include "config.h"

using namespace std;

dns_cache* dns_cache::m_static_dns_cache = new dns_cache();

dns_cache* dns_cache::getInstance()
{
    return m_static_dns_cache;
}

dns_cache::dns_cache()
{
    pthread_rwlock_init(&m_rwlock, NULL);
    m_last_save_timestmp = (uint32_t)time(NULL);
}

dns_cache::~dns_cache(){}

void dns_cache::update(const string& host, const vector<string>& ip_list)
{
    map<string, ip_list_manager >::iterator it;
    pthread_rwlock_wrlock(&m_rwlock);
    it = m_dns_cache_pool.find(host);
    if (it != m_dns_cache_pool.end())
    {
        it->second.ip_list = ip_list;
    }
    else
    {
        ip_list_manager ilm;
        ilm.ip_list = ip_list;
        m_dns_cache_pool[host] = ilm;
    }

    uint32_t time_now = (uint32_t) time(NULL);
    if (time_now > (m_last_save_timestmp + Config::getInstance()->DnsUpdateInterval()))
    {
        if(save())
        {
            ROUTN("save dns cache into [%s] OK", m_dns_cache_path.c_str());
        }
        else
        {
            ALARM("save dns cache into [%s] FAIL", m_dns_cache_path.c_str());
        }
    }
    pthread_rwlock_unlock(&m_rwlock);
}

string dns_cache::search(const string& host)
{
    string ip;
    map<string, ip_list_manager >::iterator it;
    pthread_rwlock_rdlock(&m_rwlock);
    it = m_dns_cache_pool.find(host);
    if (it != m_dns_cache_pool.end())
    {
        it->second.m_step++;
        ip = it->second.ip_list[it->second.m_step % it->second.ip_list.size()];
    }
    pthread_rwlock_unlock(&m_rwlock);
    return ip;
}

bool dns_cache::load(const char* dns_cache_path)
{
    Json::Value root;
    Json::Reader reader;
    if (0 != access(dns_cache_path, F_OK))
    {
        ALARM("load dns_cache json file FAIL. path[%s]", dns_cache_path);
        return false;
    }
    m_dns_cache_path = dns_cache_path;
    ifstream in(m_dns_cache_path.c_str());
    if (! reader.parse(in, root))
    {
        MySuicideAssert(! reader.parse(in, root));
    }

    pthread_rwlock_wrlock(&m_rwlock);
    Json::Value::Members keynames = root.getMemberNames();
    MySuicideAssert(keynames.size() > 0);
    for (uint32_t i = 0; i < keynames.size(); i++)
    {
        string host = keynames[i];
        Json::Value ip_array = root[host];
        MySuicideAssert(ip_array.isArray());
        ip_list_manager ilm;
        for (uint32_t k=0; k<ip_array.size(); k++)
        {
            //printf("host[%s] ip[%s]\n", host.c_str(), ip_array[k].asCString());
            if (!is_valid_ipv4_address(ip_array[k].asCString()))
            {
                ALARM("%s is NOT valid IPv4 address.", ip_array[k].asCString());
            }
            else
            {
                ilm.ip_list.push_back(ip_array[k].asCString());
            }
        }
        m_dns_cache_pool[host] = ilm;
    }
    pthread_rwlock_unlock(&m_rwlock);

    return true;
}

bool dns_cache::save()
{
    if (0 == m_dns_cache_path.length())
    {
        ALARM("dns_cache_path NOT set.");
        return false;
    }
    Json::Value root;
    map<string, ip_list_manager >::iterator it;
    //pthread_rwlock_rdlock(&m_rwlock);
    for (it = m_dns_cache_pool.begin(); it != m_dns_cache_pool.end(); it++)
    {
        if (it->second.ip_list.size() > 0)
        {
            Json::Value ip_array;
            for (uint32_t i=0; i<it->second.ip_list.size(); i++)
            {
                ip_array.append(it->second.ip_list[i].c_str());
            }
            root[it->first] = ip_array;
        }
    }

    //pthread_rwlock_unlock(&m_rwlock);

    string out = root.toStyledString();
    mode_t amode = (0 == access(m_dns_cache_path.c_str(), F_OK)) ? O_WRONLY|O_APPEND|O_TRUNC : O_WRONLY|O_CREAT|O_APPEND|O_TRUNC;
    int fd = open(m_dns_cache_path.c_str(), amode, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    bool ret = true;
    if (fd != -1)
    {
        ret = (out.length() == (uint32_t)write(fd, out.c_str(), out.length()));
    }
    else
    {
        ALARM("write dns cache fail[%s]", m_dns_cache_path.c_str());
        ret = false;
    }

    if(fd != -1)
    {
        close(fd);
    }
    if (ret)
    {
        m_last_save_timestmp = (uint32_t)time(NULL);
    }

    return true;
}

bool dns_cache::is_valid_ipv4_address (const char *str)
{
    bool saw_digit = false;
    int octets = 0;
    int val = 0;

    while (*str != '\0')
    {
        int ch = *str++;

        if (ch >= '0' && ch <= '9')
        {
            val = val * 10 + (ch - '0');

            if (val > 255)
                return false;
            if (!saw_digit)
            {
                if (++octets > 4)
                    return false;
                saw_digit = true;
            }
        }
        else if (ch == '.' && saw_digit)
        {
            if (octets == 4)
                return false;
            val = 0;
            saw_digit = false;
        }
        else
            return false;
    }
    if (octets < 4)
        return false;

    return true;
}
