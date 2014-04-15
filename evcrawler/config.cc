#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string>
#include <string.h>
#include <map>
#include <json/json.h>
#include <iostream>
#include <fstream>
#include <sys/types.h>
#include "mylog.h"
#include "MyException.h"
#include "config.h"

using namespace std;

const char* const Config::m_StrLogLevel        = "LogLevel";
const char* const Config::m_StrQueryPort       = "QueryPort";
const char* const Config::m_StrWorkerThreadNum = "WorkerThreadNum";
const char* const Config::m_StrReadBufferHighWaterMark = "ReadBufferHighWaterMark";
const char* const Config::m_StrHostVisitIntervalConfigPath = "HostVisitIntervalConfigPath";
const char* const Config::m_StrDnsUpdateInterval = "DnsUpdateInterval";
const char* const Config::m_StrDnsCachePath      = "DnsCachePath";
const char* const Config::m_StrUrlSavePath       = "UrlSavePath";
const char* const Config::m_ProxyIP = "IP";
const char* const Config::m_ProxyPort = "Port";
const char* const Config::m_ProxyEnabled = "ProxyEnabled";

Config* Config::m_static_config = new Config();

Config* Config::getInstance()
{
    return m_static_config;
}

Config::Config(){}

void Config::init(const char* configpath)
{
    // DEFAULT CONFIG
    m_service_shutdown = false;

    m_log_level = mylog :: ROUTN;
    m_query_port         = 2006;
    m_worker_thread_num  = 10;
    m_read_buffer_high_watermark  = 1024*1024;
    m_dns_update_interval  = 3600; // 默认1小时更新一次
    memset(m_host_visit_interval_config_path, 0, sizeof(m_host_visit_interval_config_path));

    snprintf(m_dump_data_dir,  sizeof(m_dump_data_dir), "%s", "./data/");
    snprintf(m_dump_data_name, sizeof(m_dump_data_name), "%s", "page_info");
    snprintf(m_url_save_path,  sizeof(m_url_save_path), "%s", "./data/url_save.json");

    m_proxy_enabled = false;
    snprintf(m_proxy_ip, sizeof(m_proxy_ip), "%s", "");
    m_proxy_port = 0;

    // CUSTOMIZE CONFIG
    Json::Value root;
    Json::Reader reader;
    ifstream in(configpath);
    if (! reader.parse(in, root))
    {
        MySuicideAssert(! reader.parse(in, root));
    }

    if (root.isMember("LOG")) {
        Json::Value logConfig = root["LOG"];
        if (logConfig.isMember(m_StrLogLevel) && logConfig[m_StrLogLevel].isInt())
        {
            m_log_level = logConfig[m_StrLogLevel].asUInt();
        }
    }
    SETLOG(m_log_level, PROJNAME);

    if (root.isMember("QUERY_SERVER")) {
        Json::Value qSrvConfig = root["QUERY_SERVER"];

        if (qSrvConfig.isMember(m_StrQueryPort) && qSrvConfig[m_StrQueryPort].isInt())
        {
            m_query_port = qSrvConfig[m_StrQueryPort].asUInt();
        }

        if (qSrvConfig.isMember(m_StrWorkerThreadNum) && qSrvConfig[m_StrWorkerThreadNum].isInt())
        {
            m_worker_thread_num = qSrvConfig[m_StrWorkerThreadNum].asUInt();
        }

        if (qSrvConfig.isMember(m_StrReadBufferHighWaterMark) && qSrvConfig[m_StrReadBufferHighWaterMark].isInt())
        {
            m_read_buffer_high_watermark = qSrvConfig[m_StrReadBufferHighWaterMark].asUInt();
        }

        if (qSrvConfig.isMember(m_StrDnsUpdateInterval) && qSrvConfig[m_StrDnsUpdateInterval].isInt())
        {
            m_dns_update_interval = qSrvConfig[m_StrDnsUpdateInterval].asUInt();
        }

        if (qSrvConfig.isMember(m_StrDnsCachePath) && qSrvConfig[m_StrDnsCachePath].isString())
        {
            snprintf(m_dns_cache_path, sizeof(m_dns_cache_path), "%s", qSrvConfig[m_StrDnsCachePath].asCString());
        }

        if (qSrvConfig.isMember(m_StrUrlSavePath) && qSrvConfig[m_StrUrlSavePath].isString())
        {
            snprintf(m_url_save_path, sizeof(m_url_save_path), "%s", qSrvConfig[m_StrUrlSavePath].asCString());
        }

        if (qSrvConfig.isMember(m_StrHostVisitIntervalConfigPath) && qSrvConfig[m_StrHostVisitIntervalConfigPath].isString())
        {
            snprintf(m_host_visit_interval_config_path, sizeof(m_host_visit_interval_config_path),
                    "%s", qSrvConfig[m_StrHostVisitIntervalConfigPath].asCString());
        }
    }
    ROUTN("set %s = %u.", m_StrLogLevel,                      m_log_level);
    ROUTN("set %s = %u.", m_StrQueryPort,                     m_query_port);
    ROUTN("set %s = %u.", m_StrWorkerThreadNum,               m_worker_thread_num);
    ROUTN("set %s = %u.", m_StrReadBufferHighWaterMark,       m_read_buffer_high_watermark);
    ROUTN("set %s = %u.", m_StrDnsUpdateInterval,             m_dns_update_interval);
    ROUTN("set %s = %s.", m_StrDnsCachePath,                  m_dns_cache_path);
    ROUTN("set %s = %s.", m_StrHostVisitIntervalConfigPath,   m_host_visit_interval_config_path);
    ROUTN("set %s = %s.", m_StrUrlSavePath,                   m_url_save_path);
    
    if (root.isMember("PROXY"))
    {
        Json::Value qProxyConfig = root["PROXY"];
        if (qProxyConfig.isMember(m_ProxyIP) && qProxyConfig[m_ProxyIP].isString())
        {
            snprintf(m_proxy_ip, sizeof(m_proxy_ip), "%s", qProxyConfig[m_ProxyIP].asCString());
        }
        
        if (qProxyConfig.isMember(m_ProxyPort) && qProxyConfig[m_ProxyPort].isInt())
        {
            m_proxy_port = qProxyConfig[m_ProxyPort].asInt();
        }
        
        if (qProxyConfig.isMember(m_ProxyEnabled) && qProxyConfig[m_ProxyEnabled].isInt())
        {
            m_proxy_enabled = qProxyConfig[m_ProxyEnabled].asInt() == 1;
        }
    }
    ROUTN("set %s = %u.", m_ProxyEnabled,   m_proxy_enabled);
    ROUTN("set %s = %s.", m_ProxyIP,        m_proxy_ip);
    ROUTN("set %s = %u.", m_ProxyPort,      m_proxy_port);
    
}
