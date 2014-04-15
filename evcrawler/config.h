#ifndef __CONFIG_H__
#define __CONFIG_H__
#include <stdint.h>

class Config
{
	private:
		static const char* const m_StrLogLevel;

		static const char* const m_StrQueryPort;
		static const char* const m_StrWorkerThreadNum;
		static const char* const m_StrReadBufferHighWaterMark;
		static const char* const m_StrHostVisitIntervalConfigPath;
		static const char* const m_StrDnsUpdateInterval;
		static const char* const m_StrDnsCachePath;
		static const char* const m_StrUrlSavePath;
        static const char* const m_ProxyEnabled;
        static const char* const m_ProxyIP;
        static const char* const m_ProxyPort;

		// log config
		uint32_t  m_log_level; // PRINT, DEBUG, ROUTN, ALARM, FATAL

        // query server config
		uint16_t m_query_port;
		uint32_t m_worker_thread_num;
		uint32_t m_read_buffer_high_watermark;
		uint32_t m_dns_update_interval;
		char     m_dns_cache_path[128];
		char     m_host_visit_interval_config_path[128];
		char     m_dump_data_dir[128];
		char     m_dump_data_name[128];
		char     m_url_save_path[128];
		bool     m_service_shutdown;
        bool     m_proxy_enabled;
        char     m_proxy_ip[16];
        uint32_t m_proxy_port;

		Config();
		Config(const Config&);

        static Config* m_static_config;
	public:
        static Config* getInstance();
		void init(const char* configpath);
		~Config(){}

        void     SetServiceShutDown(bool shut_down) { m_service_shutdown = shut_down; }
        bool     IsServiceShutDown() { return m_service_shutdown; }
		uint16_t QueryPort() const { return m_query_port; }
		uint32_t WorkerThreadNum() const { return m_worker_thread_num; }
        uint32_t ReadBufferHighWaterMark() { return m_read_buffer_high_watermark; }
        uint32_t DnsUpdateInterval() { return m_dns_update_interval; }
        const char* DnsCachePath() { return m_dns_cache_path; }
        const char* HostVisitIntervalConfigPath() { return m_host_visit_interval_config_path; }
        const char* DumpDataDir() { return m_dump_data_dir; }
        const char* DumpDataName() { return m_dump_data_name; }
        const char* UrlSavePath() { return m_url_save_path; }
        bool IsProxyEnabled() { return m_proxy_enabled; }
        const char* ProxyIP() const { return m_proxy_ip; }
        uint32_t ProxyPort() { return m_proxy_port;}
};

#endif
