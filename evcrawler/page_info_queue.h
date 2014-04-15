#ifndef __PAGE_INFO_QUEUE_H__
#define __PAGE_INFO_QUEUE_H__

#include <pthread.h>
#include <queue>
#include <json/json.h>

using namespace std;

#include "evcrawler_struct.h"

class page_info_t
{
    public:
        Json::Value m_meta;
        char*       m_content;
        uint32_t    m_content_size;
    public:
        page_info_t();
        ~page_info_t();
};

class page_info_queue
{
    private:
        queue<page_info_t*> m_page_info_queue;

        pthread_mutex_t m_lock;
        pthread_cond_t  m_cond;
        page_info_queue();
        static page_info_queue* m_static_page_info_queue;
    public:
        static page_info_queue* getInstance();
        ~page_info_queue();
        void put(page_info_t* page_info);
        page_info_t* get(uint32_t& qlen);
};

#endif
