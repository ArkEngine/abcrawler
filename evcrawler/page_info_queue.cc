#include "page_info_queue.h"
#include "MyException.h"

page_info_t::page_info_t()
{
    m_content = NULL;
    m_content_size = 0;
}
page_info_t::~page_info_t()
{
    free(m_content);
    m_content_size = 0;
}

page_info_queue* page_info_queue::m_static_page_info_queue = new page_info_queue();

page_info_queue* page_info_queue::getInstance()
{
    return m_static_page_info_queue;
}

page_info_queue::page_info_queue()
{
    pthread_mutex_init(&m_lock, NULL);
    pthread_cond_init(&m_cond, NULL);
}

page_info_queue::~page_info_queue(){}

void page_info_queue::put(page_info_t* page_info)
{
    MySuicideAssert(NULL != page_info);
    pthread_mutex_lock(&m_lock);
    m_page_info_queue.push(page_info);
    pthread_cond_signal(&m_cond);
    pthread_mutex_unlock(&m_lock);
}

page_info_t* page_info_queue::get(uint32_t& qlen)
{
    page_info_t* page_info = NULL;
    pthread_mutex_lock(&m_lock);
    while (m_page_info_queue.size() == 0)
    {
        pthread_cond_wait(&m_cond, &m_lock);
    }

    page_info = m_page_info_queue.front();
    m_page_info_queue.pop();
    qlen = m_page_info_queue.size();
    pthread_mutex_unlock(&m_lock);
    return page_info;
}
