#include <stdio.h>
#include <zlib.h>
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

#include <json/json.h>
#include "MyException.h"
#include "filelinkblock.h"
#include "page_info_queue.h"
#include "config.h"
#include "comprezz.h"

using namespace std;

void* dump2disk_thread(void* arg)
{
    MySuicideAssert(arg == NULL);
    MySuicideAssert(sizeof(Bytef) == sizeof(char));

    enum {
        QUEUE_CRAWLER,
    };

    enum {
        EVENT_PAGE_DETAIL,
    };

    page_info_queue* my_page_info_queue = page_info_queue::getInstance();
    filelinkblock* flb = new filelinkblock(Config::getInstance()->DumpDataDir(), Config::getInstance()->DumpDataName(), false);
    MySuicideAssert(flb != NULL);
    Json::FastWriter fwriter;
    const uint32_t DEFAULT_BUFFER_SIZE = 100*1024;
    uint32_t buff_size = DEFAULT_BUFFER_SIZE;
    char* ptmpbuf = (char*) malloc(buff_size);
    MySuicideAssert(NULL != ptmpbuf);
    char* pzipbuf = (char*) malloc(DEFAULT_BUFFER_SIZE);
    MySuicideAssert(NULL != ptmpbuf);

    comprezz zz;

    while(1)
    {
        uint32_t qlen = 0;
        page_info_t* page_info = my_page_info_queue->get(qlen);
        // TODO 当返回值为NULL时，可以优雅的退出
        MySuicideAssert(page_info != NULL);

        bool has_compressed = page_info->m_meta.isMember(HTTP_HEADER_FIELD_CONTENT_ENCODING);
        if (! has_compressed)
        {
            // 先把编码信息写入json中
            page_info->m_meta[HTTP_HEADER_FIELD_CONTENT_ENCODING] = "gzip";
        }

        string jsonstr = fwriter.write(page_info->m_meta);
        uint32_t write_buff_size = jsonstr.length() + 1 + page_info->m_content_size;
        if (write_buff_size > buff_size)
        {
            free(ptmpbuf);
            ptmpbuf = (char*) malloc(write_buff_size);
            MySuicideAssert(NULL != ptmpbuf);
            buff_size = write_buff_size;
        }

        uint16_t piece1_len = jsonstr.length() + 1; /* piece1_len包括 '\0' 的长度 */
        uint32_t flb_msg_size = 0;
        // 把'\0'也拷贝过去
        memcpy(ptmpbuf, jsonstr.c_str(), jsonstr.length() + 1);
        // 去掉json写入时讨厌的回车
        if ('\n' == ptmpbuf[jsonstr.length() - 1])
        {
            piece1_len = jsonstr.length();
            ptmpbuf[jsonstr.length() - 1] = '\0';
        }

        flb_msg_size += piece1_len;

        // 判断是否需要压缩，使用zlib在内存中对数据进行gzip压缩
        if (has_compressed)
        {
            // +1为为了避免覆盖'\0'
            memcpy(&ptmpbuf[piece1_len], page_info->m_content, page_info->m_content_size);
            flb_msg_size += page_info->m_content_size;
        }
        else if(page_info->m_content_size > 0)
        {
            uint32_t zlen = DEFAULT_BUFFER_SIZE;
            MySuicideAssert( 0 == zz.gzip((Bytef*)page_info->m_content, page_info->m_content_size, (Bytef*)pzipbuf, &zlen));
            memcpy(&ptmpbuf[piece1_len], pzipbuf, zlen);
            flb_msg_size += zlen;
        }

        uint32_t log_id = page_info->m_meta[LINK_FIELD_URL_NO].asUInt();
        // 把数据写入到磁盘中
        flb_basic_head basic_head;
        memset(&basic_head, 0, sizeof(basic_head));
        basic_head.log_id     = log_id;
        basic_head.piece1_len = piece1_len;
        basic_head.queue_type = 0;
        basic_head.event_type = 0;
        MySuicideAssert(0 == flb->write_message(basic_head, ptmpbuf, flb_msg_size));
        ROUTN("qlen[%u] jlen[%u] alen[%u] json[%s]", qlen, piece1_len, flb_msg_size, ptmpbuf);
        delete page_info;
    }

    free(ptmpbuf);
    free(pzipbuf);
    delete flb;

    return NULL;
}
