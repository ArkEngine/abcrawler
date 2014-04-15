#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "diskset.h"
#include "mylog.h"
#include "MyException.h"


diskset :: diskset(
        const char* setpath,
        bool clear_flag
        )
{
    snprintf(m_setpath, sizeof(m_setpath), "%s/", setpath);
    MySuicideAssert(0 == access(m_setpath, F_OK));
    // 初始化bucket
    char filename[128];
    snprintf(filename, sizeof(filename), "%s/diskset.bucket", m_setpath);
    bool bucket_exist = (0 == access(filename, F_OK));
    mode_t amode = (bucket_exist) ? O_RDWR : O_RDWR|O_CREAT;
    if (! bucket_exist)
    {
        int fd = open(filename, amode, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
        MySuicideAssert(fd != -1);
        close(fd);
    }
    m_pbucket_bitmap = new bitmap(setpath, "diskset.bucket", BUCKET_SIZE*sizeof(mem_block_link_t), false);
    m_bucket = (mem_block_link_t*)m_pbucket_bitmap->puint;
    if (! bucket_exist)
    {
        // 第一次创建要全部初始化
        memset(m_bucket, 0xFF, BUCKET_SIZE*sizeof(mem_block_link_t));
    }

    uint32_t block_no = 0;
    uint32_t offset = 0;
    load_offset(block_no, offset);
    m_last_mem_offset.block_no = block_no;
    m_last_mem_offset.offset   = offset;

    if (clear_flag)
    {
        // 清理以前的数据，从新开始
        // 包括m_last_mem_offset.block_no本身
        for (uint32_t i=0; i<(uint32_t)(m_last_mem_offset.block_no+1); i++)
        {
            snprintf(filename, sizeof(filename), "%s/diskset.data.%u", m_setpath, i);
            unlink(filename);
        }
        memset(m_bucket, 0xFF, BUCKET_SIZE*sizeof(mem_block_link_t));
        m_last_mem_offset.block_no = 0;
        m_last_mem_offset.offset   = 0;
        bitmap* pbitmap = _get_new_mem_block(0);
        m_mem_block_list.push_back(pbitmap);
        save_offset();
    }
    else
    {
        for (uint32_t i=0; i<(uint32_t)(m_last_mem_offset.block_no+1); i++)
        {
            snprintf(filename, sizeof(filename), "diskset.data.%u", i);
            bitmap* pbitmap = new bitmap(setpath, filename, BLOCK_MAX_SIZE*sizeof(key_value0_t), false);
            m_mem_block_list.push_back(pbitmap);
        }
    }

    pthread_rwlock_init(&m_mutex, NULL);
    ROUTN("setpath[%s] key_value0_t's size[%u] mem_block_link_t's size[%u]",
            m_setpath, sizeof(key_value0_t), sizeof(mem_block_link_t));
}

diskset :: ~diskset()
{
    // 你确认没人使用了diskset哦，否则就 SIGNAL 11
    // 遍历所有的memblocks，释放内存
    for (uint32_t i=0; i<m_mem_block_list.size(); i++)
    {
        delete m_mem_block_list[i];
    }
    delete m_pbucket_bitmap;
    m_bucket = NULL;
}

void diskset:: load_offset(uint32_t &block_no, uint32_t& offset)
{
    // 读取进度
    char filename[128];
    snprintf(filename, sizeof(filename), "%s/diskset.offset", m_setpath);
    FILE* fp = fopen(filename, "r");
    MySuicideAssert (fp != NULL);
    char rbuff[128];
    fgets(rbuff, sizeof(rbuff), fp);
    int ret = 0;
    ret = sscanf (rbuff, "block_no : %u", &block_no);
    MySuicideAssert (1 == ret);
    fgets(rbuff, sizeof(rbuff), fp);
    ret = sscanf (rbuff, "offset   : %u", &offset);
    MySuicideAssert (1 == ret);
    DEBUG("setdir[%s] block_no[%u] offset[%u]", m_setpath, block_no, offset);
    fclose(fp);
}

void diskset:: save_offset()
{
    // 写入文件
    char filename[128];
    snprintf(filename, sizeof(filename), "%s/diskset.offset", m_setpath);
    int wfd = open(filename, O_WRONLY, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    MySuicideAssert(wfd > 0);
    char wbuff[1024];
    int wlen = snprintf(wbuff, sizeof(wbuff),
            "block_no : %u\n"
            "offset   : %u\n",
            m_last_mem_offset.block_no, m_last_mem_offset.offset);
    MySuicideAssert(wlen == write(wfd, wbuff, wlen));
    MySuicideAssert( 0 == ftruncate(wfd, wlen));
    close(wfd);
    return;
}

bitmap* diskset :: _get_new_mem_block(uint32_t no)
{
    char filename[128];
    snprintf(filename, sizeof(filename), "%s/diskset.data.%u", m_setpath, no);

    unlink(filename);
    mode_t amode = (0 == access(filename, F_OK)) ? O_RDWR : O_RDWR|O_CREAT;
    int fd = open(filename, amode, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    MySuicideAssert(fd != -1);
    close(fd);

    snprintf(filename, sizeof(filename), "diskset.data.%u", no);
    bitmap* pbitmap = new bitmap(m_setpath, filename, BLOCK_MAX_SIZE*sizeof(key_value0_t), false);
    MySuicideAssert(pbitmap != NULL);
    MySuicideAssert((m_mem_block_list.size()+1) < END_OF_LIST);
    return pbitmap;
}

bool diskset :: select_key (const uint64_t key)
{
    const uint32_t* pi32 = (uint32_t*)(&key);
    const uint32_t bucket_no = (uint32_t)(pi32[0] & BUCKET_MASK);
    const uint32_t key_sign1 = pi32[1];
    const uint8_t  key_sign2 = (uint8_t)(pi32[0] >> 24);
    mem_block_link_t mem_block_link = m_bucket[bucket_no];
    bool found = false;
    //    pthread_rwlock_rdlock(&m_mutex);
    while(mem_block_link.block_no != END_OF_LIST)
    {
        key_value0_t* pkv = (key_value0_t*)(m_mem_block_list[mem_block_link.block_no]->puint);
        if (pkv[mem_block_link.offset].sign40.sign1 == key_sign1 && pkv[mem_block_link.offset].sign40.sign2 == key_sign2)
        {
            found = true;
            break;
        }
        else
        {
            mem_block_link = pkv[mem_block_link.offset].next;
        }
    }
    //    pthread_rwlock_unlock(&m_mutex);

    return found;
}

bool diskset :: insert_key (const uint64_t key)
{
    const uint32_t* pi32 = (uint32_t*)(&key);
    const uint32_t bucket_no = (uint32_t)(pi32[0] & BUCKET_MASK);
    const uint32_t key_sign1 = pi32[1];
    const uint8_t  key_sign2 = (uint8_t)(pi32[0] >> 24);
    mem_block_link_t mem_block_link = m_bucket[bucket_no];
    bool found = false;
    while(mem_block_link.block_no != END_OF_LIST)
    {
        key_value0_t* pkv = (key_value0_t*)(m_mem_block_list[mem_block_link.block_no]->puint);
        if (pkv[mem_block_link.offset].sign40.sign1 == key_sign1 && pkv[mem_block_link.offset].sign40.sign2 == key_sign2)
        {
            found = true;
            break;
        }
        else
        {
            mem_block_link = pkv[mem_block_link.offset].next;
        }
    }
    if (! found)
    {
        // 分配一个 headlist cell
        pthread_rwlock_wrlock(&m_mutex);
        if  ((uint32_t)(1+m_last_mem_offset.offset) == BLOCK_MAX_SIZE)
        {
            // 此块已经没有空余空间了。
            bitmap* pbitmap = _get_new_mem_block(m_last_mem_offset.block_no + 1);
            m_mem_block_list.push_back(pbitmap);
            m_last_mem_offset.block_no ++;
            //MySuicideAssert((m_last_mem_offset.block_no+1) == m_mem_block_list.size());
            m_last_mem_offset.offset = 0;
        }
        key_value0_t* pkv = (key_value0_t*)(m_mem_block_list[m_last_mem_offset.block_no]->puint);
        key_value0_t* pdstkv = &pkv[m_last_mem_offset.offset];
        pdstkv->sign40.sign1  = key_sign1;
        pdstkv->sign40.sign2  = key_sign2;
        pdstkv->next.block_no = m_bucket[bucket_no].block_no;
        pdstkv->next.offset   = m_bucket[bucket_no].offset;

        m_bucket[bucket_no] = m_last_mem_offset;
        m_last_mem_offset.offset++;
        save_offset();
        pthread_rwlock_unlock(&m_mutex);
    }
    return found == false;
}

void diskset :: clear()
{
    // 调用者保证执行这段代码时，没有人读写这个对象
    // 注意: 第一块内存不释放!!
    pthread_rwlock_wrlock(&m_mutex);
    for (uint32_t i=1; i<m_mem_block_list.size(); i++)
    {
        char filename[128];
        snprintf(filename, sizeof(filename), "%s/diskset.data.%u", m_setpath, i);
        unlink(filename);
        delete m_mem_block_list[i];
    }
    m_mem_block_list.resize(1);

    // 重置bucket
    memset(m_bucket, 0xFF, BUCKET_SIZE*sizeof(mem_block_link_t));

    // 重置当前待用游标
    m_last_mem_offset.block_no = 0;
    m_last_mem_offset.offset   = 0;
    save_offset();
    pthread_rwlock_unlock(&m_mutex);
}
