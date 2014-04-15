#ifndef _DISKSET_H_
#define _DISKSET_H_

#include <stdint.h>
#include <pthread.h>
#include <vector>
#include "bitmap.h"

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500 /* 支持读写锁 */
#endif

using namespace std;

// 为了节省key_value_t占用的内存
#pragma pack(push)
#pragma pack(1)
class diskset
{
    private:
		struct mem_block_link_t
		{
			uint32_t block_no :  9; ///> 最多支持512块内存
			uint32_t offset   : 23; ///> 每块内存内部支持1<<23个元素
		};

		struct sign40_t
        {
            uint32_t sign1;
            uint8_t  sign2;
        };

		struct key_value0_t
		{
			sign40_t          sign40;
			mem_block_link_t  next;
		};

        static const uint32_t END_OF_LIST    = (1<<9) - 1;
        static const uint32_t BLOCK_MAX_SIZE = (1 << 23);
        static const uint32_t BUCKET_SIZE = (1<<24);
        static const uint32_t BUCKET_MASK = (BUCKET_SIZE-1);

        char m_setpath[128];
        bitmap* m_pbucket_bitmap;
        mem_block_link_t*  m_bucket;
        pthread_rwlock_t   m_mutex;

        // mmap内存块存储在一个vector中
        vector<bitmap*> m_mem_block_list;
        mem_block_link_t m_last_mem_offset;
        bitmap* _get_new_mem_block(uint32_t no);

        diskset();
        diskset(const diskset&);

        void save_offset();
        void load_offset(uint32_t& block_no, uint32_t& offset);

    public:

        diskset(const char* setpath, bool clear_flag);
        ~diskset();
        bool select_key (const uint64_t key);
        /*
         * insert_key的返回值可以表明，插入之前的diskset中之否已经包含这个key,
         * 如果包含，则不执行插入动作，返回0，
         * 如果不包含，则执行插入动作，返回1.
         */
        bool insert_key (const uint64_t key);
        void clear(); // 清理掉diskset中的数据，恢复到初始化的状态
};
#pragma pack(pop)
#endif
