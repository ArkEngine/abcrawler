#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>
#include "diskset.h"
#include "mylog.h"

int main(const int argc, char** argv)
{
    if (argc != 2)
    {
        printf("./test KEYSIZE\n");
        return 1;
    }

    const uint32_t KEYSIZE = atoi(argv[1]);
    diskset myset("./data/", false);

    struct   timeval btv;
    struct   timeval etv;
    gettimeofday(&btv, NULL);
    for (uint32_t ukey=0; ukey<KEYSIZE; ukey++)
    {
        uint64_t u64key;
        uint32_t* pu32 = (uint32_t*)(&u64key);
        pu32[0] = ukey;
        pu32[1] = ukey;
        //assert (true == myset.insert(u64key));
        myset.insert_key(u64key);
    }
    gettimeofday(&etv, NULL);
    ROUTN ("termCount[%u] set_time-consumed[%u]us", KEYSIZE,
            (etv.tv_sec - btv.tv_sec)*1000000+(etv.tv_usec - btv.tv_usec));
    printf ("termCount[%u] set_time-consumed[%lu]us\n", KEYSIZE,
            (etv.tv_sec - btv.tv_sec)*1000000+(etv.tv_usec - btv.tv_usec));

    gettimeofday(&btv, NULL);
    for (uint32_t ukey=0; ukey<KEYSIZE; ukey++)
    {
        uint64_t u64key;
        uint32_t* pu32 = (uint32_t*)(&u64key);
        pu32[0] = ukey;
        pu32[1] = ukey;
        assert (true == myset.select_key(u64key));
    }
    gettimeofday(&etv, NULL);
    ROUTN ("termCount[%u] get_time-consumed[%u]us", KEYSIZE,
            (etv.tv_sec - btv.tv_sec)*1000000+(etv.tv_usec - btv.tv_usec));
    printf ("termCount[%u] get_time-consumed[%lu]us\n", KEYSIZE,
            (etv.tv_sec - btv.tv_sec)*1000000+(etv.tv_usec - btv.tv_usec));

    return 0;
}
