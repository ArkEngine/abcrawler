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
    uint32_t value = 0;
    for (uint32_t ukey=0; ukey<KEYSIZE; ukey++)
    {
        myset.select(ukey, value);
        assert((uint32_t)ukey == value);
    }
    gettimeofday(&etv, NULL);
    ROUTN ("termCount[%u] get_time-consumed[%u]us", KEYSIZE,
            (etv.tv_sec - btv.tv_sec)*1000000+(etv.tv_usec - btv.tv_usec));

    return 0;
}
