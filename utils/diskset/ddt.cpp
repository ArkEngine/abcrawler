#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>
#include "diskset.h"
#include "creat_sign.h"
#include "mylog.h"

int main(const int argc, char** argv)
{
    if (argc != 2)
    {
        printf("./ddt INSERT|SELECT < file\n");
        exit(1);
    }

    if (0 != strcmp(argv[1], "INSERT") && 0 != strcmp(argv[1], "SELECT"))
    {
        printf("./ddt INSERT|SELECT < file\n");
        exit(1);
    }

    bool insert_mode = (0 == strcmp(argv[1], "INSERT"));

    char tmpstr[1024];
    diskset myset("./data/", false);
    while(NULL != fgets(tmpstr, sizeof(tmpstr), stdin))
    {
        int len = (int)strlen(tmpstr);
        if (tmpstr[len - 1] == '\n')
        {
            tmpstr[len - 1] = 0;
            len --;
        }
        if (len == 0)
        {
            continue;
        }
        uint32_t sign1, sign2;
        creat_sign_64(tmpstr, len, &sign1, &sign2);
        uint64_t u64key;
        uint32_t* pu32 = (uint32_t*)(&u64key);
        pu32[0] = sign1;
        pu32[1] = sign2;
        if (insert_mode)
        {
            myset.insert(u64key, 0, true); // value全为0
        }
        else
        {
            uint32_t value = 0;
            if (0 != myset.select(u64key, value))
            {
                printf("%s\n", tmpstr);
            }
        }
    }

    return 0;
}
