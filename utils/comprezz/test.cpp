#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <zlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "comprezz.h"

void print_help()
{
    printf("Usage: ./test gzip   src_file dst_file\n");
    printf("       ./test gunzip src_file dst_file\n");
    exit(1);
}

int main(int argc, char** argv) {

    if (argc != 4)
    {
        print_help();
    }
    if ((0 != strcmp("gzip", argv[1])) && (0 != strcmp("gunzip", argv[1])))
    {
        print_help();
    }

    int rfd;
    assert ((rfd = open(argv[2], O_RDONLY)) > 0);

    int pageLen = (int)lseek(rfd, 0, SEEK_END);
    printf("src file:%s src length:%d\n", argv[2], pageLen);
    char *page = (char *)malloc(pageLen);
    lseek(rfd, 0, SEEK_SET);
    int n = (int)read(rfd, page, pageLen);
    assert (n == (int)pageLen);
    close(rfd);

    Bytef zdata[20*1024];
    memset(zdata, 0, sizeof(zdata));

    uint32_t nzdata = sizeof(zdata);
    Bytef odata[20*1024];
    uint32_t nodata = sizeof(odata);

    comprezz zz;
    //for(int i=0; i<10000; i++)
    //{
    //    assert (gzcompress((Bytef *) page, pageLen, zdata, &nzdata) == 0);
    //}
    //return 0;

    if (0 == strcmp(argv[1], "gzip"))
    {
        assert (argc == 4);

        if (zz.gzip((Bytef*)page, pageLen, zdata, &nzdata) == 0) {
            fprintf(stdout, "nzdata:%u rate: %.3f\n", nzdata, (double)nzdata/(double)pageLen);
            mode_t amode = (0 == access(argv[3], F_OK)) ? O_WRONLY|O_TRUNC : O_WRONLY|O_TRUNC|O_CREAT;
            int wfd = open(argv[3], amode, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            assert (-1 != wfd);
            assert ( nzdata == (uint32_t)write(wfd, zdata, nzdata));
            close(wfd);
            printf("write %u bytes into %s\n", nzdata, argv[3]);
        }
        else
        {
            printf("gzip FAIL!\n");
        }
    }
    else if(0 == strcmp(argv[1], "gunzip"))
    {
        assert (argc == 4);
        memset(odata, 0, sizeof(odata));
        //        for (int i=0; i<1000; i++)
        //        {
        if (zz.gunzip((Bytef*)page, pageLen, odata, &nodata) == 0) {
            fprintf(stdout, "nzdata:%u bytes\n", nodata);
            mode_t amode = (0 == access(argv[3], F_OK)) ? O_WRONLY|O_TRUNC : O_WRONLY|O_TRUNC|O_CREAT;
            int wfd = open(argv[3], amode, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
            assert (-1 != wfd);
            assert ( nodata == (uint32_t)write(wfd, odata, nodata));
            close(wfd);
            printf("write %u bytes into %s\n", nodata, argv[3]);
        }
        else
        {
            printf("*** error ***\n");
        }
        //        }
    }
    free(page);
    return 0;
}
