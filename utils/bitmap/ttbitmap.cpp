#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include "bitmap.h"
#include "MyException.h"
using namespace std;
using namespace flexse;

int main(int argc, char** argv)
{
    if (argc != 2)
    {
        printf("./test FILE\n");
        return 1;
    }
    bitmap* pmybitmap = NULL;
    int fd = open(argv[1], O_RDWR, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    if(fd == -1)
    {
        printf("file[%s] NOT exist.\n", argv[1]);
        exit(1);
    }

    try
    {
        pmybitmap = new bitmap(fd, PROT_READ, MAP_PRIVATE|MAP_LOCKED );
    }
    catch (flexse::MyException &e)
    {
        printf("exception caught. [%p]\n", pmybitmap);
    }
    catch(...)
    {
        printf("unknown exception caught. [%p]\n", pmybitmap);
    }

    delete pmybitmap;

    return 0;
}
