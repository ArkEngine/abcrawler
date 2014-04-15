#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include "bitmap.h"
#include "MyException.h"
#include "mylog.h"

using namespace evcrawler;

int main(int argc, char** argv)
{
    if (argc != 4)
    {
        printf("./test path file no\n");
        return 1;
    }
    uint32_t ID = atoi(argv[3]);
    struct stat fs;
    char path[128];
    snprintf(path, sizeof(path), "%s/%s", argv[1], argv[2]);
    MyThrowAssert( 0 == stat( path, &fs ) );
    //printf("size[%u] count[%u]\n", (uint32_t)fs.st_size, (uint32_t)((fs.st_size - 1)));
    bitmap* pmybitmap = new bitmap(argv[1], argv[2], (uint32_t)((fs.st_size - 1)));

    if (ID > (uint32_t)fs.st_size*8)
    {
        printf("ID[%u] toooooo BIG, MAX[%u]\n", ID, (uint32_t)(fs.st_size*8));
        return 1;
    }

    bitmap& mybitmap = *pmybitmap;
    printf("ID[%u] BIT[%u]\n", ID, _GET_BITMAP_(mybitmap, ID));
//    _SET_BITMAP_1_(mybitmap, ID);
//    printf("ID[%u] BIT[%u]\n", ID, _GET_BITMAP_(mybitmap, ID));
//    _SET_BITMAP_0_(mybitmap, ID);
//    printf("ID[%u] BIT[%u]\n", ID, _GET_BITMAP_(mybitmap, ID));

    delete pmybitmap;

    return 0;
}
