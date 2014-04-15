#ifndef __COMPREZZ_H_
#define __COMPREZZ_H_
#include <pthread.h>
#include <stdint.h>
#include <zlib.h>

class comprezz
{
    private:

    public:
        comprezz();
        ~comprezz();
        int gzip(Bytef *data, uint32_t ndata, Bytef *zdata, uint32_t *nzdata);
        int gunzip(Byte *zdata, uint32_t nzdata, Byte *data, uint32_t *ndata);
};

#endif
