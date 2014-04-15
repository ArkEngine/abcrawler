#ifndef __MEM_BITMAP_H
#define __MEM_BITMAP_H

#define UINT_BIT 32

class MemBitMap
{
    public:
        MemBitMap(const int max_size);
        ~MemBitMap();
        void set(const int i);
        void clear(const int i);
        bool check(const int i);

    private:
        uint32_t* buff;
};

#endif
