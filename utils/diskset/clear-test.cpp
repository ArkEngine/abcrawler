#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <time.h>
#include <sys/time.h>
#include "diskset.h"
#include "mylog.h"

int main(const int argc, char** argv)
{
    diskset myset("./data/", true);
    myset.clear();

    return 0;
}
