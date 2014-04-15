#include <stdlib.h>
#include <getopt.h>
#include <stdio.h>
#include <assert.h>
#include <zlib.h>
#include "filelinkblock.h"
#include "comprezz.h"

using namespace std;

void print_help(void)
{               
	printf("\nUsage:\n");
	printf("%s <options>\n", PROJNAME);
	printf("  options:\n");
	printf("    -p:  #queue path\n");
	printf("    -n:  #queue name\n");
	printf("    -b:  #queue block_id\n");
	printf("    -f:  #queue file no\n");
	printf("    -c:  #read block count\n");
	printf("    -v:  #Print version information\n");
	printf("    -h:  #This page\n");
	printf("\n\n");
}               

void print_version(void)
{
	printf("Project    :  %s\n", PROJNAME);
	printf("Version    :  %s\n", VERSION);
	printf("Cvstag     :  %s\n", CVSTAG);
	printf("BuildDate  :  %s\n", __DATE__);
}

char* pzzbuff = NULL;
uint32_t zz_size = 1024*1024;

void read_and_print(filelinkblock& flb, char* readbuff, uint32_t SIZE)
{
    comprezz zz;
    flb_basic_head basic_head;
	uint32_t read_size = flb.read_message(basic_head, readbuff, SIZE);
	assert (0 < read_size);
    readbuff[read_size] = 0;
	fprintf(stdout, "===== log_id[%u] block_id[%u] msglen[%u] =====\n", basic_head.log_id, basic_head.block_id, read_size);
	fprintf(stdout, "%s\n", readbuff);
    if (basic_head.piece1_len != 0)
    {
        uint32_t zz_now_size = zz_size;
        uint32_t zipped_size = read_size-(basic_head.piece1_len+1);
        zz.gunzip((Bytef*)&readbuff[basic_head.piece1_len+1], zipped_size, (Bytef*)pzzbuff, &zz_now_size);
        pzzbuff[zz_now_size] = '\0';
        fprintf(stdout, "%s\n", pzzbuff);
    }
	fflush(stdout);
}

int main(int argc, char * argv[])
{
	const char* queue_path = NULL;
	const char* queue_name = NULL;
	int read_block_count   = 0;
	int begin_block_id     = 0;
	int begin_file_no      = 0;
	char c = '\0';
	while ((c = (char)getopt(argc, argv, "f:c:b:p:n:hv?")) != -1) 
	{
		switch (c) 
		{
			case 'f':
				begin_file_no = atoi(optarg);
				break;
			case 'c':
				read_block_count = atoi(optarg);
				break;
			case 'b':
				begin_block_id = atoi(optarg);
				break;
			case 'p':
				queue_path = optarg;
				break;
			case 'n':
				queue_name = optarg;
				break;
			case 'h':
			case '?':
				print_help();
				return 0;
			case 'v':
				print_version();
				return 0;
			default:
				break;
		}
	}

	if (queue_path == NULL || queue_name == NULL)
	{
		print_help();
		exit(1);
	}

    pzzbuff = (char*)malloc(zz_size);

	filelinkblock flb(queue_path, queue_name, true);
	flb.set_channel("queue.debug");
	flb.seek_message(begin_file_no, begin_block_id);
	const uint32_t SIZE = 40000000;
	char*      readbuff = (char*)malloc(SIZE);
	if (read_block_count > 0)
	{
		while(read_block_count -- )
		{
			read_and_print(flb, readbuff, SIZE);
		}
	}
	else
	{
		while( 1 )
		{
			read_and_print(flb, readbuff, SIZE);
		}
	}
	free(readbuff);
	free(pzzbuff);

	return 0;
}
