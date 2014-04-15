CC= g++
SHELL= /bin/sh

OBJ= config.o evcrawler.o host_links.o crawling_thread.o evcrawler_struct.o page_info_queue.o dump2disk_thread.o dns_cache.o

MAIN= evcrawler

VERINFO= -DPROJNAME="\"$(MAIN)\"" -DVERSION="\"$(MAIN) 0.0.0.1\""

INCS= -I ../utils/mylog/include/ \
	  -I ../utils/xhead/include/ \
	  -I ../utils/creat_sign/include/ \
	  -I ../utils/comprezz/include/ \
	  -I ../utils/MyException/include/ \
	  -I ../utils/filelinkblock/include/ \
	  -I ../opensrc/jsoncpp/include/ \
	  -I /home/worker/opensource/include/ \

LDFLAGS= -L ../opensrc/jsoncpp/lib/            -ljson \
		 -L ../utils/filelinkblock/lib/        -lfilelinkblock \
		 -L ../utils/xhead/lib/                -lxhead \
		 -L ../utils/creat_sign/lib/           -lcreat_sign \
		 -L ../utils/comprezz/lib/             -lcomprezz \
		 -L ../utils/mylog/lib/                -lmylog \
		 -L ../utils/MyException/lib/          -lMyException \
		 -L /home/worker/opensource/lib/      -levent \

CPPFLAGS= $(DEBUG) $(INCS) $(VERINFO) -g  -pipe  -W -Wall \
		  -Wshadow -Wreturn-type \
		  -Wtrigraphs -Wformat -Wparentheses -Wpointer-arith  -Werror \

all: $(MAIN) 
	rm *.o -rf

$(MAIN): $(OBJ) 
	$(CC) -o $@ $^ $(LDFLAGS) -lpthread -lz
	rm -rf bin/ log/ 
	mkdir bin
	mv $(MAIN) bin/ -f

%.o: %.cpp %.h
	$(CC) $(CPPFLAGS) $< -c -o $@

clean:
	rm *.o -rf
	rm $(MAIN) -rf
	rm -rf bin/ log/
