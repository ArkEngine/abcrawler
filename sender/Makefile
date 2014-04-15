CC= g++
SHELL= /bin/sh

OBJ= sender.o xhead_message.o

MAIN= sender

VERINFO= -DPROJNAME="\"$(MAIN)\"" -D_VERSION="\"$(MAIN) 0.0.0.1\""

INCS= -I ../utils/mylog/include/ \
	  -I ../utils/filelinkblock/include/ \
	  -I ../utils/xhead/include/ \
	  -I ../utils/MyException/include/ \
	  -I ../opensrc/jsoncpp/include/ \

LDFLAGS= -L ../opensrc/jsoncpp/lib/     -ljson \
		 -L ../utils/filelinkblock/lib/ -lfilelinkblock \
		 -L ../utils/creat_sign/lib/    -lcreat_sign \
		 -L ../utils/xhead/lib/         -lxhead \
		 -L ../utils/mylog/lib/         -lmylog \
		 -L ../utils/MyException/lib/   -lMyException \


CPPFLAGS= $(DEBUG) $(INCS) $(VERINFO) -g  -pipe  \
		  -Wtrigraphs -Wformat -Wparentheses -Wpointer-arith \
		  -W -Wall\
		  -Wshadow -Wreturn-type \

all: $(MAIN) 
	rm *.o -rf

$(MAIN): $(OBJ) 
	$(CC) -o $@ $^ $(LDFLAGS) -lpthread
	rm -rf bin/ log/
	mkdir log bin
	mv $(MAIN) ./bin/

%.o: %.cpp %.h
	$(CC) $(CPPFLAGS) $< -c -o $@

clean:
	rm *.o -rf
	rm $(MAIN) -rf
	rm -rf bin/ log/
