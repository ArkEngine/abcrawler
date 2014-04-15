CC= g++
SHELL= /bin/sh
RANLIB=ranlib

LIB= libdiskset.a
LIBOBJ= diskset.o

LDFLAGS= -L ./lib/ -ldiskset \
		 -L ../MyException/lib -lMyException \
		 -L ../bitmap/lib/ -lbitmap \
		 -L ../creat_sign/lib/ -lcreat_sign \
		 -L ../../opensrc/jsoncpp/lib/ -ljson \
		 -L ../MyException/lib/ -lMyException \
		 -L ../mylog/lib/ -lmylog -lpthread \

INCS= -I ../mylog/include/ \
	  -I ../MyException/include \
	  -I ../bitmap/include/ \
	  -I ../creat_sign/include/ \

CPPFLAGS= $(INCS) -g -pipe -fPIC -W -Wall -Wshadow -Wreturn-type \
		  -Wtrigraphs -Wformat -Wparentheses -Wpointer-arith \
		  -Wwrite-strings -Wswitch -Wconversion -Wsign-promo -Wredundant-decls -Wreorder \
		  #			-Weffc++ \-Werror \

# 用来控制是否执行代码覆盖率检查
# 若想执行，在命令行中执行 export COVER=xxoo即可
# 若不想执行，则在命令行中执行 unset COVER即可
ifneq ($(origin COVER), undefined)
LDFLAGS+= -L ../unittest/lib/ -lgcover
CPPFLAGS+= -fprofile-arcs -ftest-coverage
endif


all: clean $(LIB) test
	rm *.o -f

$(LIB): $(LIBOBJ)
	ar cr $(LIB) $(LIBOBJ)
	$(RANLIB) $(LIB)
	rm -rf lib include
	mkdir lib/
	mkdir include/
	mv $(LIB) lib/
	cp diskset.h include

%.o: %.cpp %.h
	$(CC) $(CPPFLAGS) $< -c -o $@

clean:
	rm $(LIB) -f
	rm *.o -f
	rm -rf lib/ include/
	rm test -f

test:
	$(CC) test.cpp -o test $(CPPFLAGS) $(LDFLAGS) -lpthread

url2id:
	$(CC) url2id.cpp -o url $(CPPFLAGS) $(LDFLAGS) -lpthread

