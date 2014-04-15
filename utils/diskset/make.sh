swig -c++ -python diskset.i
g++ -c diskset.cpp diskset_wrap.cxx -I /usr/include/python2.7/ -I ../bitmap/include/ -I ../mylog/include/ -I ../MyException/include/
g++ -shared diskset.o diskset_wrap.o -o _diskset.so -L ../bitmap/lib/ -lbitmap -L ../mylog/lib/ -lmylog -L ../MyException/lib/ -lMyException
