g++ -c diskset.cpp pydiskset.cc -I /usr/include/python2.7/ -I ../bitmap/include/ -I ../mylog/include/ -I ../MyException/include/
g++ -shared diskset.o pydiskset.o -o pydiskset.so -L ../bitmap/lib/ -lbitmap -L ../mylog/lib/ -lmylog -L ../MyException/lib/ -lMyException
