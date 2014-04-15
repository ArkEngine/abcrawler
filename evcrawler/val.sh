rm val.log -f
valgrind --tool=memcheck --leak-check=full --log-file=val.log ./bin/evcrawler
