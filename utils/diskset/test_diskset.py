import diskset
import sys
import time

loop = int(sys.argv[1])
ds = diskset.diskset('./data/', False)

bt = time.time()
for x in xrange(loop):
    #k = (x<<32) + x
    ds.insert_key(x,x)
et = time.time()
print 'insert[%u] time-cost:%.3f' % (loop, et-bt)

bt = time.time()
for x in xrange(loop):
    #k = (x<<32) + x
    ds.select_key(x,x)
et = time.time()
print 'select[%u] time-cost:%.3f' % (loop, et-bt)
