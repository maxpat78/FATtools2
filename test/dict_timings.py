# cProfile x
import collections

d = {x:x+1 for x in xrange(50000) }

a = None
for i in xrange(1000000):
    if i in d:
        a = d[i]
