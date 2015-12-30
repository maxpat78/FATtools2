# -*- coding: mbcs -*-
import os, sys

#~ import logging
#~ logging.basicConfig(level=logging.DEBUG, filename='PYTEST.LOG', filemode='w')

from ctypes import *
from FAT import *

root = opendisk('G:')
dirp = root.opendir(sys.argv[1])
print "Directory of '%s' of %d bytes" % (sys.argv[1], dirp.stream.size)

c = dirp.start
i = 1
while 1:
    if c == dirp.fat.last: break
    print "Cluster #%d is #%d @%Xh" % (i, c, root.boot.cl2offset(c))
    i+=1
    c = dirp.fat[c]

outf = open("Chain%08d.bin"%dirp.start, 'wb')
dirp.stream.seek(0)
outf.write(dirp.stream.read())
