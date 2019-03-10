# -*- coding: cp1252 -*-
import sys, glob

#~ import logging
#~ logging.basicConfig(level=logging.DEBUG, filename='test_namegen.log', filemode='w')

import Volume, mkfat
#~ Volume.DEBUG = 255
#~ Volume.FAT.DEBUG = 255

from Volume import *

def printn(s):
 print s

fssize = 512<<20 # MB

# Create a blank image file, quickly allocating slack space
f = open('G.IMA', 'wb')
f.seek(fssize)
f.truncate()
f.seek(0)
mkfat.fat16_mkfs(f, fssize)
f.close()

root = opendisk('g.ima', 'r+b')
subdir = root.mkdir('DESTDIR')
copy_tree_in('C:\Bin\DOS', subdir, printn, 2)
