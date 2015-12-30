# -*- coding: mbcs -*-
import os, sys

import logging
logging.basicConfig(level=logging.DEBUG, filename='PYTEST.LOG', filemode='w')

from ctypes import *
from FAT import *

root = opendisk('G:', 'r+b')

base = r'C:\Python27'
#~ base = r'C:\Python27\Lib'
for r, d, f in os.walk(base):
    for n in f:
        src = os.path.join(r, n)
        rel = src[len(base)+1:]
        s = open(src,'rb').read()
        print "Copying", rel
        
        subdirs = rel.split('\\')
        if len(subdirs) > 1:
            target_dir = root.opendir(os.path.dirname(rel))
        else:
            target_dir = root
        if not target_dir:
            target_dir = root
            for subdir in subdirs[:-1]:
                print "Creating", os.path.join(target_dir.path, subdir)
                target_dir.mkdir(subdir)
                target_dir = target_dir.opendir(subdir)
            target_dir = root.opendir(os.path.dirname(rel))
        dst = target_dir.create(os.path.basename(rel))
        dst.File.write(s)
        dst.close()
