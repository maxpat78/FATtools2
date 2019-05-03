# -*- coding: cp1252 -*-

import sys, glob, ctypes, uuid, stress, hashlib, pprint

import logging
logging.basicConfig(level=logging.DEBUG, filename='test_vhd_delta4.log', filemode='w')

import Volume, mkfat, vhdutils
#~ Volume.DEBUG = 255
#~ Volume.vhdutils.DEBUG = 255
#~ Volume.partutils.DEBUG = 255
#~ Volume.FAT.DEBUG = 255
#~ Volume.exFAT.DEBUG = 255
#~ Volume.disk.DEBUG=1

from Volume import *

def printn(s):
 print s

def copy_sectors(src, dest, size):
    todo = size
    u_total = todo
    src.seek(0*512)
    n = (8<<20)
    while todo:
        dest.write(src.read((n, todo)[todo<n]))
        todo -= min(n, todo)


f = openpart('delta.vhd').open()
print f.listdir()

bad=0
L = f.open('hashes.sha1').read()
for o in L.split('\n'):
 sha, pname = o[:40], o[42:]
 if len(pname)<1: continue
 try:
    s = f.open(str(pname)).read()
 except:
    print "Exception on", pname
 test = hashlib.sha1(s).hexdigest()==sha
 if not test:
    bad+=1
    print 'bad', pname
if bad:
    print bad, "wrong file checksums detected!"
else:
    print "all checksums were ok"
