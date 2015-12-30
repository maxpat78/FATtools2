# -*- coding: mbcs -*-
import os, sys, optparse

import logging
logging.basicConfig(level=logging.DEBUG, filename='PYTEST.LOG', filemode='w')

from datetime import datetime
from ctypes import *

from utils import FSguess
from FAT import *
import disk

disk = disk.disk('\\\\.\\G:', 'r+b')
#~ disk = disk.disk('FLOPPY.IMA', 'r+b')
#~ disk = disk.disk('G.ima', 'r+b')
fstyp = FSguess(boot_fat16(disk))
disk.seek(0)

if fstyp in ('FAT12', 'FAT16'):
    boot = boot_fat16(disk)
elif fstyp == 'FAT32':
    boot = boot_fat32(disk)
elif fstyp == 'EXFAT':
    boot = boot_exfat(disk)
elif fstyp == 'NTFS':
    print fstyp, "file system not supported. Aborted."
    sys.exit(1)
else:
    print "File system not recognized. Aborted."
    sys.exit(1)

fat = FAT(disk, boot.fatoffs, boot.clusters(), bitsize={'FAT12':12,'FAT16':16,'FAT32':32,'EXFAT':32}[fstyp], exfat=(fstyp=='EXFAT'))

print "File system is %s, %d clusters of %.1fK, root @%Xh, cluster #2 @%Xh" % (fstyp,boot.clusters(),boot.cluster/1024.0,boot.root(),boot.dataoffs)
print fat

n, maxrun = fat.findmaxrun()
print "%d bytes free in %d clusters (max run of %d at #%d)" % (n*boot.cluster, n, maxrun[1], maxrun[0]) 

dt = Dirtable(boot, fat, boot.dwRootCluster)

dt.mkdir('Nuova Directory dal Nome Lungo')
subdir = dt.opendir('Nuova Directory dal Nome Lungo')
print "Directory of", subdir.path
subdir.list()

for i in range(16):
    # autoclosing
    f = subdir.create('Shorty%d.txt'%i)
    f.File.write('%d'%i)
    f.close()

for i in range(16, 32):
    # autoclosing
    f = subdir.create('Shorty%d.txt'%i)

print "Creating SHORTY.TXT of 3 clusters..."
f = subdir.create('Shorty.txt')
f.File.write(boot.cluster*'A' + boot.cluster*'a' + boot.cluster*'X')
f.close()

print "Re-opening SHORTY.TXT for truncating to 1024 bytes..."
f = subdir.open('Shorty.txt')
f.File.seek(1024)
f.File.trunc()
f.close()

print "Re-opening SHORTY.TXT for truncating to 64 bytes and appending <CR><LF>..."
f = subdir.open('Shorty.txt')
f.File.seek(64)
f.File.trunc()
f.File.write('\r\n')
f.close()

f = subdir.create('Nome di file lungo abbastanza lungo nella nuova subdirectory.txt')
f.File.write("Nuovo file di testo.")
f.close()

#~ f = subdir.open('Nome di file lungo abbastanza lungo nella nuova subdirectory.txt')
#~ f.File.write("Nuovo file di testo ancora maggiore, modificato con open.")
#~ f.close()

#~ for name in dt.iterator():
    #~ print name.Name()
#~ sys.exit(1)

print "Root at first:", dt.stream

# TEST 1 - OK
f = dt.create('file0001.txt')
if not f.IsValid:
    print "FATAL: can't create new file!"
    sys.exit(1)
f.File.write('File contents written at '+str(datetime.now()))
for i in range(10000):
    f.File.write('%d\r\n' % i)
f.File.frags()
f.close()

f = dt.create('Un Nome di file Lungo piuttosto lungo.txt')
if not f.IsValid:
    print "FATAL: can't create new file!"
    sys.exit(1)
f.File.write('File contents written at '+str(datetime.now()))
for i in range(10000):
    f.File.write('%d\r\n' % i)
f.File.frags()
f.close()

# TEST2
for j in range(4):
    f = dt.create('Altro Nome di file lungo abbastanza lungo %08d.bin' % j)
    f.File.write(boot.cluster*chr(0xF0+j))
    f.close()

# TEST3
for j in range(4):
    f = subdir.create('File dal Nome Lungo abbastanza lungo nella nuova subdirectory %03d.txt' % j)
    f.File.write("Nuovo file di testo %03d." % j)
    f.close()

for j in range(4):
    f = subdir.open('File dal Nome Lungo abbastanza lungo nella nuova subdirectory %03d.txt' % j)
    f.File.write("Modifica al Nuovo file di testo %03d." % j)
    f.close()

print "Root as by now:", dt.stream
print "Compacting & cleaning root dir..."
dt.stream.frags()

lista='''Nuova Directory dal Nome Lungo
Un Nome di file Lungo piuttosto lungo.txt
Altro Nome di file lungo abbastanza lungo 00000001.bin
Altro Nome di file lungo abbastanza lungo 00000002.bin
Altro Nome di file lungo abbastanza lungo 00000003.bin
Altro Nome di file lungo abbastanza lungo 00000004.bin
Altro Nome di file lungo abbastanza lungo 00000005.bin
Altro Nome di file lungo abbastanza lungo 00000006.bin
Altro Nome di file lungo abbastanza lungo 00000000.bin'''
dt._sortby.fix = lista.split('\n')
#~ dt.sort(dt._sortby)

#~ dt.shrink() 
print "Root as by now:", dt.stream
dt.stream.frags()

#~ dt.stream.seek(0)
#~ file('ROOT.BIN','wb').write(dt.stream.read())

#~ subdir.stream.seek(0)
#~ file('SUBDIR.BIN','wb').write(subdir.stream.read())
