# -*- coding: cp1252 -*-
"Sample: creates a MBR disk with a FAT32 LBA primary partition, formats it with FAT and then injects the Python directory"

import sys, glob

#~ import logging
#~ logging.basicConfig(level=logging.DEBUG, filename='test_inject.log', filemode='w')

import Volume, mkfat
#~ Volume.DEBUG = 255
#~ Volume.FAT.DEBUG = 255
#~ Volume.exFAT.DEBUG = 255
#~ Volume.disk.DEBUG=1

from Volume import *

def printn(s):
 print s

DISK = 'HD'
fssize = 256<<20 # MB

print "Creating a blank %.02f MiB disk image" % (fssize/float(1<<20) )
f = open(DISK, 'wb')
f.seek(fssize)
f.truncate()
f.seek(0)

print "Making a MBR FAT32-LBA primary partition on it"
mbr = Volume.partutils.MBR(None, disksize=fssize)
mbr.setpart(0, 63*512, fssize-(1<<20)) # creates primary partition
mbr.partitions[0].bType = 12 # FAT32 LBA
p0_offset = mbr.partitions[0].dwFirstSectorLBA*512
p0_size = mbr.partitions[0].dwTotalSectors*512

f.write(mbr.pack())
f.close()

print "Applying FAT16 File System on partition"
f = openpart(DISK, 'r+b')
mkfat.fat16_mkfs(f, p0_size)
#~ mkfat.exfat_mkfs(f, fssize)

print "Injecting a Python27 tree"
root = openpart(DISK, 'r+b').open()
subdir = root.mkdir('Python')
copy_tree_in('C:\Python27', subdir, printn, 2)
