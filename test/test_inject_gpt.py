# -*- coding: cp1252 -*-
"Sample: creates a GPT disk with a primary partition, formats it with exFAT and then injects the Python directory"
import sys, glob, ctypes, uuid

#~ import logging
#~ logging.basicConfig(level=logging.DEBUG, filename='test_inject.log', filemode='w')

import Volume, mkfat
#~ Volume.DEBUG = 255
#~ Volume.partutils.DEBUG = 255
#~ Volume.FAT.DEBUG = 255
#~ Volume.exFAT.DEBUG = 255
#~ Volume.disk.DEBUG=1

from Volume import *

def printn(s):
 print s

DISK = 'HD'
fssize = 256<<20 # MB

print "Creating a blank %.02f MiB disk image" % (fssize/float(1<<20))
f = open(DISK, 'wb')
f.seek(fssize)
f.truncate()
f.seek(0)

print "Making a GPT data partition on it"
print "Writing protective MBR"
mbr = Volume.partutils.MBR(None, disksize=fssize)
mbr.setpart(0, 512, fssize-512) # create primary partition
mbr.partitions[0].bType = 0xEE # Protective GPT MBR
mbr.partitions[0].dwTotalSectors = 0xFFFFFFFF
f.write(mbr.pack())
print mbr

print "Writing GPT Header and 16K Partition Array"
gpt = Volume.partutils.GPT(None)
gpt.sEFISignature = 'EFI PART'
gpt.dwRevision = 0x10000
gpt.dwHeaderSize = 92
gpt.u64MyLBA = 1
gpt.u64AlternateLBA = (fssize-512)/512
gpt.u64FirstUsableLBA = 0x22
gpt.dwNumberOfPartitionEntries = 0x80
gpt.dwSizeOfPartitionEntry = 0x80
# Windows stores a backup copy of the GPT array (16 KiB) before Alternate GPT Header
gpt.u64LastUsableLBA = gpt.u64AlternateLBA - (gpt.dwNumberOfPartitionEntries*gpt.dwSizeOfPartitionEntry)/512
gpt.u64DiskGUID = uuid.uuid4().get_bytes_le()
gpt.u64PartitionEntryLBA = 2

gpt.parse(ctypes.create_string_buffer(gpt.dwNumberOfPartitionEntries*gpt.dwSizeOfPartitionEntry))
gpt.setpart(0, gpt.u64FirstUsableLBA, gpt.u64LastUsableLBA-gpt.u64FirstUsableLBA+1, "My Partition")

f.write(gpt.pack())
f.seek(gpt.u64PartitionEntryLBA*512)
f.write(gpt.raw_partitions)

# writes backup
f.seek((gpt.u64LastUsableLBA+1)*512)
f.write(gpt.raw_partitions)
f.write(gpt._buf)
print gpt
f.close()

print "Applying exFAT File System on partition"
f = openpart(DISK, 'r+b')
#~ mkfat.fat16_mkfs(f, (gpt.partitions[0].u64EndingLBA-gpt.partitions[0].u64StartingLBA+1)*512)
mkfat.exfat_mkfs(f, (gpt.partitions[0].u64EndingLBA-gpt.partitions[0].u64StartingLBA+1)*512)

print "Injecting a Python27 tree"
root = openpart(DISK, 'r+b').open()
subdir = root.mkdir('Python')
copy_tree_in('C:\Python27', subdir, printn, 2)
