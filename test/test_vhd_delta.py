# -*- coding: cp1252 -*-

import sys, glob, ctypes, uuid, stress

import logging
logging.basicConfig(level=logging.DEBUG, filename='test_delta_vhd.log', filemode='w')

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

DISK='mybase.vhd'
fssize = 1<<30 # MB

print "Creating a blank %.02f MiB Dynamic VHD disk image" % (fssize/float(1<<20))
vhdutils.mk_dynamic(DISK, fssize, upto=40<<30)

print "Creating a blank %.02f MiB Differencing VHD disk image, linked to previous one" % (fssize/float(1<<20))
vhdutils.mk_diff('delta.vhd', DISK)
DISK='delta.vhd'

f = vhdutils.Image(DISK, 'r+b')

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
gpt.u64LastUsableLBA = gpt.u64AlternateLBA - (gpt.dwNumberOfPartitionEntries*gpt.dwSizeOfPartitionEntry)/512 - 1
gpt.u64DiskGUID = uuid.uuid4().get_bytes_le()
gpt.u64PartitionEntryLBA = 2

gpt.parse(ctypes.create_string_buffer(gpt.dwNumberOfPartitionEntries*gpt.dwSizeOfPartitionEntry))
gpt.setpart(0, gpt.u64FirstUsableLBA, gpt.u64LastUsableLBA-gpt.u64FirstUsableLBA+1, "My Partition")

f.write(gpt.pack())
f.seek(gpt.u64PartitionEntryLBA*512)
f.write(gpt.raw_partitions)

f.seek((gpt.u64LastUsableLBA+1)*512)
f.write(gpt.raw_partitions) # writes backup
f.write(gpt._buf)
print gpt
f.close()

print "Applying FAT File System on partition"
f = openpart(DISK, 'r+b')
mkfat.fat16_mkfs(f, (gpt.partitions[0].u64EndingLBA-gpt.partitions[0].u64StartingLBA+1)*512)
#~ mkfat.exfat_mkfs(f, (gpt.partitions[0].u64EndingLBA-gpt.partitions[0].u64StartingLBA+1)*512)

root = openpart(DISK, 'r+b').open()
root.create('a.txt').write('CIAO')

#~ print "Injecting a Python27 tree"
#~ root = openpart(DISK, 'r+b').open()
#~ subdir = root.mkdir('Python')
#~ copy_tree_in('C:\Python27', subdir, printn, 2)

#~ print "Running stress test..."
#~ class Opts():
 #~ pass
 
#~ opts = Opts()
#~ opts.threshold=99
#~ opts.file_size=1<<20
#~ opts.programs=63
#~ opts.debug=0
#~ opts.sha1=1
#~ opts.fix=0
#~ stress.stress(opts, [DISK])
