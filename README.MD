FATtools
========


PLEASE NOTE:
THIS CODE IS NOT MAINTAINED ANYMORE!
SWITCH TO PYTHON 3 AND LATEST FATTOOLS FOR PYTHON 3!


This is a refinement and extension of my FATtools provided in PyDiskTools on GitHub.

Born to re-sort in an arbitrary order the directory entries in a FAT32 root table to cope with some hardware MP3 players' limits, it now provides full read/write support in Python 2.7 (32- and 64-bit) for FAT12/16/32 and exFAT filesystems, for hacking and recovering purposes.

It is able to open disk partitioned with both MBR or GPT and to help in partitioning.
It can transparently create, read and write Dynamic and Differencing VHD disk images.

Actually, following features are implemented (mostly in Python, with a few ctypes calls to handle Win32 disks natively; *but it works in Linux, also*):
- sector aligned read/writes with both file images and real disks
- sector based caching mechanism (for both reading and writing) to speed-up FAT and directory table operations
- run length encoded map (with tuples and dictionaries) of free clusters, free directory slots, allocated cluster chains
- transparent reading and writing of FAT12/16/32 and exFAT filesystems with FS boot-sector auto recognizer
- MBR and GPT partitions handling
- Long File Name and Unicode support
- tools to open, create, rename, list and delete files and directories, and to partition disks
- facilities to sort, clean and shrink directory tables
- file fragmentation calculator
- mkfat tool to properly apply a FAT12/16/32 or exFAT filesystem to a block device (file or disk) and let CHKDSK be happy with it (included exFAT compressed Up-Case table generator)

*Obviously, since a filesystem is an extremely complex and delicate matter, and big bugs may lay around, you'll USE IT TOTALLY AT YOUR OWN RISK!* But it seems quite stable and usable, now.

The most fragile area (and, thus, subject to bugs) was the caching mechanism, that operates in different ways:
- intercepting small I/O (<= 512 bytes), which is cached in a small circular buffer. Bigger I/O bypasses the cache; when the cache is full, all dirty sectors are committed to disk and the cache buffer is zeroed. Sectors and buffers are paired with Python builtin dictionaries: this permits a good (from a Pythonic perspective) I/O speed during FAT and directory tables access;
- mantaining a dictionary of pre-decoded FAT indexes, to improve the speed of repetitive access to cluster chains;
- mantaining a dictionary of short and long names (paired with their respective directory slots) for each directory table, to speed up searches and updates in directory tables;
- mantaining a RLE map of of free clusters, free directory slots and allocated cluster chains, to dramatically improve speed of allocation and file access. 

*Actually, the I/O speed is closer to system's one.*

Code is GPLed (look at GPL.TXT inside folder).


Sample usage (see inside 'test' and 'sample' directories for more samples).


At a glance

- to open an existing plain or VHD disk image, or real disk:
```
# -*- coding: cp1252 -*-
from Volume import *
disk = vopen('MyDiskImage.img', 'r+b', 'disk')
```

- to make a single GPT partition from all disk space:
```
gpt = Volume.partutils.partition(disk)
```

- to format such partition with the exFAT file system:
```
import mkfat
part = vopen('MyDiskImage.img', 'r+b', 'partition0')
mkfat.exfat_mkfs(part, (gpt.partitions[0].u64EndingLBA-gpt.partitions[0].u64StartingLBA+1)*512)
```

- to order root directory table in USB drive X:
```
# -*- coding: cp1252 -*-
from Volume import *

# Assuming we have DirA, DirB, DirC in this disk order into X:
root = vopen('X:', 'r+b')

new_order = '''DirB
DirC
DirA'''

root._sortby.fix = new_order.split('\n') # uses built-in directory sort algorithm
root.sort(root._sortby)
```
