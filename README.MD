FATtools
========

This is a refinement and extension of my FATtools provided in PyDiskTools on GitHub.

Born to re-sort in an arbitrary order the directory entries in a FAT32 root table to cope with some hardware MP3 players' limits, it now aims to provide full read/write support in Python for FAT12/16/32 and exFAT filesystems, for hacking and recovering purposes.

Actually, following features are implemented (mostly in Python, with a few ctypes calls to handle Win32 disks natively):
- sector aligned read/writes with both file images and real Win32 disks
- sector based caching mechanism (for both reading and writing) to speed-up FAT and directory table operations
- reading and writing of FAT12/16/32 filesystems (with FS boot-sector auto recognizer)
- Long File Name support
- tools to open, create, rename, list and delete files and directories
- facilities to sort, clean and shrink directory tables
- file fragmentation calculator
- mkfs tool to properly apply a FAT12/16/32 filesystem to a block device (file or disk) and let CHKDSK be happy with it

exFAT read support has to be merged from original FATtools; write & mkfs support has to be written from scratch, instead.

Obviously, since a filesystem is an extremely complex and delicate matter, and big bugs may lay around, you'll USE IT TOTALLY AT YOUR OWN RISK!

The most fragile area (and, thus, subject to bugs) is the caching mechanism, that operates in different ways:
- intercepting small I/O (< 512 bytes), which is cached in a 1MB buffer. Bigger I/O bypasses the cache; when the cache is full, all dirty sectors are committed to disk and the cache buffer is zeroed. Sectors and buffers are paired with Python builtin dictionaries: this permits a good (from a Pythonic perspective) I/O speed during FAT and directory tables access;
- mantaining a dictionary of pre-decoded FAT indexes, to improve the speed of repetitive access to cluster chains;
- mantaining a dictionary of short and long names (paired with their respective directory slots) for each directory table, to speed up searches and updates in directory tables.
