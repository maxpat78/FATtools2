# -*- coding: cp1252 -*-
"""
La numerazione CHS (Cylinder, Head, Sector) 24-bit inizia dal settore 1.
I tre byte nell'ordine:

        H (8 bit)     S (6 bit)   C (8+2 bit)
        |             |           |
    HHHHHHHH -+- CC SSSSSS -+- CCCCCCCC

e, quindi, i valori massimi sono: C=1024 (0-1023), H=255 (0-254), S=63 (1-63).

Il limite per il BIOS � (1024,16,63) o 1.032.192 settori per le vecchie
versioni, pari a 504 MiB con settori da 512 byte.
Per i nuovi BIOS (1024,255,63) o 16.450.560 settori, pari a 8.032 MiB.
Si noti che a causa di un bug nel DOS si sfruttano solo 255 Heads.

MS-DOS 6.22/7 rileva 8025 MiB su 10 GiB e permette una partizione primaria di
2047 MiB e una estesa di 5977, divisibile in unit� logiche da 2047 MiB.

MS-DOS 7.1 (Windows 95 OSR2) rileva 10 GiB e pu� creare una partizione
primaria FAT32-LBA (tipo 0x0C) assegnando tutto lo spazio.
Il limite di LBA � 2^32 settori o 2 TiB.

Se il settore finale LBA non � rappresentabile in CHS si usa la terna
(1023, 254, 63) o FE FF FF.
GPT adotta nel MBR di protezione la terna (1023, 255, 63) o FF FF FF e il
tipo di partizione 0xEE."""

import utils, struct
from gptutils import *

DEBUG = 0

from debug import log

def chs2lba(c, h, s, max_hpc=16, max_spc=63):
	# Max sectors per cylinder (track): 63
	if max_spc < 1 or max_spc > 63:
		return -1
	# Max heads per cyclinder (track): 255
	if max_hpc < 1 or max_hpc > 255:
		return -2
	if s < 1 or s > max_spc:
		return -0x10
	if h < 0 or h > max_hpc:
		return -0x20
	return (c*max_hpc+h)*max_spc + (s-1)

def lba2chs(lba, hpc=0):
	spc = 63
	if not hpc:
		for hpc in (16,32,64,128,255):
			if lba <= spc*hpc: break
	c = lba/(hpc*spc)
	h = (lba/spc)%hpc
	s = (lba%spc)+1
	return c, h, s

def size2chs(n, getgeometry=0):
    lba = n/512
    for hpc in (16,32,64,128,255):
        c,h,s = lba2chs(lba,hpc)
        if c < 1024: break
    if DEBUG&1: log("size2chs: calculated Heads Per Cylinder: %d", hpc)
    if not getgeometry:
        return c,h,s
    else:
        # partition that fits in the given space
        # full number of cylinders, HPC and SPT to use
        return c-1, hpc, 63
    
def chs2raw(t):
    "A partire da una tupla (C,H,S) calcola i 3 byte nell'ordine registrato nel Master Boot Record"
    c,h,s = t
    if c > 1023:
        B1, B2, B3 = 254, 255, 255
    else:
        B1, B2, B3 = h, (c&768)>>2|s, c&255
    #~ print "DEBUG: MBR bytes for LBA %d (%Xh): %02Xh %02Xh %02Xh"%(lba, lba, B1, B2, B3)
    return '%c%c%c' % (B1, B2, B3)

def raw2chs(t):
    "Converte i 24 bit della struttura CHS nel MBR in tupla"
    h,s,c = ord(t[0]),ord(t[1]),ord(t[2])
    return ((s & 192) << 2) | c, h, s & 63

def mkpart(offset, size, hpc=16):
    c, h, s = size2chs(size, 1)
    #~ print "Rounded CHS for %.02f MiB is %d-%d-%d (%.02f MiB)" % (size/(1<<20), c,h,s, 512*c*h*s/(1<<20))
    size = 512*c*h*s # adjust size
    dwFirstSectorLBA = offset/512
    sFirstSectorCHS = lba2chs(dwFirstSectorLBA, hpc)
    dwTotalSectors = (size/512)
    sLastSectorCHS = lba2chs(dwFirstSectorLBA+dwTotalSectors-1, hpc)
    return dwFirstSectorLBA, dwTotalSectors, sFirstSectorCHS,sLastSectorCHS



class MBR_Partition(object):
    "Partition entry in MBR/EBR Boot record (16 bytes)"
    layout = { # { offset: (name, unpack string) }
    0x1BE: ('bStatus', 'B'), # 80h=bootable, 00h=not bootable, other=invalid
    0x1BF: ('sFirstSectorCHS', '3s'), # absolute (=disk relative) CHS address of 1st partition sector
    0x1C2: ('bType', 'B'), # partition type: 7=NTFS, exFAT; C=FAT32 LBA; E=FAT16 LBA; F=Extended LBA; EE=GPT
    0x1C3: ('sLastSectorCHS', '3s'), # CHS address of last sector (or, if >8GB, FE FF FF [FF FF FF if GPT])
    0x1C6: ('dwFirstSectorLBA', '<I'), # LBA address of 1st sector
    # dwFirstSectorLBA in MBR/EBR 1st entry (logical partition) is relative to such partition start (typically 63 sectors);
    # in EBR *2nd* entry it's relative to *extended* partition start
    0x1CA: ('dwTotalSectors', '<I'), # number of sectors
    # 3 identical 16-byte groups corresponding to the other 3 primary partitions follow
    # DOS uses always 2 of the 4 slots
    # Modern Windows use LBA addressing
    } # Size = 0x10 (16 byte)

    def __init__ (self, s=None, offset=0, index=0):
        self.index = index
        self._i = 0
        self._pos = offset # base offset
        self._buf = s or bytearray(512)
        self._kv = {} # { offset: name}
        for k, v in MBR_Partition.layout.items():
            self._kv[k+index*16] = v
        self._vk = {} # { name: offset}
        for k, v in self._kv.items():
            self._vk[v[0]] = k # partition 0...3
        
    __getattr__ = utils.common_getattr

    def pack(self):
        "Update internal buffer"
        for k, v in self._kv.items():
            self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(self, v[0]))
        return self._buf

    def __str__ (self):
        return utils.class2str(self, "DOS %s Partition\n" % ('Primary','Extended','Unused','Unused')[self.index])

    def offset(self):
        "Returns partition offset"
        if self.bType in (0x7, 0xC, 0xE, 0xF): # if NTFS or FAT LBA
            return self.lbaoffset()
        else:
            return self.chsoffset()

    def chsoffset(self):
        "Returns partition absolute (=disk) byte offset"
        c, h, s = raw2chs(self.sFirstSectorCHS)
        if DEBUG&1: log("chsoffset: returning %016X", chs2lba(c, h, s, self.heads_per_cyl)*512)
        return chs2lba(c, h, s, self.heads_per_cyl)*512

    def lbaoffset(self):
        "Returns partition relative byte offset (from this/extended partition start)"
        return 512 * self.dwFirstSectorLBA
    
    def size(self):
        return 512 * self.dwTotalSectors


class MBR(object):
    "Master (or DOS Extended) Boot Record Sector"
    layout = { # { offset: (name, unpack string) }
    0x1FE: ('wBootSignature', '<H') # 55 AA
    } # Size = 0x200 (512 byte)

    def __init__ (self, s=None, offset=0, stream=None, disksize=0):
        self._i = 0
        self._pos = offset # base offset
        self._buf = s or bytearray(512) # normal MBR size
        self.stream = stream
        self.heads_per_cyl = 0 # Heads Per Cylinder (disk based)
        self.is_lba = 0
        self._kv = self.layout.copy()
        self._vk = {} # { name: offset}
        self.partitions = []
        self.heads_per_cyl = size2chs(disksize, True)[1] # detects disk geometry, size based
        if DEBUG&1: log("Calculated Heads Per Cylinder: %d", self.heads_per_cyl)
        for k, v in self._kv.items():
            self._vk[v[0]] = k
        for i in range(2): # Part. 2-3 unused in DOS
            self.partitions += [MBR_Partition(self._buf, index=i)]
            self.partitions[-1].heads_per_cyl = self.heads_per_cyl
    
    __getattr__ = utils.common_getattr

    def pack(self):
        "Update internal buffer"
        self.wBootSignature = 0xAA55 # set valid record signature
        for k, v in self._kv.items():
            self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(self, v[0]))
        for i in self.partitions:
            for k, v in i._kv.items():
                self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(i, v[0]))
        return self._buf

    def __str__ (self):
        s = utils.class2str(self, "Master/Extended Boot Record @%X\n" % self._pos)
        s += '\n' + str(self.partitions[0]) 
        s += '\n' + str(self.partitions[1])
        return s

    def delpart(self, index):
        "Deletes a partition, explicitly zeroing all fields"
        self.partitions[index].bStatus = 0
        self.partitions[index].sFirstSectorCHS = '\0\0\0'
        self.partitions[index].bType = 0
        self.partitions[index].sLastSectorCHS = '\0\0\0'
        self.partitions[index].dwFirstSectorLBA = 0
        self.partitions[index].dwTotalSectors = 0
    
    def setpart(self, index, start, size, hpc=16):
        "Creates a partition, given the start offset and size in bytes"
        part = MBR_Partition(index=index)
        dwFirstSectorLBA, dwTotalSectors, sFirstSectorCHS, sLastSectorCHS = mkpart(start, size, self.heads_per_cyl)
        if DEBUG&1: log("setpart(%d,%d,%d,%d): dwFirstSectorLBA=%08Xh, dwTotalSectors=%08Xh, sFirstSectorCHS=%s, sLastSectorCHS=%s",index, start, size, hpc, dwFirstSectorLBA, dwTotalSectors, sFirstSectorCHS, sLastSectorCHS)
        part.dwFirstSectorLBA = dwFirstSectorLBA
        part.dwTotalSectors = dwTotalSectors
        if sFirstSectorCHS[0] > 1023:
            part.sFirstSectorCHS = (0, 0, 1)
        else:
            part.sFirstSectorCHS = chs2raw(sFirstSectorCHS)
        if sLastSectorCHS[0] > 1023:
            part.sLastSectorCHS = (1023, 254, 63)
        else:
            part.sLastSectorCHS = chs2raw(sLastSectorCHS)
        #~ if index==0:
            #~ part.bStatus = 0x80
        # More bType: 01h=FAT12 Primary; 0Bh=FAT32 CHS; 0Ch=FAT32 LBA; 0Eh=FAT16 LBA; 0Fh=Extended LBA
        part.bType = 6 # Primary FAT16 > 32MiB
        if index > 0:
            if (start+size) < 8<<30:
                part.bType = 5 # Extended CHS
            else:
                part.bType = 15 # Extended LBA
        if size < 32<<20:
            part.bType = 4 # FAT16 < 32MiB
        elif size > 8032<<20:
            part.bType = 0xC # FAT32 LBA
        if DEBUG&1: log("setpart: auto set partition type %02X", part.bType)
        self.partitions[index] = part
