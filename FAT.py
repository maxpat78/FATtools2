# -*- coding: mbcs -*-
# Utilities to manage a FAT12/16/32 file system
#

DEBUG_FAT=0

import copy, os, struct, time, cStringIO, atexit
from datetime import datetime
from collections import OrderedDict
from zlib import crc32
import disk, utils

if DEBUG_FAT:
    import hexdump
    import logging

class FATException(Exception):
	pass

class boot_fat32(object):
    "FAT32 Boot Sector"
    layout = { # { offset: (name, unpack string) }
    0x00: ('chJumpInstruction', '3s'),
    0x03: ('chOemID', '8s'),
    0x0B: ('wBytesPerSector', '<H'),
    0x0D: ('uchSectorsPerCluster', 'B'),
    0x0E: ('wSectorsCount', '<H'), # reserved sectors (min 32?)
    0x10: ('uchFATCopies', 'B'),
    0x11: ('wMaxRootEntries', '<H'),
    0x13: ('wTotalSectors', '<H'),
    0x15: ('uchMediaDescriptor', 'B'),
    0x16: ('wSectorsPerFAT', '<H'), # not used, see 24h instead
    0x18: ('wSectorsPerTrack', '<H'),
    0x1A: ('wHeads', '<H'),
    0x1C: ('wHiddenSectors', '<H'),
    0x1E: ('wTotalHiddenSectors', '<H'),
    0x20: ('dwTotalLogicalSectors', '<I'),
    0x24: ('dwSectorsPerFAT', '<I'),
    0x28: ('wMirroringFlags', '<H'), # bits 0-3: active FAT, it bit 7 set; else: mirroring as usual
    0x2A: ('wVersion', '<H'),
    0x2C: ('dwRootCluster', '<I'), # usually 2
    0x30: ('wFSISector', '<H'), # usually 1
    0x32: ('wBootCopySector', '<H'), # 0x0000 or 0xFFFF if unused, usually 6
    0x34: ('chReserved', '12s'),
    0x40: ('chPhysDriveNumber', 'B'),
    0x41: ('chFlags', 'B'),
    0x42: ('chExtBootSignature', 'B'),
    0x43: ('dwVolumeID', '<I'),
    0x47: ('sVolumeLabel', '11s'),
    0x52: ('sFSType', '8s'),
    #~ 0x72: ('chBootstrapCode', '390s'),
    0x1FE: ('wBootSignature', '<H') # 55 AA
    } # Size = 0x200 (512 byte)

    def __init__ (self, s=None, offset=0, stream=None):
        self._i = 0
        self._pos = offset # base offset
        self._buf = s or bytearray(512) # normal boot sector size
        self.stream = stream
        self._kv = self.layout.copy()
        self._vk = {} # { name: offset}
        for k, v in self._kv.items():
            self._vk[v[0]] = k
        self.__init2__()

    def __init2__(self):
        if not self.wBytesPerSector: return
        # Cluster size (bytes)
        self.cluster = self.wBytesPerSector * self.uchSectorsPerCluster
        # Offset of the 1st FAT copy
        self.fatoffs = self.wSectorsCount * self.wBytesPerSector + self._pos
        # Data area offset (=cluster #2)
        self.dataoffs = self.fatoffs + self.uchFATCopies * self.dwSectorsPerFAT * self.wBytesPerSector + self._pos
        # Number of clusters represented in this FAT (if valid buffer)
        self.fatsize = self.dwTotalLogicalSectors/self.uchSectorsPerCluster
        if self.stream:
            self.fsinfo = fat32_fsinfo(stream=self.stream, offset=self.wFSISector*self.cluster)
        else:
            self.fsinfo = None

    __getattr__ = utils.common_getattr

    def __str__ (self):
        return utils.class2str(self, "FAT32 Boot Sector @%x\n" % self._pos)

    def pack(self):
        "Update internal buffer"
        for k, v in self._kv.items():
            self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(self, v[0]))
        self.__init2__()
        return self._buf

    def clusters(self):
        "Return the number of clusters in the data area"
        # Total sectors minus sectors preceding the data area
        return (self.dwTotalLogicalSectors - (self.dataoffs/self.wBytesPerSector)) / self.uchSectorsPerCluster

    def cl2offset(self, cluster):
        "Return the real offset of a cluster"
        return self.dataoffs + (cluster-2)*self.cluster

    def root(self):
        "Return the offset of the root directory"
        return self.cl2offset(self.dwRootCluster)

    def fat(self, fatcopy=0):
        "Return the offset of a FAT table (the first by default)"
        return self.fatoffs + fatcopy * self.dwSectorsPerFAT * self.wBytesPerSector



class fat32_fsinfo(object):
    "FAT32 FSInfo Sector (usually sector 1)"
    layout = { # { offset: (name, unpack string) }
    0x00: ('sSignature1', '4s'), # RRaA
    0x1E4: ('sSignature2', '4s'), # rrAa
    0x1E8: ('dwFreeClusters', '<I'), # 0xFFFFFFFF if unused (may be incorrect)
    0x1EC: ('dwNextFreeCluster', '<I'), # hint only (0xFFFFFFFF if unused)
    0x1FE: ('wBootSignature', '<H') # 55 AA
    } # Size = 0x200 (512 byte)

    def __init__ (self, s=None, offset=0, stream=None):
        self._i = 0
        self._pos = offset # base offset
        self._buf = s or bytearray(512) # normal FSInfo sector size
        self.stream = stream
        self._kv = self.layout.copy()
        self._vk = {} # { name: offset}
        for k, v in self._kv.items():
            self._vk[v[0]] = k

    __getattr__ = utils.common_getattr

    def pack(self):
        "Update internal buffer"
        for k, v in self._kv.items():
            self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(self, v[0]))
        return self._buf

    def __str__ (self):
        return utils.class2str(self, "FAT32 FSInfo Sector @%x\n" % self._pos)



class boot_fat16(object):
    "FAT12/16 Boot Sector"
    layout = { # { offset: (name, unpack string) }
    0x00: ('chJumpInstruction', '3s'),
    0x03: ('chOemID', '8s'),
    0x0B: ('wBytesPerSector', '<H'),
    0x0D: ('uchSectorsPerCluster', 'B'),
    0x0E: ('wSectorsCount', '<H'),
    0x10: ('uchFATCopies', 'B'),
    0x11: ('wMaxRootEntries', '<H'),
    0x13: ('wTotalSectors', '<H'),
    0x15: ('uchMediaDescriptor', 'B'),
    0x16: ('wSectorsPerFAT', '<H'), #DWORD in FAT32
    0x18: ('wSectorsPerTrack', '<H'),
    0x1A: ('wHeads', '<H'),
    0x1C: ('dwHiddenSectors', '<I'), # Here differs from FAT32
    0x20: ('dwTotalLogicalSectors', '<I'),
    0x24: ('chPhysDriveNumber', 'B'),
    0x25: ('uchCurrentHead', 'B'),
    0x26: ('uchSignature', 'B'), # 0x28 or 0x29
    0x27: ('dwVolumeID', '<I'),
    0x2B: ('sVolumeLabel', '11s'),
    0x36: ('sFSType', '8s'),
    0x1FE: ('wBootSignature', '<H') # 55 AA
    } # Size = 0x200 (512 byte)

    def __init__ (self, s=None, offset=0, stream=None):
        self._i = 0
        self._pos = offset # base offset
        self._buf = s or bytearray(512) # normal boot sector size
        self.stream = stream
        self._kv = self.layout.copy()
        self._vk = {} # { name: offset}
        for k, v in self._kv.items():
            self._vk[v[0]] = k
        self.__init2__()

    def __init2__(self):
        if not self.wBytesPerSector: return
        # Cluster size (bytes)
        self.cluster = self.wBytesPerSector * self.uchSectorsPerCluster
        # Offset of the 1st FAT copy
        self.fatoffs = self.wSectorsCount * self.wBytesPerSector + self._pos
        # Number of clusters represented in this FAT
        # Here the DWORD field seems to be set only if WORD one is too small
        self.fatsize = (self.dwTotalLogicalSectors or self.wTotalSectors)/self.uchSectorsPerCluster
        # Offset of the fixed root directory table (immediately after the FATs)
        self.rootoffs = self.fatoffs + self.uchFATCopies * self.wSectorsPerFAT * self.wBytesPerSector + self._pos
        # Data area offset (=cluster #2)
        self.dataoffs = self.rootoffs + (self.wMaxRootEntries*32)
        # Set for compatibility with FAT32 code
        self.dwRootCluster = 0

    __getattr__ = utils.common_getattr

    def __str__ (self):
        return utils.class2str(self, "FAT12/16 Boot Sector @%x\n" % self._pos)

    def pack(self):
        "Update internal buffer"
        for k, v in self._kv.items():
            self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(self, v[0]))
        self.__init2__()
        return self._buf

    def clusters(self):
        "Return the number of clusters in the data area"
        # Total sectors minus sectors preceding the data area
        return ((self.dwTotalLogicalSectors or self.wTotalSectors) - (self.dataoffs/self.wBytesPerSector)) / self.uchSectorsPerCluster

    def cl2offset(self, cluster):
        "Return the real offset of a cluster"
        return self.dataoffs + (cluster-2)*self.cluster

    def root(self):
        "Return the offset of the root directory"
        return self.rootoffs

    def fat(self, fatcopy=0):
        "Return the offset of a FAT table (the first by default)"
        return self.fatoffs + fatcopy * self.wSectorsPerFAT * self.wBytesPerSector



# NOTE: limit decoded dictionary size! Zero or {}.popitem()?
class FAT(object):
    "Decodes a FAT (12, 16, 32 o EX) table on disk"
    def __init__ (self, stream, offset, clusters, bitsize=32, exfat=0):
        self.stream = stream
        self.size = clusters # total clusters in the data area (max = 2^x - 11)
        self.bits = bitsize # cluster slot bits (12, 16 or 32)
        self.offset = offset # relative FAT offset (1st copy)
        # CAVE! This accounts the 0-1 unused cluster index?
        self.offset2 = offset + rdiv(rdiv(clusters*bitsize, 8), 512)*512 # relative FAT offset (2nd copy)
        self.exfat = exfat # true if exFAT (aka FAT64)
        self.reserved = 0x0FF7
        self.bad = 0x0FF7
        self.last = 0x0FFF
        if bitsize == 32:
            self.fat_slot_size = 4
            self.fat_slot_fmt = '<I'
        else:
            self.fat_slot_size = 2
            self.fat_slot_fmt = '<H'
        if bitsize == 16:
            self.reserved = 0xFFF7
            self.bad = 0xFFF7
            self.last = 0xFFF8
        elif bitsize == 32:
            self.reserved = 0x0FFFFFF7 # FAT32 uses 28 bits only
            self.bad = 0x0FFFFFF7
            self.last = 0x0FFFFFF8
            if exfat: # EXFAT uses all 32 bits...
                self.reserved = 0xFFFFFFF7
                self.bad = 0xFFFFFFF7
                self.last = 0xFFFFFFFF
        # maximum cluster index effectively addressable
        # clusters ranges from 2 to 2+n-1 clusters (zero based), so last valid index is n+1
        self.real_last = min(self.reserved-1, self.size+2-1)
        self.decoded = {} # {cluster index: cluster content}
        self.last_free_alloc = 2 # last free cluster allocated (also set in FAT32 FSInfo)
        self.free_clusters = None # tracks free clusters
        # ordered (by disk offset) dictionary {first_cluster: run_length} mapping free space
        self.free_clusters_map = None
        self.map_free_space()

    def __str__ (self):
        return "%d-bit %sFAT table of %d clusters starting @%Xh\n" % (self.bits, ('','ex')[self.exfat], self.size, self.offset)

    def __getitem__ (self, index):
        "Retrieve the value stored in a given cluster index"
        try:
            assert 2 <= index <= self.real_last
        except AssertionError:
            if DEBUG_FAT: logging.debug("Attempt to read unexistant FAT index #%d", index)
            raise FATException("Attempt to read unexistant FAT index #%d" % index)
            return self.last
        slot = self.decoded.get(index)
        if slot: return slot
        pos = self.offset+(index*self.bits)/8
        self.stream.seek(pos)
        slot = struct.unpack(self.fat_slot_fmt, self.stream.read(self.fat_slot_size))[0]
        #~ print "getitem", self.decoded
        if self.bits == 12:
            # Pick the 12 bits we want
            if index % 2: # odd cluster
                slot = slot >> 4
            else:
                slot = slot & 0x0FFF
        self.decoded[index] = slot
        if DEBUG_FAT: logging.debug("Got FAT1[0x%X]=0x%X @0x%X", index, slot, pos)
        return slot

    # Defer write on FAT#2 allowing undelete?
    def __setitem__ (self, index, value):
        "Set the value stored in a given cluster index"
        try:
            assert 2 <= index <= self.real_last
        except AssertionError:
            if DEBUG_FAT: logging.debug("Attempt to set invalid cluster index 0x%X with value 0x%X", index, value)
            return
        try:
            assert value <= self.real_last or value >= self.reserved
        except AssertionError:
            if DEBUG_FAT: logging.debug("Attempt to set invalid value 0x%X in cluster 0x%X", value, index)
            return
        self.decoded[index] = value
        dsp = (index*self.bits)/8
        pos = self.offset+dsp
        if self.bits == 12:
            # Pick and set only the 12 bits we want
            self.stream.seek(pos)
            slot = struct.unpack(self.fat_slot_fmt, self.stream.read(self.fat_slot_size))[0]
            if index % 2: # odd cluster
                # Value's 12 bits moved to top ORed with original bottom 4 bits
                #~ print "odd", hex(value), hex(slot), self.decoded
                value = (value << 4) | (slot & 0xF)
                #~ print hex(value), hex(slot)
            else:
                # Original top 4 bits ORed with value's 12 bits
                #~ print "even", hex(value), hex(slot)
                value = (slot & 0xF000) | value
                #~ print hex(value), hex(slot)
        if DEBUG_FAT: logging.debug("setting FAT1[0x%X]=0x%X @0x%X", index, value, pos)
        self.stream.seek(pos)
        value = struct.pack(self.fat_slot_fmt, value)
        self.stream.write(value)
        if self.exfat: return # exFAT has one FAT only (default)
        pos = self.offset2+dsp
        #~ logging.debug("setting FAT2[%Xh]=%Xh @%Xh", index, value, pos)
        self.stream.seek(pos)
        self.stream.write(value)

    def isvalid(self, index):
        "Test if index is a valid cluster number in this FAT"
        # Inline explicit test avoiding func call to speed-up
        if (index >= 2 and index <= self.real_last) or self.islast(index) or self.isbad(index):
            return 1
        if DEBUG_FAT: logging.debug("invalid cluster index: %x", index)
        return 0

    def islast(self, index):
        "Test if index is the last cluster in the chain"
        return self.last <= index <= self.last+7 # *F8 ... *FF

    def isbad(self, index):
        "Test if index is a bad cluster"
        return index == self.bad

    def count(self, startcluster):
        "Count the clusters in a chain. Returns a tuple (<total clusters>, <last cluster>)"
        n = 1
        while not (self.last <= self[startcluster] <= self.last+7): # islast
            startcluster = self[startcluster]
            n += 1
        return (n, startcluster)

    def count_to(self, startcluster, clusters):
        "Find the index of the n-th cluster in a chain"
        while clusters and not (self.last <= self[startcluster] <= self.last+7): # islast
            startcluster = self[startcluster]
            clusters -= 1
        return startcluster

    def count_run(self, start, count=0):
        """Return the count of the clusters in a contiguous run from 'start'
        and the next cluster, eventually limiting to the first 'count' clusters"""
        n = 1
        while not (self.last <= self[start] <= self.last+7): # islast
            prev = start
            start = self[start]
            # If next LCN is not contig
            if prev != start-1:
                break
            # If max run length reached
            if count > 0:
                if  count-1 == 0:
                    break
                else:
                    count -= 1
            n += 1
        return n, start

    def findmaxrun(self):
        "Find the greatest cluster run available. Returns a tuple (total_free_clusters, (run_start, clusters))"
        t = 1,0
        maxrun=(0,0)
        n=0
        while 1:
            t = self.findfree(t[0]+1)
            if t[0] < 0: break
            if DEBUG_FAT: logging.debug("Found %d free clusters from #%d", t[1], t[0])
            maxrun = max(t, maxrun, key=lambda x:x[1])
            n += t[1]
            t = (t[0]+t[1], t[1])
        if DEBUG_FAT: logging.debug("Found the biggest run of %d clusters from #%d on %d total free clusters", maxrun[1], maxrun[0], n)
        return n, maxrun

    def map_free_space(self):
        "Maps the free clusters in an ordered dictionary {start_cluster: run_length}"
        if self.exfat: return
        startpos = self.stream.tell()
        self.free_clusters_map = OrderedDict()
        FREE_CLUSTERS=0
        if self.bits < 32:
            # FAT16 is max 130K...
            PAGE = self.offset2 - self.offset - (2*self.bits)/8
        else:
            # FAT32 could reach ~1GB!
            PAGE = 1<<20
        END_OF_CLUSTERS = self.offset + rdiv(self.size*self.bits, 8) + (2*self.bits)/8
        i = self.offset+(2*self.bits)/8 # address of cluster #2
        self.stream.seek(i)
        while i < END_OF_CLUSTERS:
            s = self.stream.read(min(PAGE, END_OF_CLUSTERS-i)) # slurp full FAT, or 1M page if FAT32
            if DEBUG_FAT: logging.debug("map_free_space: loaded FAT page of %d bytes @0x%X", len(s), i)
            j=0
            while j < len(s):
                first_free = -1
                run_length = -1
                while j < len(s):
                    if self.bits == 32:
                        if s[j] != 0 or s[j+1] != 0 or s[j+2] != 0 or s[j+3] != 0:
                            j += 4
                            if run_length > 0: break
                            continue
                    elif self.bits == 16:
                        if s[j] != 0 or s[j+1] != 0:
                            j += 2
                            if run_length > 0: break
                            continue
                    elif self.bits == 12:
                        # Pick the 12 bits wanted
                        #     0        1        2
                        # AAAAAAAA AAAABBBB BBBBBBBB
                        if not j%3:
                            if s[j] != 0 or s[j+1]>>4 != 0:
                                j += 1
                                if run_length > 0: break
                                continue
                        elif j%3 == 1:
                            j+=1
                            continue # simply skips median byte
                        else: # j%3==2
                            if s[j] != 0 or s[j-1] & 0x0FFF != 0:
                                j += 1
                                if run_length > 0: break
                                continue
                    if first_free < 0:
                        first_free = (i-self.offset+j)*8/self.bits
                        if DEBUG_FAT: logging.debug("map_free_space: found run from %d", first_free)
                        run_length = 0
                    run_length += 1
                    j+=self.bits/8
                if first_free < 0: continue
                FREE_CLUSTERS+=run_length
                if self.free_clusters_map:
                    ff, rl = self.free_clusters_map.popitem()
                    if ff+rl == first_free:
                        if DEBUG_FAT: logging.debug("map_free_space: merging run (%d, %d) with previous one", first_free, run_length)
                        first_free = ff
                        run_length = rl+run_length
                    else:
                        self.free_clusters_map[ff] =  rl # push back item
                self.free_clusters_map[first_free] =  run_length
                if DEBUG_FAT: logging.debug("map_free_space: appended run (%d, %d)", first_free, run_length)
            i += len(s) # advance to next FAT page to examine
        self.stream.seek(startpos)
        self.free_clusters = FREE_CLUSTERS
        if DEBUG_FAT: logging.debug("map_free_space: %d clusters free in %d runs", FREE_CLUSTERS, len(self.free_clusters_map))
        return FREE_CLUSTERS, len(self.free_clusters_map)

    def findfree(self, start=2, count=0):
        """Return index and length of the first free clusters run beginning from
        'start' or (-1,0) in case of failure. If 'count' is given, limit the search
        to that amount."""
        if self.free_clusters_map == None:
            self.map_free_space()
        try:
            i, n = self.free_clusters_map.popitem(0)
        except KeyError:
            return -1, -1
        if DEBUG_FAT: logging.debug("got run of %d free clusters from #%x", n, i)
        if n-count > 0:
            self.free_clusters_map[i+count] = n-count # updates map
        self.free_clusters-=min(n,count)
        return i, min(n, count)
    
    def map_compact(self, strategy=0):
        "TODO: consolidate contiguous runs; sort by run size" 
        self.free_clusters_map = OrderedDict(sorted(self.free_clusters_map.items(), key=lambda t: t[0])) # sort by disk offset
        if DEBUG_FAT: logging.debug("Free space map (%d runs):\n%s", len(self.free_clusters_map), self.free_clusters_map)
        #~ print "Compacted free space map: %s" % self.free_clusters_map
        
    # TODO: split very large runs
    # About 12% faster injecting a Python2 tree
    def mark_run(self, start, count, clear=False):
        "Mark a range of consecutive FAT clusters (optimized for FAT16/32)"
        if self.bits == 12:
            while count:
                self[start] = (start+1, 0)(clear==True)
                start+=1
                count-=1
            return
        dsp = (start*self.bits)/8
        pos = self.offset+dsp
        self.stream.seek(pos)
        if clear:
            for i in xrange(start, start+count):
                self.decoded[i] = 0
            self.stream.write(bytearray(count*(self.bits/8)*'\x00'))
            return
        # consecutive values to set
        L = xrange(start+1, start+1+count)
        for i in L:
            self.decoded[i-1] = i
        self.decoded[start+count-1] = self.last
        # converted in final LE WORD/DWORD array
        L = map(lambda x: struct.pack(self.fat_slot_fmt, x), L)
        L[-1] = struct.pack(self.fat_slot_fmt, self.last)
        self.stream.write(bytearray().join(L))

    def alloc(self, count, start=2, first=0, runs_map=None):
        """Allocate a chain of free clusters and appropriately mark the FATs, optionally recording runs' start & length.
        Returns the first and last clusters or raise an exception in case of failure"""
        if DEBUG_FAT: logging.debug("request to allocate %d clusters from #%Xh(%d), search from #%Xh", count, start, start, self.last_free_alloc)

        self.map_compact()
        
        if self.free_clusters < count:
            if DEBUG_FAT: logging.debug("can't allocate clusters, there are only %d free", self.free_clusters)
            raise FATException("FATAL! Free clusters exhausted, couldn't allocate %d more!" % count)
        if DEBUG_FAT: logging.debug("ok to search, %d clusters free", self.free_clusters)

        last = start

        while count:
            i, n = self.findfree(self.last_free_alloc, count)
            #~ print "(1) findfree(%d, %d) returned (%d, %d)" %(self.last_free_alloc, count, i, n)
            if DEBUG_FAT: logging.debug("(1) findfree(%Xh, %d) returned (%Xh, %d)", self.last_free_alloc, count, i, n)
            if runs_map != None:
                #~ print "runs_map[%d] = %d"%(i,n)
                runs_map[i] = n
            if not first:
                first = i # save the first cluster in the chain
            else: # if we continue a chain...
                self[last] = i
            self.mark_run(i, min(count, n))
            last = i + min(count, n) - 1
            count -= n

        self.last_free_alloc = last

        if DEBUG_FAT: logging.debug("clusters successfully allocated from %Xh (last=%Xh)", first, last)
        return first, last

    def free(self, start, runs=None):
        "Free a clusters chain, one run at a time (except FAT12)"
        if runs:
            for run in runs:
                if DEBUG_FAT: logging.debug("free1: directly zeroing run of %d clusters from %Xh", runs[run], run)
                self.mark_run(run, runs[run], True)
                if not self.exfat:
                    self.free_clusters += runs[run]
                    self.free_clusters_map[run] = runs[run]
            return

        while True:
            length, next = self.count_run(start)
            if DEBUG_FAT: logging.debug("free1: count_run returned %d, %Xh", length, next)
            if not next: break
            if DEBUG_FAT: logging.debug("free1: zeroing run of %d clusters from %Xh (next=%Xh)", length, start, next)
            self.mark_run(start, length, True)
            if not self.exfat:
                self.free_clusters += length
                self.free_clusters_map[start] = length
            start = next



class Chain(object):
    "Open a cluster chain like a plain file"
    def __init__ (self, boot, fat, cluster, size=0, nofat=0, end=0):
        self.stream = boot.stream
        self.boot = boot
        self.fat = fat
        self.start = cluster # start cluster or zero if empty
        self.end = end # end cluster
        self.filesize = size # file size, if available
        self.size = rdiv(size, boot.cluster) * boot.cluster # clusters chain size
        if self.start:
            if not size or not end:
                # Get chain size & last cluster
                self.size, self.end = fat.count(cluster)
                self.size *= boot.cluster
        self.nofat = nofat # does not use FAT (=contig)
        self.pos = 0 # virtual stream linear pos
        # Virtual Cluster Number (cluster index in this chain)
        self.vcn = -1
        # Virtual Cluster Offset (current offset in VCN)
        self.vco = -1
        self.lastvlcn = (0, cluster) # last cluster VCN & LCN (=real cluster index in FAT)
        self.runs = OrderedDict() # RLE map of fragments
        if DEBUG_FAT: logging.debug("Cluster chain of %d%sbytes (%d bytes) @LCN %Xh:LBA %Xh)", self.filesize, (' ', ' contiguous ')[nofat], self.size, cluster, self.boot.cl2offset(cluster))

    def __str__ (self):
        return "Chain of %d (%d) bytes from LCN #%Xh (LBA %Xh)" % (self.filesize, self.size, self.start, self.boot.cl2offset(self.start))

    def _get_frags(self):
        "Maps the cluster runs composing the chain"
        start = self.start
        while 1:
            length, next = self.fat.count_run(start)
            self.runs[start] = length
            if next == start: break
            start = next
        if DEBUG_FAT: logging.debug("Fragments map for %s: %s", self, self.runs)

    def _alloc(self, count, start=2, first=0):
        "Allocates clusters and updates the runs map. Returns first and last allocated cluster LCN"
        #~ print "(0):_alloc(%d, %d, %d):%s"%(count, start,first,self.runs)
        st, en = self.fat.alloc(count, start, first, self.runs) # st is first in chain, not in new fragment
        #~ print "(1):_alloc:%s"%self.runs
        # Merges consecutive runs
        d=copy.copy(self.runs)
        for k,v in self.runs.iteritems():
            v1 = d.get(k+v)
            if v1: # if contig run exists, merge
                if DEBUG_FAT: logging.debug("Compacting clusters map: {%d:%d} -> {%d:%d}", k,v,k,v+v1)
                del d[k+v]
                d[k] = v+v1
        self.runs = d
        #~ print "(2):_alloc:%s"%self.runs
        return st, en

    def maxrun4len(self, length):
        "Returns the longest run of clusters, up to 'length' bytes, from current position"
        if not self.runs:
            self._get_frags()
        n = rdiv(length, self.boot.cluster) # contig clusters searched for
        items = self.runs.items()
        for start, count in items:
            # if current LCN is in run
            if start <= self.lastvlcn[1] < start+count:
                i = items.index((start, count))
                count = start+count-self.lastvlcn[1] # clusters to end of run
                if count == n:
                    if i+1 == len(items):
                        next = start+count-1 # last of chain
                    else:
                        next = items[i+1][0] # first of next run
                else:
                    next = self.lastvlcn[1] + n # n-th in the same run
                count = min(n, count) # eventually cut to searched amount
                maxchunk = count * self.boot.cluster
                if DEBUG_FAT: logging.debug("maxrun4len: run of %d bytes (%d clusters) from VCN #%d (first,next LCN=%Xh,%Xh)", maxchunk, n, self.lastvlcn[0], self.lastvlcn[1], next)
                break
        # Updates VCN & next LCN
        self.lastvlcn = (self.lastvlcn[0]+n, next)
        return maxchunk

    def tell(self): return self.pos

    def realtell(self):
        return self.boot.cl2offset(self.lastvlcn[1])+self.vco

    def seek(self, offset, whence=0):
        if whence == 1:
            self.pos += offset
        elif whence == 2:
            if self.size:
                self.pos = self.size - offset
        else:
            self.pos = offset
        # if emtpy chain, allocate some clusters
        if self.pos and not self.size:
            clusters = rdiv(self.pos, self.boot.cluster)
            self.start, self.end = self._alloc(clusters)
            self.size = clusters * self.boot.cluster
            if DEBUG_FAT: logging.debug("Chain.seek:allocated %d clusters from #%Xh seeking %X", clusters, self.start, self.pos)
        # file size is the top pos reached during seek
        self.filesize = max(self.filesize, self.pos)
        self.vcn = self.pos / self.boot.cluster # n-th cluster chain
        self.vco = self.pos % self.boot.cluster # offset in it
        self.realseek()

    def realseek(self):
        if DEBUG_FAT: logging.debug("realseek with VCN=%d VCO=%d", self.vcn,self.vco)
        if self.size and self.pos >= self.size:
            if DEBUG_FAT: logging.debug("%s: detected chain end at VCN #%d while seeking", self, self.vcn)
            self.vcn = -1
            return
        if self.nofat:
            cluster = self.start + self.vcn
            if cluster > self.start + self.size/self.boot.cluster:
                self.vcn = -1
        else:
            # If we've reached a preceding and nearer cluster already...
            if self.lastvlcn[0] < self.vcn:
                si = self.lastvlcn[0]
                cluster = self.lastvlcn[1]
            else:
                si = 0
                cluster = self.start
            for i in xrange(si, self.vcn):
                cluster = self.fat[cluster]
                #~ if not self.fat.isvalid(cluster):
                if not ((cluster >= 2 and cluster <= self.fat.real_last) or \
                (self.fat.last <= cluster <= self.fat.last+7) or \
                cluster == self.fat.bad):
                    raise FATException("End of stream reached!")
            self.lastvlcn = (self.vcn, cluster)
            #~ if self.fat.islast(cluster):
            if (self.fat.last <= cluster <= self.fat.last+7):
                self.vcn = -1
        if DEBUG_FAT: logging.debug("realseek seeking VCN=%d LCN=%Xh [%Xh:] @%Xh", self.vcn, cluster, self.vco, self.boot.cl2offset(cluster))
        self.stream.seek(self.boot.cl2offset(cluster)+self.vco)

    def read(self, size=-1):
        if DEBUG_FAT: logging.debug("read(%d) called from offset %Xh", size, self.pos)
        self.seek(self.pos)
        # If negative size, set it to file size
        if size < 0:
            size = self.filesize
        # If requested size is greater than file size, limit to the latter
        if self.pos + size > self.filesize:
            size = self.filesize - self.pos
            if size < 0: size = 0
        buf = bytearray()
        if self.nofat: # contiguous clusters
            if not size or self.vcn == -1:
                return buf
            buf += self.stream.read(size)
            self.pos += size
            if DEBUG_FAT: logging.debug("read %d contiguous bytes, VCN=%Xh[%Xh:]", len(buf), self.vcn, self.vco)
            return buf
        while 1:
            if not size or self.vcn == -1:
                break
            n = min(size, self.maxrun4len(size))
            buf += self.stream.read(n)
            size -= n
            self.pos += n
            self.seek(self.pos)
        if DEBUG_FAT: logging.debug("read %d byte, VCN=%Xh[%Xh:]", len(buf), self.vcn, self.vco)
        return buf

    def write(self, s):
        self.seek(self.pos)
        if DEBUG_FAT: logging.debug("Chain.write(buf[:%d]) called from offset %Xh, VCN=%Xh[%Xh:]", len(s), self.pos, self.vcn, self.vco)
        new_allocated = 0
        if self.pos + len(s) > self.size:
            # Alloc more clusters from actual last one
            reqb = self.pos + len(s) - self.size
            reqc = rdiv(reqb, self.boot.cluster)
            if DEBUG_FAT: logging.debug("pos=%X(%d), len=%d, size=%d(%Xh)", self.pos, self.pos, len(s), self.size, self.size)
            if DEBUG_FAT: logging.debug("needed %d bytes (%d clusters) more to continue writing", reqb, reqc)
            if self.start:
                lastc = self.end
                self.end = self._alloc(reqc, lastc, lastc)[1] # first=lastc since we continue the chain
                self.size += reqc*self.boot.cluster
            else:
                # if chain is empty, again, simply allocate the clusters...
                self.start, self.end = self._alloc(reqc)
                # ...and force a seek on the real medium
                self.size = reqc*self.boot.cluster
                self.seek(self.pos)
            new_allocated = 1
        i = 0
        btoe = self.boot.cluster - self.vco # bytes to cluster's end
        if len(s) > btoe:
            if DEBUG_FAT: logging.debug("writing %d bytes to end of cluster", btoe)
            self.seek(self.pos)
            self.stream.write(s[:btoe])
            self.pos += btoe
            i += btoe
        while i < len(s):
            self.seek(self.pos)
            # write minimum between s rest and maximum contig run
            n = min(len(s)-i, self.maxrun4len(len(s)-i))
            self.stream.write(s[i:i+n])
            self.pos += n
            i += n
            if DEBUG_FAT: logging.debug("written s[%d:%d] for %d contiguous bytes (todo=%d)", i-n, i, n, len(s)-i)
        #~ # file size is the top pos reached during write
        #~ self.filesize = max(self.filesize, self.pos)
        self.seek(self.pos)
        # Windows FS zeroes the last written sector only, not the full cluster remainder
        # When allocating a directory table, it is strictly necessary that only the first byte in
        # an empty slot is set to NULL
        if new_allocated:
            if self.pos < self.size:
                if DEBUG_FAT: logging.debug("blanking newly allocated cluster tip, %d bytes @%Xh", self.size-self.pos, self.pos)
                self.stream.write(bytearray(self.size - self.pos))

    def trunc(self):
        "Truncates the clusters chain to the current one, freeing the rest"
        x = self.pos/self.boot.cluster # last VCN (=actual) to set
        n = rdiv(self.size, self.boot.cluster) - x - 1 # number of clusters to free
        if DEBUG_FAT: logging.debug("%s: truncating @VCN %d, freeing %d clusters", self, x, n)
        if not n:
            if DEBUG_FAT: logging.debug("nothing to truncate!")
            return 1
        # Updates chain and virtual stream sizes
        self.size = (x+1)*self.boot.cluster
        self.filesize = self.pos
        #~ print "%s: truncating @VCN %d, freeing %d clusters. %d %d" % (self, x, n, self.pos, self.size)
        #~ print "Start runs:\n", self.runs
        while 1:
            if not n: break
            start, length = self.runs.popitem()
            if n >= length:
                #~ print "Zeroing %d from %d" % (length, start)
                self.fat.mark_run(start, length, True)
                if n == length:
                    k = self.runs.keys()[-1]
                    self.fat[k+self.runs[k]-1] = self.fat.last
                    #~ print "last=%d"%(k+self.runs[k]-1) 
                n -= length
            else:
                #~ print "Zeroing %d from %d, last=%d" % (n, start+length-n, start+length-n-1)
                self.fat.mark_run(start+length-n, n, True)
                self.fat[start+length-n-1] = self.fat.last
                self.runs[start] = length-n
                n=0
        #~ print "Final runs:\n", self.runs
        #~ for start, length in self.runs.items():
            #~ for i in range(length):
                #~ print "Cluster %d=%d"%(start+i, self.fat[start+i])
        return 0

    def frags(self):
        if DEBUG_FAT: logging.debug("Fragmentation of %s", self)
        if DEBUG_FAT: logging.debug("Detected %d fragments for %d clusters", len(self.runs), self.size/self.boot.cluster)
        if DEBUG_FAT: logging.debug("Fragmentation is %f", float(len(self.runs)-1) / float(self.size/self.boot.cluster))
        return len(self.runs)



class FixedRoot(object):
    "Handle the FAT12/16 fixed root table like a file"
    def __init__ (self, boot, fat):
        self.stream = boot.stream
        self.boot = boot
        self.fat = fat
        self.start = boot.root()
        self.size = 32*boot.wMaxRootEntries
        self.pos = 0 # virtual stream linear pos

    def __str__ (self):
        return "Fixed root @%Xh" % self.start

    def tell(self): return self.pos

    def realtell(self):
        return self.stream.tell()

    def seek(self, offset, whence=0):
        if whence == 1:
            pos = self.pos + offset
        elif whence == 2:
            if self.size:
                pos = self.size - offset
        else:
            pos = offset
        if pos > self.size:
            if DEBUG_FAT: logging.debug("Attempt to seek @%Xh past fixed root end @%Xh", pos, self.size)
            return
        self.pos = pos
        if DEBUG_FAT: logging.debug("FixedRoot: seeking @%Xh (@%Xh)", pos, self.start+pos)
        self.stream.seek(self.start+pos)

    def read(self, size=-1):
        if DEBUG_FAT: logging.debug("FixedRoot: read(%d) called from offset %Xh", size, self.pos)
        self.seek(self.pos)
        # If negative size, adjust
        if size < 0:
            size = 0
            if self.size: size = self.size
        # If requested size is greater than file size, limit to the latter
        if self.size and self.pos + size > self.size:
            size = self.size - self.pos
        buf = bytearray()
        if not size: return buf
        buf += self.stream.read(size)
        self.pos += size
        return buf

    def write(self, s):
        if DEBUG_FAT: logging.debug("FixedRoot: writing %d bytes at offset %Xh", len(s), self.pos)
        self.seek(self.pos)
        if self.pos + len(s) > self.size:
            return
        self.stream.write(s)
        self.pos += len(s)
        self.seek(self.pos)

    def trunc(self):
        return 0

    def frags(self):
        pass


class Handle(object):
    "Manage an open table slot"
    def __init__ (self):
        self.IsValid = False # determines whether update or not on disk
        self.File = None # file contents
        self.Entry = None # direntry slot
        self.Dir = None #dirtable owning the handle
        self.IsReadOnly = True # use this to prevent updating a Direntry on a read-only filesystem
        atexit.register(self.close) # forces close() on exit if user didn't call it

    def __del__ (self):
        self.close()

    def update_time(self, i=0):
        cdate, ctime = FATDirentry.GetDosDateTime()
        if i == 0:
            self.Entry.wADate = cdate
        elif i == 1:
            self.Entry.wMDate = cdate
            self.Entry.wMTime = ctime

    def tell(self):
        return self.File.tell()

    def seek(self, offset, whence=0):
        self.File.seek(offset, whence)

        self.Entry.dwFileSize = self.File.filesize
        self.Dir._update_dirtable(self.Entry)

    def read(self, size=-1):
        self.update_time()
        return self.File.read(size)

    def write(self, s):
        self.File.write(s)
        self.update_time(1)
        self.IsReadOnly = False

        self.Entry.dwFileSize = self.File.filesize
        self.Dir._update_dirtable(self.Entry)

    # NOTE: FAT permits chains with more allocated clusters than those required by file size!
    # Distinguish a ftruncate w/deallocation and update Chain.__init__ and Handle flushing accordingly!
    def ftruncate(self, length, free=0):
        "Truncates a file to a given size (eventually allocating more clusters), optionally unlinking clusters in excess."
        self.File.seek(length)
        self.File.filesize = length

        self.Entry.dwFileSize = self.File.filesize
        self.Dir._update_dirtable(self.Entry)

        if not free:
            return 0
        return self.File.trunc()

    def close(self):
        if not self.IsValid:
            return

        # Force setting the start cluster if allocated on write
        self.Entry.Start(self.File.start)

        if not self.Entry.IsDir():
            if self.Entry._buf[-32] == 0xE5 and self.Entry.Start():
                if DEBUG_FAT: logging.debug("Deleted file: deallocating cluster(s)")
                self.File.fat.free(self.Entry.Start())
                # updates the Dirtable cache: mandatory if we allocated on write
                # (or start cluster won't be set)
                self.Dir._update_dirtable(self.Entry)
                return

            self.Entry.dwFileSize = self.File.filesize

            # 25.05.17: an empty file can legally keep clusters allocated for future needs! 
            # Free cluster allocated if empty at last
            #~ if not self.Entry.dwFileSize and self.Entry.Start():
                #~ if DEBUG_FAT: logging.debug("Empty file: deallocating cluster(s)")
                #~ self.File.fat.free(self.Entry.Start())
                #~ self.Entry.wClusterHi = 0
                #~ self.Entry.wClusterLo = 0

        self.Dir.stream.seek(self.Entry._pos)
        if DEBUG_FAT: logging.debug('Closing Handle @%Xh(%Xh) to "%s", cluster=%Xh tell=%d chain=%d size=%d', \
        self.Entry._pos, self.Dir.stream.realtell(), os.path.join(self.Dir.path,self.Entry.Name()), self.Entry.Start(), self.File.pos, self.File.size, self.File.filesize)
        self.Dir.stream.write(self.Entry.pack())
        self.IsValid = False
        self.Dir._update_dirtable(self.Entry)


class Direntry(object):
    pass

DirentryType = type(Direntry())


class FATDirentry(Direntry):
    "Represents a FAT direntry of one or more slots"

    "Represents a 32 byte FAT (not exFAT) slot"
    layout = { # { offset: (name, unpack string) }
    0x00: ('sName', '8s'),
    0x08: ('sExt', '3s'),
    0x0B: ('chDOSPerms', 'B'),
    0x0C: ('chFlags', 'B'), # bit 3/4 set: lowercase basename/extension (NT)
    0x0D: ('chReserved', 'B'), # creation time fine resolution in 10 ms units, range 0-199
    0x0E: ('wCTime', '<H'),
    0x10: ('wCDate', '<H'),
    0x12: ('wADate', '<H'),
    0x14: ('wClusterHi', '<H'),
    0x16: ('wMTime', '<H'),
    0x18: ('wMDate', '<H'),
    0x1A: ('wClusterLo', '<H'),
    0x1C: ('dwFileSize', '<I') }

    "Represents a 32 byte FAT LFN slot"
    layout_lfn = { # { offset: (name, unpack string) }
    0x00: ('chSeqNumber', 'B'), # LFN slot #
    0x01: ('sName5', '10s'),
    0x0B: ('chDOSPerms', 'B'), # always 0xF
    0x0C: ('chType', 'B'), # always zero in VFAT LFN
    0x0D: ('chChecksum', 'B'),
    0x0E: ('sName6', '12s'),
    0x1A: ('wClusterLo', '<H'), # always zero
    0x1C: ('sName2', '4s') }

    def __init__ (self, s, pos=-1):
        self._i = 0
        self._buf = s
        self._pos = pos
        self._kv = {}
        for k in self.layout:
            self._kv[k-32] = self.layout[k]
        self._vk = {} # { name: offset}
        for k, v in self._kv.items():
            self._vk[v[0]] = k

    __getattr__ = utils.common_getattr

    def pack(self):
        "Updates internal buffer"
        s = ''
        keys = self._kv.keys()
        keys.sort()
        for k in keys:
            v = self._kv[k]
            s += struct.pack(v[1], getattr(self, v[0]))
        self._buf[-32:] = bytearray(s) # update always non-LFN part
        return self._buf

    def __str__ (self):
        s = "FAT %sDirentry @%Xh\n" % ( ('','LFN ')[self.IsLfn()], self._pos )
        return utils.class2str(self, s)

    def IsLfn(self):
        return self._buf[0x0B] == 0x0F and self._buf[0x0C] == self._buf[0x1A] == self._buf[0x1B] == 0

    def IsDeleted(self):
        return self._buf[0] == 0xE5

    def IsDir(self, value=-1):
        "Gets or sets the slot's Dir DOS permission"
        if value != -1:
            self._buf[-21] = value
        return (self._buf[-21] & 0x10) == 0x10

    def IsLabel(self, mark=0):
        "Gets or sets the slot's Label DOS permission"
        if mark:
            self._buf[0x0B] = 0x08
        return self._buf[0x0B] == 0x08

    def Start(self, cluster=None):
        "Gets or sets cluster WORDs in slot"
        if cluster != None:
            self.wClusterHi = cluster >> 16
            self.wClusterLo = cluster & 0xFFFF
        return (self.wClusterHi<<16) | self.wClusterLo

    def LongName(self):
        if not self.IsLfn():
            return ''
        i = len(self._buf)-64
        ln = ''
        while i >= 0:
            ln += self._buf[i+1:i+1+10].decode('utf-16le') + \
            self._buf[i+14:i+14+12].decode('utf-16le') + \
            self._buf[i+28:i+28+4].decode('utf-16le')
            i -= 32
        i = ln.find('\x00') # ending NULL may be omitted!
        if i < 0:
            return ln
        else:
            return ln[:i]

    def ShortName(self):
        return self.GenShortName(self._buf[-32:-21], self.chFlags)

    def Name(self):
        return self.LongName() or self.ShortName()

    @staticmethod
    def ParseDosDate(wDate):
        "Decodes a DOS date WORD into a tuple (year, month, day)"
        return (wDate>>9)+1980, (wDate>>5)&0xF, wDate&0x1F

    @staticmethod
    def ParseDosTime(wTime):
        "Decodes a DOS time WORD into a tuple (hour, minute, second)"
        return wTime>>11, (wTime>>5)&0x3F, wTime&0x1F

    @staticmethod
    def MakeDosTime(t):
        "Encodes a tuple (hour, minute, second) into a DOS time WORD"
        return (t[0] << 11) | (t[1] << 5) | (t[2]/2)

    @staticmethod
    def MakeDosDate(t):
        "Encodes a tuple (year, month, day) into a DOS date WORD"
        return ((t[0]-1980) << 9) | (t[1] << 5) | (t[2])

    @staticmethod
    def GetDosDateTime(format=0):
        "Returns a 2 WORDs tuple (DOSDate, DOSTime) or a DWORD, representing DOS encoding of current datetime"
        tm = time.localtime()
        cdate = ((tm[0]-1980) << 9) | (tm[1] << 5) | (tm[2])
        ctime = (tm[3] << 11) | (tm[4] << 5) | (tm[5]/2)
        if format:
            return ctime<<16 | cdate # DWORD
        else:
            return (cdate, ctime)

    @staticmethod
    def GenRawShortName(name):
        "Generates an old-style 8+3 DOS short name"
        name, ext = os.path.splitext(name)
        chFlags = 0
        if not ext and name in ('.', '..'): # special case
            name = '%-11s' % name
        elif 1 <= len(name) <= 8 and len(ext) <= 4:
            if ext and ext[0] == '.':
                ext = ext[1:]
            if name.islower():
                chFlags |= 8
            if ext.islower():
                chFlags |= 16
            name = '%-8s%-3s' % (name, ext)
            name = name.upper()
        if DEBUG_FAT: logging.debug("GenRawShortName returned %s:%d",name,chFlags)
        return name, chFlags

    @staticmethod
    def GenShortName(shortname, chFlags=0):
        "Makes a human readable short name from slot's one"
        shortname=str(shortname)
        name = shortname[:8].rstrip()
        if chFlags & 0x8: name = name.lower()
        ext = shortname[8:].rstrip()
        if chFlags & 0x16: ext = ext.lower()
        if DEBUG_FAT: logging.debug("GenShortName returned %s.%s",name,ext)
        if not ext: return name
        return name + '.' + ext

    @staticmethod
    def GenRawShortFromLongName(name, id=1):
        "Generates a DOS 8+3 short name from a long one (Windows 95 style)"
        # Replaces valid LFN chars prohibited in short name
        nname = name.replace(' ', '')
        # CAVE! Multiple dots?
        for c in '[]+,;=':
            nname = nname.replace(c, '_')
        nname, ext = os.path.splitext(nname)
        #~ print nname, ext
        # If no replacement and name is short (LIBs -> LIBS)
        if len(nname) < 9 and nname in name and ext in name:
            if DEBUG_FAT: logging.debug("GenRawShortFromLongName (0) returned %s:%s",nname,ext[1:4])
            return (nname + ext[1:4]).upper()
        # Windows 9x: ~1 ... ~9999... as needed
        tilde = '~%d' % id
        i = 8 - len(tilde)
        if i > len(nname): i = len(nname)
        if DEBUG_FAT: logging.debug("GenRawShortFromLongName (1) returned %s:%s",nname[:i]+tilde,ext[1:4])
        return (nname[:i] + tilde + ext[1:4]).upper()

    @staticmethod
    def GenRawShortFromLongNameNT(name, id=1):
        "Generates a DOS 8+3 short name from a long one (NT style)"
        if id < 5: return FATDirentry.GenRawShortFromLongName(name, id)
        #~ There's an higher probability of generating an unused alias at first
        #~ attempt, and an alias mathematically bound to its long name
        crc = crc32(name) & 0xFFFF
        longname = name
        name, ext = os.path.splitext(name)
        tilde = '~%d' % (id-4)
        i = 6 - len(tilde)
        # Windows NT 4+: ~1...~4; then: orig chars (1 or 2)+some CRC-16 (4 chars)+~1...~9
        # Expands tilde index up to 999.999 if needed like '95
        shortname = (name[:2] + hex(crc)[::-1][:i] + tilde + ext[1:4]).upper()
        if DEBUG_FAT: logging.debug("Generated NT-style short name %s for %s", shortname, longname)
        return shortname

    def GenRawSlotFromName(self, shortname, longname=None):
        # Is presence of invalid (Unicode?) chars checked?
        shortname, chFlags = self.GenRawShortName(shortname)

        cdate, ctime = self.GetDosDateTime()

        self._buf = bytearray(struct.pack('<11s3B7HI', shortname, 0x20, chFlags, 0, ctime, cdate, cdate, 0, ctime, cdate, 0, 0))

        if longname:
            longname = longname.decode('mbcs').encode('utf-16le') # CHECK if 'mbcs' is good for Linux!
            if len(longname) > 510:
                raise FATException("Long name '%s' is >255 characters!" % longname)
            csum = self.Checksum(shortname)
            # If the last slot isn't filled, we must NULL terminate
            if len(longname) % 26:
                longname += '\x00\x00'
            # And eventually pad with 0xFF, also
            if len(longname) % 26:
                longname += '\xFF'*(26 - len(longname)%26)
            slots = len(longname)/26
            B=bytearray()
            while slots:
                b = bytearray(32)
                b[0] = slots
                j = (slots-1)*26
                b[1:11] = longname[j: j+10]
                b[11] = 0xF
                b[13] = csum
                b[14:27] = longname[j+10: j+22]
                b[28:32] = longname[j+22: j+26]
                B += b
                slots -= 1
            B[0] = B[0] | 0x40 # mark the last slot (first to appear)
            self._buf = B+self._buf

    @staticmethod
    def IsShortName(name):
        "Checks if name is an old-style 8+3 DOS short name"
        is_8dot3 = False
        name, ext = os.path.splitext(name)
        if not ext and name in ('.', '..'): # special case
            is_8dot3 = True
        # name.txt or NAME.TXT --> short
        # Name.txt or name.Txt etc. --> long (preserve case)
        # NT: NAME.txt or name.TXT or name.txt (short, bits 3-4 in 0x0C set accordingly)
        # tix8.4.3 --> invalid short (name=tix8.4, ext=.3)
        # dde1.3 --> valid short, (name=dde1, ext=.3)
        elif 1 <= len(name) <= 8 and len(ext) <= 4 and (name==name.upper() or name==name.lower()):
            if FATDirentry.IsValidDosName(name):
                is_8dot3 = True
        return is_8dot3

    special_short_chars = ''' "*/:<>?\|[]+.,;=''' + ''.join([chr(c) for c in range(32)])
    special_lfn_chars = '''"*/:<>?\|''' + ''.join([chr(c) for c in range(32)])

    @staticmethod
    def IsValidDosName(name, lfn=False):
        if name[0] == '\xE5': return False
        if lfn:
            special = FATDirentry.special_lfn_chars
        else:
            special = FATDirentry.special_short_chars
        for c in special:
            if c in name:
                return False
        return True

    def Match(self, name):
        "Checks if given short or long name matches with this slot's name"
        n =name.lower()
        # File.txt (LFN) == FILE.TXT == file.txt (short with special bits set) etc.
        if n == self.LongName().lower() or n == self.ShortName().lower(): return True
        return False

    @staticmethod
    def Checksum(name):
        "Calculates the 8+3 DOS short name LFN checksum"
        sum = 0
        for c in name:
            sum = ((sum & 1) << 7) + (sum >> 1) + ord(c)
            sum &= 0xff
        return sum



class Dirtable(object):
    "Manages a FAT12/16/32 directory table"
    dirtable = {} # {cluster: {'LFNs':{}, 'Names':{}}}

    def __init__(self, boot, fat, startcluster, size=0, path='.'):
        # non-zero size is a special case for fixed FAT12/16 root
        self.boot = boot
        self.fat = fat
        self.start = startcluster
        self.path = path
        self.slots_map = {} # RLE map of free slots
        if startcluster == size == 0: # FAT12/16 fixed root
            self.stream = FixedRoot(boot, fat)
            self.fixed_size = self.stream.size
        else:
            tot, last = fat.count(startcluster)
            self.stream = Chain(boot, fat, startcluster, (boot.cluster*tot, size)[size>0], end=last)
        if startcluster not in Dirtable.dirtable:
            Dirtable.dirtable[startcluster] = {'LFNs':{}, 'Names':{}} # LFNs key MUST be Unicode!
        self.map_slots()

    def __str__ (self):
        s = "Directory table @LCN %X (LBA %Xh)" % (self.start, self.boot.cl2offset(self.start))
        return s
        
    def getdiskspace(self):
        "Returns the disk free space in a tuple (clusters, bytes)"
        free_bytes = self.fat.free_clusters * self.boot.cluster
        return (self.fat.free_clusters, free_bytes)

    def open(self, name):
        "Opens the chain corresponding to an existing file name"
        res = Handle()
        if type(name) != DirentryType:
            root, fname = os.path.split(name)
            if root:
                root = self.opendir(root)
                if not root:
                    return res
            else:
                root = self
            e = root.find(fname)
        else: # We assume it's a Direntry if not a string
            e = name
        if e:
            # Ensure it is not a directory or volume label
            if e.IsDir() or e.IsLabel():
                return res
            res.IsValid = True
            # If cluster is zero (empty file), then we must allocate one:
            # or Chain won't work!
            res.File = Chain(self.boot, self.fat, e.Start(), e.dwFileSize)
            res.Entry = e
            res.Dir = self
        return res

    def opendir(self, name):
        """Opens an existing relative directory path beginning in this table and
        return a new Dirtable object or None if not found"""
        name = name.replace('/','\\')
        path = name.split('\\')
        found = self
        for com in path:
            e = found.find(com)
            if e and e.IsDir():
                found = Dirtable(self.boot, self.fat, e.Start(), path=os.path.join(found.path, com))
                continue
            found = None
            break
        if DEBUG_FAT and found: logging.debug("opened directory table '%s' @%Xh (cluster %Xh)", found.path, self.boot.cl2offset(found.start), found.start)
        return found

    def _alloc(self, name, clusters=0):
        "Allocates a new Direntry slot (both file/directory)"
        if len(os.path.join(self.path, name))+2 > 260:
            raise FATException("Can't add '%s' to directory table '%s', pathname >260!"%(name, self.path))
        dentry = FATDirentry(bytearray(32))
        # If name is a LFN, generate a short one valid in this table
        if not FATDirentry.IsShortName(name):
            i = 1
            short = FATDirentry.GenShortName(FATDirentry.GenRawShortFromLongNameNT(name, i))
            while self.find(short):
                i += 1
                short = FATDirentry.GenShortName(FATDirentry.GenRawShortFromLongNameNT(name, i))
            dentry.GenRawSlotFromName(short, name)
        else:
            dentry.GenRawSlotFromName(name)

        pos = self.findfree(len(dentry._buf))
        dentry._pos = pos

        res = Handle()
        res.IsValid = True
        if clusters:
            start, last = self.fat.alloc(clusters)
        else:
            start, last = 0, 0
        res.File = Chain(self.boot, self.fat, start, end=last)
        dentry.Start(res.File.start)
        res.Entry = dentry
        return res

    def create(self, name, prealloc=0):
        "Creates a new file chain and the associated slot. Erase pre-existing filename."
        e = self.open(name)
        if e.IsValid:
            e.IsValid = False
            self.erase(name)
        # Check if it is a supported name (=at least valid LFN)
        if not FATDirentry.IsValidDosName(name, True):
            raise FATException("Invalid characters in name '%s'" % name)
        handle = self._alloc(name, prealloc)
        self.stream.seek(handle.Entry._pos)
        self.stream.write(handle.Entry.pack())
        handle.Dir = self
        self._update_dirtable(handle.Entry)
        if DEBUG_FAT: logging.debug("Created new file '%s' @%Xh", name, handle.File.start)
        return handle

    def mkdir(self, name):
        "Creates a new directory slot, allocating the new directory table"
        r = self.opendir(name)
        if r:
            if DEBUG_FAT: logging.debug("mkdir('%s') failed, entry already exists!", name)
            return r
        # Check if it is a supported name (=at least valid LFN)
        if not FATDirentry.IsValidDosName(name, True):
            if DEBUG_FAT: logging.debug("mkdir('%s') failed, name contains invalid chars!", name)
            return None
        handle = self._alloc(name, 1)
        self.stream.seek(handle.Entry._pos)
        if DEBUG_FAT: logging.debug("Making new directory '%s' @%Xh", name, handle.File.start)
        handle.Entry.chDOSPerms = 0x10
        self.stream.write(handle.Entry.pack())
        handle.Dir = self
        # PLEASE NOTE: Windows 10 opens a slot as directory and works regularly
        # even if table does not start with dot entries: but CHKDSK corrects it!
        # . in new table
        dot = FATDirentry(bytearray(32), 0)
        dot.GenRawSlotFromName('.')
        dot.Start(handle.Entry.Start())
        dot.chDOSPerms = 0x10
        handle.File.write(dot.pack())
        # .. in new table
        dot = FATDirentry(bytearray(32), 32)
        dot.GenRawSlotFromName('..')
        # Non-root parent's cluster # must be set
        if self.path != '.':
            dot.Start(self.stream.start)
        dot.chDOSPerms = 0x10
        handle.File.write(dot.pack())
        handle.File.write(bytearray(self.boot.cluster-64)) # blank table
        self._update_dirtable(handle.Entry)
        handle.close()
        return self.opendir(name)

    def rmtree(self, name=None):
        "Removes a full directory tree"
        if name:
            if DEBUG_FAT: logging.debug("rmtree:opening %s", name)
            target = self.opendir(name)
        else:
            target = self
        if not target:
            if DEBUG_FAT: logging.debug("rmtree:target '%s' not found!", name)
            return 0
        for it in target.iterator():
            n = it.Name()
            if it.IsDir():
                if n in ('.', '..'): continue
                target.opendir(n).rmtree()
            if DEBUG_FAT: logging.debug("rmtree:erasing '%s'", n)
            target.erase(n)
        del target
        if name:
            if DEBUG_FAT: logging.debug("rmtree:erasing '%s'", name)
            self.erase(name)
        return 1

    def close(self, handle):
        "Updates a modified entry in the table"
        handle.close()

    def map_compact(self):
        "Compacts slots map"
        d=copy.copy(self.slots_map)
        for k,v in self.slots_map.iteritems():
            v1 = self.slots_map.get(k+32*v)
            if v1: # if contig run exists, merge
                if DEBUG_FAT: logging.debug("Compacting slots map: {%d:%d} -> {%d:%d}", k,v,k,v+v1)
                del d[k+32*v]
                d[k] = v+v1
        self.slots_map = d

    def map_slots(self):
        "Fills the free slots map and file names table once at first access"
        if not self.slots_map:
            self.stream.seek(0)
            pos = 0
            s = ''
            while True:
                first_free = -1
                run_length = -1
                buf = bytearray()
                while True:
                    s = self.stream.read(32)
                    if not s or not s[0]: break
                    if s[0] == 0xE5: # if erased
                        if first_free < 0:
                            first_free = pos
                            run_length = 0
                        run_length += 1
                        pos += 32
                        continue
                    # if not, and we record an erased slot...
                    if first_free > -1:
                        self.slots_map[first_free] = run_length
                        first_free = -1
                    if s[0x0B] == 0x0F and s[0x0C] == s[0x1A] == s[0x1B] == 0: # LFN
                        buf += s
                        pos += 32
                        continue
                    # if normal, in-use slot
                    buf += s
                    pos += 32
                    self._update_dirtable(FATDirentry(buf, pos-len(buf)))
                    buf = bytearray()
                if not s or not s[0]:
                    # Maps unallocated space to max table size
                    if self.path == '.' and hasattr(self, 'fixed_size'): # FAT12/16 root
                        self.slots_map[pos] = (self.fixed_size - pos)/32
                    else:
                        self.slots_map[pos] = ((2<<20) - pos)/32
                    break
            self.map_compact()
            if DEBUG_FAT: logging.debug("%s collected slots map: %s", self, self.slots_map)
            if DEBUG_FAT: logging.debug("%s dirtable: %s", self, Dirtable.dirtable[self.start])
        
    # Assume table free space is zeroed
    def findfree(self, length=32):
        "Returns the offset of the first free slot or requested slot group size (in bytes)"
        length /= 32 # convert length in slots
        if DEBUG_FAT: logging.debug("%s: findfree(%d) in map: %s", self, length, self.slots_map)
        for start in sorted(self.slots_map):
            rl = self.slots_map[start]
            if length > 1 and length > rl: continue
            del self.slots_map[start]
            if length < rl:
                self.slots_map[start+32*length] = rl-length # updates map
            if DEBUG_FAT: logging.debug("%s: found free slot @%d, updated map: %s", self, start, self.slots_map)
            return start
        # FAT table limit is 2 MiB or 65536 slots (65534 due to "." and ".." entries)
        # So it can hold max 65534 files (all with short names)
        # FAT12&16 root have significantly smaller size (typically 224 or 512*32)
        raise FATException("Directory table of '%s' has reached its maximum extension!" % self.path)

    def iterator(self):
        "Iterates through directory table slots, generating a FATDirentry for each one"
        told = self.stream.tell()
        buf = bytearray()
        s = 1
        pos = 0
        while s:
            self.stream.seek(pos)
            s = self.stream.read(32)
            pos += 32
            if not s or s[0] == 0: break
            if s[0] == 0xE5: continue
            if s[0x0B] == 0x0F and s[0x0C] == s[0x1A] == s[0x1B] == 0: # LFN
                buf += s
                continue
            buf += s
            yield FATDirentry(buf, self.stream.tell()-len(buf))
            buf = bytearray()
        self.stream.seek(told)

    def _update_dirtable(self, it, erase=False):
        "Updates internal cache of object names and their associated slots"
        if erase:
            del Dirtable.dirtable[self.start]['Names'][it.ShortName().lower()]
            ln = it.LongName()
            if ln:
                del Dirtable.dirtable[self.start]['LFNs'][ln.lower()]
            return
        Dirtable.dirtable[self.start]['Names'][it.ShortName().lower()] = it
        ln = it.LongName()
        if ln:
            Dirtable.dirtable[self.start]['LFNs'][ln.lower()] = it

    def find(self, name):
        "Finds an entry by name. Returns it or None if not found"
        # Create names cache
        if not Dirtable.dirtable[self.start]['Names']:
            self.map_slots()
        if DEBUG_FAT: logging.debug("find: searching for %s (%s lower-cased)", name, name.lower())
        if DEBUG_FAT: logging.debug("find: LFNs=%s", Dirtable.dirtable[self.start]['LFNs'])
        name = name.decode('mbcs').lower()
        return Dirtable.dirtable[self.start]['LFNs'].get(name) or \
        Dirtable.dirtable[self.start]['Names'].get(name)

    def erase(self, name):
        "Marks a file's slot as erased and free the corresponding cluster chain"
        if type(name) == DirentryType:
            e = name
        else:
            e = self.find(name)
            if not e:
                return 0
        if e.IsDir():
            it = self.opendir(e.Name()).iterator()
            it.next(); it.next()
            if next in it:
                if DEBUG_FAT: logging.debug("Can't erase non empty directory slot @%d (pointing at #%d)", e._pos, e.Start())
                return 0
        start = e.Start()
        e.Start(0)
        e.dwFileSize = 0
        self._update_dirtable(e, True)
        for i in range(0, len(e._buf), 32):
            e._buf[i] = 0xE5
        self.stream.seek(e._pos)
        self.stream.write(e._buf)
        self.slots_map[e._pos] = len(e._buf)/32 # updates slots map
        self.map_compact()
        if start:
            self.fat.free(start)
        if DEBUG_FAT:
            logging.debug("Erased slot '%s' @%Xh (pointing at LCN %Xh)", name, e._pos, start)
            logging.debug("Mapped new free slot {%d: %d}", e._pos, len(e._buf)/32)
        return 1

    def rename(self, name, newname):
        "Renames a file or directory slot"
        if type(name) == DirentryType:
            e = name
        else:
            e = self.find(name)
            if not e:
                if DEBUG_FAT: logging.debug("Can't find file to rename: '%'s", name)
                return 0
        if self.find(newname):
            if DEBUG_FAT: logging.debug("Can't rename, file exists: '%s'", newname)
            return 0
        # Alloc new slot
        ne = self._alloc(newname)
        if not ne:
            if DEBUG_FAT: logging.debug("Can't alloc new file slot for '%s'", newname)
            return 0
        # Copy attributes from old to new slot
        ne.Entry._buf[-21:] = e._buf[-21:]
        # Write new entry
        self.stream.seek(ne.Entry._pos)
        self.stream.write(ne.Entry._buf)
        ne.IsValid = False
        if DEBUG_FAT: logging.debug("'%s' renamed to '%s'", name, newname)
        self._update_dirtable(ne.Entry)
        self._update_dirtable(e, True)
        # Mark the old one as erased
        for i in range(0, len(e._buf), 32):
            e._buf[i] = 0xE5
        self.stream.seek(e._pos)
        self.stream.write(e._buf)
        return 1

    @staticmethod
    def _sortby(a, b):
        "Helper function that sorts following the order in a list set by the caller in 'fix' variable."
        if a not in Dirtable._sortby.fix:
            return -1 # Unknown item comes first
        elif b not in Dirtable._sortby.fix:
            return 1
        else:
            return cmp(Dirtable._sortby.fix.index(a), Dirtable._sortby.fix.index(b))

    def clean(self, shrink=False):
        "Compacts used slots and blanks unused ones, optionally shrinking the table"
        if DEBUG_FAT: logging.debug("Cleaning directory table with keep sort function")
        return self.sort(lambda x:0, shrink) # keep order

    def stats(self):
        "Prints informations about slots in this directory table"
        in_use = 0
        count = 0
        for e in self.iterator():
            count+=1
            in_use+=len(e._buf)
        print "%d entries in %d slots on %d allocated" % (count, in_use/32, self.stream.size/32)
        
    def sort(self, by_func=None, shrink=False):
        """Sorts the slot entries alphabetically or applying by_func, compacting
        them and zeroing unused ones. Optionally shrinks table. Returns a tuple (used slots, blank slots)."""
        d = {}
        for e in self.iterator():
            d[e.Name()] = e
        names = d.keys()
        names.sort(by_func)
        self.stream.seek(0)
        for name in names:
            self.stream.write(d[name]._buf) # re-writes ordered slots
        last = self.stream.tell()
        unused = self.stream.size - last
        self.stream.write(bytearray(unused)) # blank unused area
        if DEBUG_FAT: logging.debug("%s: sorted %d slots, blanked %d", last/32, unused/32)
        if shrink:
            c_alloc = rdiv(self.stream.size, self.boot.cluster)
            c_used = rdiv(last, self.boot.cluster)
            if c_used < c_alloc:
                self.stream.seek(last)
                self.stream.trunc()
                if DEBUG_FAT: logging.debug("Shrank directory table freeing %d clusters", c_alloc-c_used)
                unused -= (c_alloc-c_used/32)
            else:
                if DEBUG_FAT: logging.debug("Can't shrink directory table, free space < 1 cluster!")
        # Rebuilds Dirtable caches
        self.slots_map = {}
        Dirtable.dirtable[self.start] = {'LFNs':{}, 'Names':{}}
        self.map_slots()
        return last/32, unused/32

    def listdir(self):
        "Returns a list of file and directory names in this directory, sorted by on disk position"
        return map(lambda o:o.Name(), [o for o in self.iterator()])

    def list(self, bare=False):
        "Simple directory listing, with size and last modification time"
        print "   Directory of", self.path, "\n"
        tot_files = 0
        tot_bytes = 0
        tot_dirs = 0
        for it in self.iterator():
            if it.IsLabel(): continue
            if bare:
                print it.Name()
            else:
                tot_bytes += it.dwFileSize
                if it.IsDir(): tot_dirs += 1
                else: tot_files += 1
                mtime = datetime(*(it.ParseDosDate(it.wMDate) + it.ParseDosTime(it.wMTime))).isoformat()[:-3].replace('T',' ')
                print "%8s  %s  %s" % ((str(it.dwFileSize),'<DIR>')[it.IsDir()], mtime, it.Name())
        if not bare:
            print "%18s Files    %s bytes" % (tot_files, tot_bytes)
            print "%18s Directories %12s bytes free" % (tot_dirs, self.getdiskspace()[1])

    def walk(self):
        """Walks across this directory and its childs. For each visited directory,
        returns a tuple (root, dirs, files) sorted in disk order. """
        dirs = []
        files = []
        for o in self.iterator():
            if o.IsLabel(): continue
            n = o.Name()
            if n in ('.', '..'): continue
            if o.IsDir():
                dirs += [n]
            else:
                files += [n]
        yield self.path, dirs, files
        for subdir in dirs:
            for a,b,c in self.opendir(subdir).walk():
                yield a, b, c



def rdiv(a, b):
    "Divide a by b eventually rounding up"
    if a % b:
        return a/b + 1
    else:
        return a/b



         #############################
        # HIGH LEVEL HELPER ROUTINES #
        ############################


def fat_copy_clusters(boot, fat, start):
    """Duplicate a cluster chain copying the cluster contents to another position.
    Returns the first cluster of the new chain."""
    count = fat.count(start)[0]
    src = Chain(boot, fat, start, boot.cluster*count)
    target = fat.alloc(count)[0] # possibly defragmented
    dst = Chain(boot, fat, target, boot.cluster*count)
    if DEBUG_FAT: logging.debug("Copying %s to %s", src, dst)
    s = 1
    while s:
        s = src.read(boot.cluster)
        dst.write(s)
    return target
