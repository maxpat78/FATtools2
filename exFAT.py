# -*- coding: mbcs -*-
# Utilities to manage an exFAT  file system
#

DEBUG_EXFAT=0

""" BUGS/TODO:
- implement trunc, frags, clean & co.
- implement set/clear Label (FAT too)
- implement tree extraction (FAT too)
- Bitmap findfree alternative?
- set ranges of bits/fat slots at once!
- read bitmap 128 bit at once and use math to find set/clear bits?
- write set bit runs multiple bytes at once?
- bitmap is FATted, so it's more slower: dict{pos:QWORD}?
- defer Bitmap file update (allocation is really showed already by slot or FAT)?
- fix setting last alloced cluster (is lower than reqd?)
- parameters {} to tune cluster allocator?
- generalize pack() in utils.py

Advantages of common_getattr & pack technique in respect of property():
- defer unpacking at effective access time
- unpack *once* only what needed
- pack *once* the full buffer"""
import copy, os, struct, time, cStringIO, atexit
from datetime import datetime
import logging
if DEBUG_EXFAT: import hexdump

import disk, utils
from FAT import boot_fat16, boot_fat32, FAT


class boot_exfat(object):
    "exFAT boot sector"
    layout = { # { offset: (nome, stringa di unpack) }
    0x00: ('chJumpInstruction', '3s'),
    0x03: ('chOemID', '8s'),
    0x0B: ('chDummy', '53s'),
    0x40: ('u64PartOffset', '<Q'),
    0x48: ('u64VolumeLength', '<Q'), # sectors
    0x50: ('dwFATOffset', '<I'), # sectors
    0x54: ('dwFATLength', '<I'), # sectors
    0x58: ('dwDataRegionOffset', '<I'), # sectors
    0x5C: ('dwDataRegionLength', '<I'),
    0x60: ('dwRootCluster', '<I'),
    0x64: ('dwVolumeSerial', '<I'),
    0x68: ('wFSRevision', '<H'), # 0x100 or 1.00
    # bit 0: active FAT & Bitmap (0=first, 1=second)
    # bit 1: volume is dirty? (0=clean)
    # bit 2: media failure (0=none, 1=some I/O failed)
    0x6A: ('wFlags', '<H'), # field not included in VBR checksum
    0x6C: ('uchBytesPerSector', 'B'), # 2 exponent
    0x6D: ('uchSectorsPerCluster', 'B'), # 2 exponent
    0x6E: ('uchFATCopies', 'B'), # 1 by default
    0x6F: ('uchDriveSelect', 'B'),
    0x70: ('uchPercentInUse', 'B'), # field not included in VBR checksum
    0x71: ('chReserved', '7s'),
    0x1FE: ('wBootSignature', '<H') } # Size = 0x200 (512 byte)

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
        if not self.uchBytesPerSector: return
        # Cluster size (bytes)
        self.cluster = (1 << self.uchBytesPerSector) * (1 << self.uchSectorsPerCluster)
        # FAT offset
        self.fatoffs = self.dwFATOffset * (1 << self.uchBytesPerSector) + self._pos
        # Clusters in the Data region
        self.fatsize = self.dwDataRegionLength
        # Data region offset (=cluster #2)
        self.dataoffs = self.dwDataRegionOffset * (1 << self.uchBytesPerSector) + self._pos

    __getattr__ = utils.common_getattr

    def pack(self):
        "Update internal buffer"
        for k, v in self._kv.items():
            self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(self, v[0]))
        self.__init2__()
        return self._buf

    def __str__ (self):
        return utils.class2str(self, "exFAT Boot sector @%x\n" % self._pos)

    def clusters(self):
        "Return the number of clusters in the data area"
        # Total sectors minus sectors preceding the data area
        return self.fatsize

    def cl2offset(self, cluster):
        "Return a real's cluster offset"
        return self.dataoffs + (cluster-2)*self.cluster

    def root(self):
        "Root offset"
        return self.cl2offset(self.dwRootCluster)

    @staticmethod
    def GetChecksum(s, UpCase=False):
        "Computate the checksum for the VBR sectors (the first 11) or the UpCase table"
        hash = 0
        for i in xrange(len(s)):
            if not UpCase and i in (106, 107, 112): continue
            hash = (((hash<<31) | (hash >> 1)) & 0xFFFFFFFF) + s[i]
            hash &= 0xFFFFFFFF
        return hash



def upcase_expand(s):
    "Expand a compressed Up-Case table"
    i = 0
    expanded_i = 0
    tab = []
    # print "Processing compressed table of %d bytes" % len(s)
    while i < len(s):
        word = struct.unpack('<H', s[i:i+2])[0]
        if word == 0xFFFF and i+2 < len(s):
            # print "Found compressed run at 0x%X (%04X)" % (i, expanded_i)
            word = struct.unpack('<H', s[i+2:i+4])[0]
            # print "Expanding range of %04X chars from %04X to %04X" % (word, expanded_i, expanded_i+word)
            for j in xrange(expanded_i, expanded_i+word):
                tab += [struct.pack('<H', j)]
            i += 4
            expanded_i += word
        else:
            # print "Decoded uncompressed char at 0x%X (%04X)" % (i, expanded_i)
            tab += [s[i:i+2]]
            i += 2
            expanded_i += 1
    return bytearray().join(tab)



class Chain(object):
    "Open a cluster chain like a plain file"
    def __init__ (self, boot, fat, cluster, size=0, nofat=0):
        self.stream = boot.stream
        self.boot = boot
        self.fat = fat
        self.start = cluster # start cluster or zero if empty
        self.nofat = nofat # does not use FAT (=contig)
        # Size in bytes of allocated cluster(s)
        if self.start and not nofat:
            self.size = fat.count(cluster)[0]*boot.cluster
        else:
            self.size = size
        self.filesize = size or self.size # file size, if available, or chain size
        self.pos = 0 # virtual stream linear pos
        # Virtual Cluster Number (cluster index in this chain)
        self.vcn = -1
        # Virtual Cluster Offset (current offset in VCN)
        self.vco = -1
        self.lastvlcn = (0, cluster) # last cluster VCN & LCN
        if DEBUG_EXFAT: logging.debug("Cluster chain of %d%sbytes (%d bytes) @%Xh", self.filesize, (' ', ' contiguous ')[nofat], self.size, cluster)

    def __str__ (self):
        return "Chain of %d (%d) bytes from #%Xh" % (self.filesize, self.size, self.start)

    def maxrun4len(self, length):
        n = rdiv(length, self.boot.cluster)
        count, next = self.fat.count_run(self.lastvlcn[1], n)
        maxchunk = count * self.boot.cluster
        if DEBUG_EXFAT: logging.debug("maxrun4len: run of %d bytes (%d clusters) from VCN #%d (first,next LCN=%Xh,%Xh)", maxchunk, n, self.lastvlcn[0], self.lastvlcn[1], next)
        # Update (Last VCN, Next LCN) for fragment
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
            self.start, self.nofat = self.boot.bitmap.alloc(clusters)
            self.size = clusters * self.boot.cluster
            if DEBUG_EXFAT: logging.debug("Chain%08X: allocated %d clusters from 0x%X seeking 0x%X", self.start, clusters, self.start, self.pos)
        self.vcn = self.pos / self.boot.cluster # n-th cluster chain
        self.vco = self.pos % self.boot.cluster # offset in it
        self.realseek()

    def realseek(self):
        if DEBUG_EXFAT: logging.debug("Chain%08X: realseek with VCN=%d VCO=%d", self.start, self.vcn,self.vco)
        if self.size and self.pos >= self.size:
            if DEBUG_EXFAT: logging.debug("Chain%08X: detected chain end at VCN %d while seeking", self.start, self.vcn)
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
                    raise utils.EndOfStream
            self.lastvlcn = (self.vcn, cluster)
            #~ if self.fat.islast(cluster):
            if (self.fat.last <= cluster <= self.fat.last+7):
                self.vcn = -1
        if DEBUG_EXFAT: logging.debug("Chain%08X: realseek seeking VCN=%d LCN=%Xh [%Xh:] @%Xh", self.start, self.vcn, cluster, self.vco, self.boot.cl2offset(cluster))
        self.stream.seek(self.boot.cl2offset(cluster)+self.vco)

    def read(self, size=-1):
        if DEBUG_EXFAT: logging.debug("Chain%08X: read(%d) called from offset 0x%X", self.start, size, self.pos)
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
            if DEBUG_EXFAT: logging.debug("Chain%08X: read %d contiguous bytes, VCN=0x%X[0x%X:]", self.start, len(buf), self.vcn, self.vco)
            return buf
        while 1:
            if not size or self.vcn == -1:
                break
            n = min(size, self.maxrun4len(size))
            buf += self.stream.read(n)
            size -= n
            self.pos += n
            self.seek(self.pos)
        if DEBUG_EXFAT: logging.debug("Chain%08X: read %d byte, VCN=0x%X[0x%X:]", self.start, len(buf), self.vcn, self.vco)
        return buf

    def write(self, s):
        self.seek(self.pos)
        if DEBUG_EXFAT: logging.debug("Chain%08X: write(buf[:%d]) called from offset 0x%X, VCN=0x%X[0x%X:]", self.start, len(s), self.pos, self.vcn, self.vco)
        new_allocated = 0
        if self.pos + len(s) > self.size:
            # Alloc more clusters from actual last one
            # reqb=requested bytes, reqc=requested clusters, lastc=last cluster in the chain
            reqb = self.pos + len(s) - self.size
            reqc = rdiv(reqb, self.boot.cluster)
            if DEBUG_EXFAT: logging.debug("pos=%X(%d), len=%d, size=%d(%Xh)", self.pos, self.pos, len(s), self.size, self.size)
            if DEBUG_EXFAT: logging.debug("needed %d bytes [%d cluster(s)] more to write", reqb, reqc)
            if self.start:
                if self.nofat:
                    lastc = self.start + self.size/self.boot.cluster - 1
                else:
                    lastc = self.fat.count(self.start)[1]
                # first=lastc since we continue the chain
                start, nofat = self.boot.bitmap.alloc(reqc, lastc, lastc, self.nofat)
                self.size += reqc*self.boot.cluster
                # if blocks are now fragmented, set the FAT for the
                # original clusters
                if self.nofat and not nofat:
                    if DEBUG_EXFAT: logging.debug("Chain%08X: chain got fragmented, setting FAT from 0x%X to 0x%X", self.start, self.start, lastc)
                    self.nofat = 0
                    for i in xrange(self.start, lastc):
                        self.fat[i] = i+1
                #~ self.lastvlcn = (self.size/self.bootcluster, ???) # update last cluster VCN & LCN
                # force lastvlcn update
                pos = self.pos
                self.seek(0)
                self.seek(pos)
            else:
                # if chain is empty, again, simply allocate the clusters...
                self.start, self.nofat = self.boot.bitmap.alloc(reqc)
                # ...and force a seek on the real medium
                self.size = reqc*self.boot.cluster
                self.seek(self.pos)
            new_allocated = 1
        i = 0
        btoe = self.boot.cluster - self.vco # bytes to cluster's end
        if len(s) > btoe:
            if DEBUG_EXFAT: logging.debug("Chain%08X: writing %d bytes to end of cluster", self.start, btoe)
            self.seek(self.pos)
            self.stream.write(s[:btoe])
            self.pos += btoe
            i += btoe
        while i < len(s):
            self.seek(self.pos)
            if self.nofat:
                # write all bytes, since space is contiguous
                n = len(s)-i
            else:
                # write minimum between s rest and maximum contig run
                n = min(len(s)-i, self.maxrun4len(len(s)-i))
            self.stream.write(s[i:i+n])
            self.pos += n
            i += n
            if DEBUG_EXFAT: logging.debug("Chain%08X: written s[%d:%d] for %d contiguous bytes (todo=%d)", self.start, i-n, i, n, len(s)-i)
        # file size is the top pos reached during write
        self.filesize = max(self.filesize, self.pos)
        self.seek(self.pos)
        if new_allocated:
            if self.pos < self.size:
                logging.debug("Chain%08X: blanking newly allocated cluster tip, %d bytes @0x%X", self.start, self.size-self.pos, self.pos)
                self.stream.write(bytearray(self.size - self.pos))

    def trunc(self):
        "Truncate the chain to actual offset, freeing subsequent clusters accordingly"
        logging.debug("called trunc() on %s @%d", self, self.pos)
        n = (self.size-self.pos)/self.boot.cluster # number of clusters to free
        end = self.size/self.boot.cluster - n - 1# new last cluster
        if end < 0:
            logging.debug("trunc() returned 0, end<0")
            return 0
        st = self.fat.count_to(self.start, end)
        logging.debug("Truncating %s to %d by %d clusters (#%Xh new last cluster)", self, self.pos, n, st)
        if n:
            # Free the chain from next to last
            self.fat.free(self.fat[st])
            # Set new last cluster
            self.fat[st] = self.fat.last
            # Update chain and virtual stream sizes
            self.size -= n*self.boot.cluster
        self.filesize = self.pos
        return 1

    def frags(self):
        logging.debug("Fragmentation of %s", self)
        runs = 0
        start = self.start
        while 1:
            length, next = self.fat.count_run(start)
            if next == start: break
            runs += 1
            logging.debug("Run of %d clusters from %Xh (next=%Xh)", length, start, next)
            start = next
        logging.debug("Detected %d fragments for %d clusters", runs, self.size/self.boot.cluster)
        logging.debug("Fragmentation is %f", float(runs-1) / float(self.size/self.boot.cluster))
        return runs



class Bitmap(Chain):
    def __init__ (self, boot, fat, cluster, size=0):
        self.stream = boot.stream
        self.boot = boot
        self.fat = fat
        self.start = cluster # start cluster or zero if empty
        # Size in bytes of allocated cluster(s)
        if self.start:
            self.size = fat.count(cluster)[0]*boot.cluster
        self.filesize = size or self.size # file size, if available, or chain size
        self.pos = 0 # virtual stream linear pos
        # Virtual Cluster Number (cluster index in this chain)
        self.vcn = -1
        # Virtual Cluster Offset (current offset in VCN)
        self.vco = -1
        self.lastvlcn = (0, cluster) # last cluster VCN & LCN
        self.last_free_alloc = 2
        self.runs = {} # {pos: QWORD}
        # Bitmap always uses FAT, even if contig, but is fixed size
        self.nofat = self.size == self.maxrun4len(self.size)
        if DEBUG_EXFAT: logging.debug("exFAT Bitmap of %d bytes (%d clusters) @%Xh", self.filesize, self.filesize*8, self.start)

    def __str__ (self):
        return "exFAT Bitmap of %d bytes (%d clusters) @%Xh" % (self.filesize, self.filesize*8, self.start)

    def isset(self, cluster):
        "Test if the bit corresponding to a given cluster is set"
        assert cluster > 1
        cluster-=2
        self.seek(cluster/8)
        B = self.read(1)[0]
        return (B & (1 << (cluster%8))) != 0

    def set(self, cluster, length=1, clear=False):
        "Set or clear a bit or bits run"
        assert cluster > 1
        cluster-=2 # since bit zero represents cluster #2
        pos = cluster/8
        rem = cluster%8
        if DEBUG_EXFAT: logging.debug("set(%Xh,%d%s) start @0x%X:%d", cluster+2, length, ('',' (clear)')[clear!=False], pos, rem)
        self.seek(pos)
        if rem:
            B = self.read(1)[0]
            if DEBUG_EXFAT: logging.debug("got byte 0x%X", B)
            todo = min(8-rem, length)
            if clear:
                B &= ~((0xFF>>(8-todo)) << rem)
            else:
                B |= ((0xFF>>(8-todo)) << rem)
            self.seek(-1, 1)
            self.write(chr(B))
            length -= todo
            if DEBUG_EXFAT: logging.debug("set byte 0x%X, remaining=%d", B, length)
        octets = length/8
        while octets:
            i = min(32768, octets)
            octets -= i
            if clear:
                self.write(i*'\x00')
            else:
                self.write(i*'\xFF')
        rem = length%8
        if rem:
            if DEBUG_EXFAT: logging.debug("last bits=%d", rem)
            B = self.read(1)[0]
            if DEBUG_EXFAT: logging.debug("got B=0x%X", B)
            if clear:
                B &= ~(0xFF>>(8-rem))
            else:
                B |= (0xFF>>(8-rem))
            self.seek(-1, 1)
            self.write(chr(B))
            if DEBUG_EXFAT: logging.debug("set B=0x%X", B)

    def findfree_new(self, start=2, count=0):
        """Return index and length of the first free clusters run beginning from
        'start' or (-1,0) in case of failure. If 'count' is given, limit the search
        to that amount."""
        if start < 2:
            start = 2
        n = 0
        i = -1
        bytepos = (start-2)/8
        self.seek(bytepos)
        B = self.read(1)[0]
        while True:
            if start > self.fat.real_last:
                return -1, -1 # is this right if we reach last valid cluster?
            bitpos = (start-2)
            if bytepos != bitpos/8:
                B = self.read(1)[0]
                bytepos = (start-2)/8
            is_set = B & (1 << (bitpos%8))
            if not is_set:
                while not is_set and start <= self.fat.real_last:
                    if i < 0: i = start
                    start += 1
                    n += 1
                    if n == count: break # stop search if we got the required amount
                    bitpos = (start-2)
                    if bytepos != bitpos/8:
                        B = self.read(1)[0]
                        bytepos = (start-2)/8
                    is_set = B & (1 << (bitpos%8))
                break
            start += 1
        #~ logging.debug("findfree: found %d free clusters from #%X", n, i)
        return i, n

    def findfree(self, start=2, count=0):
        """Return index and length of the first free clusters run beginning from
        'start' or (-1,0) in case of failure. If 'count' is given, limit the search
        to that amount."""
        if start < 2:
            start = 2
        n = 0
        i = -1
        while 1:
            if start > self.fat.real_last:
                return -1, -1 # is this right if we reach last valid cluster?
            if not self.isset(start):
                while not self.isset(start) and start <= self.fat.real_last:
                    if i < 0: i = start
                    start += 1
                    n += 1
                    if n == count: break # stop search if we got the required amount
                break
            start += 1
        #~ logging.debug("findfree: found %d free clusters from #%X", n, i)
        return i, n

    def findmaxrun(self, count=0):
        "Find a run of at least count clusters or the greatest run available. Returns a tuple (total_free_clusters, (run_start, clusters))"
        t = self.last_free_alloc,0
        maxrun=(0,0)
        n=0
        while 1:
            t = self.findfree(t[0]+1, count)
            if t[0] < 0: break
            if DEBUG_EXFAT: logging.debug("Found %d free clusters from #%d", t[1], t[0])
            maxrun = max(t, maxrun, key=lambda x:x[1])
            n += t[1]
            if count and maxrun[1] >= count: break # break if we found the required run
            t = (t[0]+t[1], t[1])
        if DEBUG_EXFAT: logging.debug("Found the biggest run of %d clusters from #%d on %d total clusters", maxrun[1], maxrun[0], n)
        return n, maxrun

# alloc(n) if first allocation, alloc(n,x,y) if continuation
# is_contiguous is True if the unique or extended block is or remains contiguous, False otherwise
# in the latter case, the FAT is marked for the newly allocated block, the caller must set the FAT
# for the previous one
    def alloc(self, count, start=2, first=0, nofat=False): # first signal if first run
        """Allocate a run and/or chain of free clusters and appropriately mark the FAT.
        Returns the first cluster or zero in case of failure"""
        if DEBUG_EXFAT: logging.debug("alloc: requested %d cluster(s) from 0x%X", count, start)

        last = start
        is_contiguous = False
        is_firstround = True

        while count:
            if DEBUG_EXFAT: logging.debug("alloc: searching %d cluster(s) from 0x%X", count, self.last_free_alloc)
            i, n = self.findfree(self.last_free_alloc, count)
            if i < 0 and self.last_free_alloc > 2:
                if DEBUG_EXFAT: logging.debug("alloc: restarting search from cluster 0x2")
                self.last_free_alloc = 2 # retry search
                i, n = self.findfree(self.last_free_alloc, count)
            if i < 0: break # no more free clusters
            self.set(i, n) # mark the run as allocated in Bitmap
            # If we found all contiguous clusters on 1st attempt...
            if is_firstround and n == count:
                # ... and it's first allocation or continuation of a contiguous run
                if not first or (nofat and i == last+1):
                    if DEBUG_EXFAT: logging.debug("alloc: found run of %d clusters from 0x%X", count, i)
                    count = 0
                    is_contiguous = True
                    if not first:
                        first = i
                    last = i + count - 1
                    break
            # In all other cases, we must update the FAT
            is_firstround = False
            if not first:
                first = i # save the first cluster in the chain
            else: # if we continue a chain...
                self.fat[last] = i
            while count and n:
                self.fat[i] = i+1 # set the FAT slot as usual and...
                i += 1
                n -= 1
                count -= 1
            last = i-1
            self.fat[last] = self.fat.last # temporarily mark as last
            self.last_free_alloc = last # try this in regular FAT too!

        self.last_free_alloc = last

        # If we can't allocate all required clusters...
        if count:
            #...free all the clusters we allocated
            # CAVE: might be failure WITHOUT FAT!
            if DEBUG_EXFAT: logging.debug("FATAL: couldn't allocate %d more clusters", count)
            self.free(first)
            return 0
        if DEBUG_EXFAT: logging.debug("clusters successfully allocated from 0x%X%s", first, ('',' in a contiguous run')[is_contiguous])
        return first, is_contiguous

    # TODO: detect FAT runs, and clear bits sequences accordingly!
    def free(self, start):
        "Free the Bitmap following a clusters chain"
        if DEBUG_EXFAT: logging.debug("freeing cluster chain from %Xh", start)
        while not (self.fat.last <= self.fat[start] <= self.fat.last+7): # islast
            prev = start
            start = self.fat[start]
            #~ self.fat[prev] = 0 # FAT itself can remain dirty?
            self.set(prev, clear=True)
            if DEBUG_EXFAT: logging.debug("freed cluster %x", prev)
        #~ self.fat[start] = 0
        self.set(start, clear=True)
        if DEBUG_EXFAT: logging.debug("freed last cluster %Xh", start)



class Handle(object):
    "Manage an open table slot"
    def __init__ (self):
        self.IsValid = False # determine whether update or not on disk
        self.File = None # file contents
        self.Entry = None # direntry slot
        self.Dir = None #dirtable owning the handle
        self.IsReadOnly = True # use this to prevent updating a Direntry on a read-only filesystem
        atexit.register(self.close)

    def __del__ (self):
        self.close()

    def update_time(self, i=0):
        cdatetime, ms = exFATDirentry.GetDosDateTimeEx()
        if i == 0:
            self.Entry.dwATime = cdatetime
            self.Entry.chmsATime = ms
        elif i == 1:
            self.Entry.dwMTime = cdatetime
            self.Entry.chmsMTime = ms

    def tell(self):
        return self.File.tell()

    def seek(self, offset, whence=0):
        self.File.seek(offset, whence)

    def read(self, size=-1):
        self.update_time()
        return self.File.read(size)

    def write(self, s):
        self.File.write(s)
        self.update_time(1)
        self.IsReadOnly = False

    def close(self):
        if not self.IsValid:
            return

        # Force setting the start cluster if allocated on write
        self.Entry.Start(self.File.start)

        # If got fragmented at run time
        if self.File.nofat:
            self.Entry.chSecondaryFlags |= 2
        else:
            if self.Entry.chSecondaryFlags & 2:
                self.Entry.chSecondaryFlags ^= 2

        if not self.Entry.IsDir():
            if self.Entry.IsDeleted() and self.Entry.Start():
                if DEBUG_EXFAT: logging.debug("Deleted file: deallocating cluster(s)")
                self.File.fat.free(self.Entry.Start())
                return

            self.Entry.u64ValidDataLength = self.File.filesize
            self.Entry.u64DataLength = self.File.filesize

            # Free cluster allocated if empty at last
            if not self.Entry.u64ValidDataLength and self.Entry.Start():
                if DEBUG_EXFAT: logging.debug("Empty file: deallocating cluster(s)")
                self.File.boot.bitmap.free(self.Entry.Start())
                self.Entry.dwStartCluster = 0
        else:
            self.Entry.u64ValidDataLength = self.File.size
            self.Entry.u64DataLength = self.File.size

        self.Dir.stream.seek(self.Entry._pos)
        if DEBUG_EXFAT: logging.debug('Closing Handle @%Xh(%Xh) to "%s", cluster=%Xh tell=%d chain=%d size=%d', \
        self.Entry._pos, self.Dir.stream.realtell(), os.path.join(self.Dir.path,self.Entry.Name()), self.Entry.Start(), self.File.pos, self.File.size, self.File.filesize)
        self.Dir.stream.write(self.Entry.pack())
        self.IsValid = False
        if DEBUG_EXFAT: logging.debug("Handle close wrote:\n%s", hexdump.hexdump(str(self.Entry._buf),'return'))



class Direntry(object):
    pass

DirentryType = type(Direntry())
HandleType = Handle()


class exFATDirentry(Direntry):
    "Represent an exFAT direntry of one or more slots"

    "Represent a 32 byte exFAT slot"
    # chEntryType bit 7: 0=unused entry, 1=active entry
    volume_label_layout = {
    0x00: ('chEntryType', 'B'), # 0x83, 0x03
    0x01: ('chCount', 'B'), # Label length (max 11 chars)
    0x02: ('sVolumeLabel', '22s'),
    0x18: ('sReserved', '8s') }

    bitmap_layout = {
    0x00: ('chEntryType', 'B'), # 0x81, 0x01
    0x01: ('chFlags', 'B'), # bit 0: 0=1st bitmap, 1=2nd bitmap (T-exFAT only)
    0x02: ('sReserved', '18s'),
    0x14: ('dwStartCluster', '<I'), # typically cluster #2
    0x18: ('u64DataLength', '<Q')	} # bitmap length in bytes

    upcase_layout = {
    0x00: ('chEntryType', 'B'), # 0x82, 0x02
    0x01: ('sReserved1', '3s'),
    0x04: ('dwChecksum', '<I'),
    0x08: ('sReserved2', '12s'),
    0x14: ('dwStartCluster', '<I'),
    0x18: ('u64DataLength', '<Q')	}

    volume_guid_layout = {
    0x00: ('chEntryType', 'B'), # 0xA0, 0x20
    0x01: ('chSecondaryCount', 'B'),
    0x02: ('wChecksum', '<H'),
    0x04: ('wFlags', '<H'),
    0x06: ('sVolumeGUID', '16s'),
    0x16: ('sReserved', '10s') }

    texfat_padding_layout = {
    0x00: ('chEntryType', 'B'), # 0xA1, 0x21
    0x01: ('sReserved', '31s') }

    # A file entry slot group is made of a File Entry slot, a Stream Extension slot and
    # one or more Filename Extension slots
    file_entry_layout = {
    0x00: ('chEntryType', 'B'), # 0x85, 0x05
    0x01: ('chSecondaryCount', 'B'), # other slots in the group (2 minimum)
    0x02: ('wChecksum', '<H'), # slots group checksum
    0x04: ('wFileAttributes', '<H'), # usual MS-DOS file attributes (0x10 = DIR, etc.)
    0x06: ('sReserved2', '2s'),
    0x08: ('dwCTime', '<I'), # date/time in canonical MS-DOS format
    0x0C: ('dwMTime', '<I'),
    0x10: ('dwATime', '<I'),
    0x14: ('chmsCTime', 'B'), # 10-milliseconds unit (0...199)
    0x15: ('chmsMTime', 'B'),
    0x16: ('chtzCTime', 'B'), # Time Zone in 15' increments (0x80=UTC, ox84=CET, 0xD0=DST)
    0x17: ('chtzMTime', 'B'),
    0x18: ('chtzATime', 'B'),
    0x19: ('sReserved2', '7s') }

    stream_extension_layout = {
    0x00: ('chEntryType', 'B'), # 0xC0, 0x40
    # bit 0: 1=can be allocated
    # bit 1: 1=contiguous contents, FAT is not used
    0x01: ('chSecondaryFlags', 'B'),
    0x02: ('sReserved1', 's'),
    0x03: ('chNameLength', 'B'),
    0x04: ('wNameHash', '<H'), # hash of the UTF-16, uppercased filename
    0x06: ('sReserved2', '2s'),
    0x08: ('u64ValidDataLength', '<Q'), # should be real file size
    0x10: ('sReserved3', '4s'),
    0x14: ('dwStartCluster', '<I'),
    0x18: ('u64DataLength', '<Q') } # should be allocated size: in fact, it seems they MUST be equal

    file_name_extension_layout = {
    0x00: ('chEntryType', 'B'), # 0xC1, 0x41
    0x01: ('chSecondaryFlags', 'B'),
    0x02: ('sFileName', '30s') }

    slot_types = {
    0x00: ({0x00: ('sRAW','32s')}, "Unknown"),
    0x01: (bitmap_layout, "Allocation Bitmap"),
    0x02: (upcase_layout, "Upcase Table"),
    0x03: (volume_label_layout, "Volume Label"),
    0x05: (file_entry_layout, "File Entry"),
    0x20: (volume_guid_layout, "Volume GUID"),
    0x21: (texfat_padding_layout, "T-exFAT padding"),
    0x40: (stream_extension_layout, "Stream Extension"),
    0x41: (file_name_extension_layout, "Filename Extension") }

    def __init__ (self, s, pos=-1):
        self._i = 0
        self._buf = s
        self._pos = pos
        self._kv = {}
        self.type = self._buf[0] & 0x7F
        if self.type == 0 or self.type not in self.slot_types:
            logging.warning("Unknown slot type: %Xh", self.type)
        self._kv = self.slot_types[self.type][0].copy() # select right slot ype
        self._name = self.slot_types[self.type][1]
        self._vk = {} # { name: offset}
        for k, v in self._kv.items():
            self._vk[v[0]] = k
        if self.type == 5:
            for k in (1,3,4,8,0x14,0x18):
                self._kv[k+32] = self.stream_extension_layout[k]
                self._vk[self.stream_extension_layout[k][0]] = k+32
        #~ logging.debug("Decoded %s", self)

    __getattr__ = utils.common_getattr

    def __str__ (self):
        return utils.class2str(self, "%s @%x\n" % (self._name, self._pos))

    def pack(self):
        "Update internal buffer"
        for k, v in self._kv.items():
            self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(self, v[0]))
        if self.type == 5:
            self.wChecksum = self.GetSetChecksum(self._buf) # update the slots set checksum
            self._buf[2:4] = struct.pack('<H', self.wChecksum)
        if DEBUG_EXFAT: logging.debug("Packed %s", self)
        return self._buf

    @staticmethod
    def DatetimeParse(dwDatetime):
        "Decodes a datetime DWORD into a tuple"
        wDate = (dwDatetime & 0xFFFF0000) >> 16
        wTime = (dwDatetime & 0x0000FFFF)
        return (wDate>>9)+1980, (wDate>>5)&0xF, wDate&0x1F, wTime>>11, (wTime>>5)&0x3F, wTime&0x1F, 0, None

    @staticmethod
    def MakeDosDateTimeEx(t):
        "Encode a tuple into a DOS datetime DWORD"
        cdate = ((t[0]-1980) << 9) | (t[1] << 5) | (t[2]) 
        ctime = (t[3] << 11) | (t[4] << 5) | (t[5]/2)
        tms = 0
        if t[5] % 2: tms += 100 # odd DOS seconds
        return (cdate<<16 | ctime), tms

    @staticmethod
    def GetDosDateTimeEx():
        "Return a tuple with a DWORD representing DOS encoding of current datetime and 10 milliseconds exFAT tuning"
        tm = datetime.now()
        cdate = ((tm.year-1980) << 9) | (tm.month << 5) | (tm.day)
        ctime = (tm.hour << 11) | (tm.minute << 5) | (tm.second/2)
        tms = tm.microsecond/10000
        if tm.second % 2: tms += 100 # odd DOS seconds
        return (cdate<<16 | ctime), tms

    def IsContig(self, value=0):
        if value:
            self.chSecondaryFlags |= 2
        else:
            return bool(self.chSecondaryFlags & 2)

    def IsDeleted(self):
        return self._buf[0] & 0x80 != 0x80

    def IsDir(self, value=-1):
        "Get or set the slot's Dir DOS permission"
        if value != -1:
            self.wFileAttributes = value
        return (self.wFileAttributes & 0x10) == 0x10

    def IsLabel(self, mark=0):
        "Get or set the slot's Label DOS permission"
        return self.type == 0x03

    @staticmethod
    def IsValidDosName(name):
        special = ''':?/|\<>'''
        for c in special:
            if c in name:
                return False
        return True

    def Start(self, cluster=None):
        "Get or set cluster WORDs in slot"
        if cluster != None:
            self.dwStartCluster = cluster
        return self.dwStartCluster

    def Name(self):
        "Decodes the file name"
        ln = ''
        if self.type == 5:
            i = 64
            while i < len(self._buf):
                ln += self._buf[i+2:i+32].decode('utf-16le')
                i += 32
            return ln[:self.chNameLength]
        return ln

    @staticmethod
    def GetNameHash(name):
        "Computate the Stream Extension file name hash (UTF-16 LE encoded)"
        hash = 0
        name = name.upper()
        for c in name:
            hash = (((hash<<15) | (hash >> 1)) & 0xFFFF) + ord(c)
            hash &= 0xFFFF
        return hash

    @staticmethod
    def GetSetChecksum(s):
        "Computate the checksum for a set of slots (primary and secondary entries)"
        hash = 0
        for i in xrange(len(s)):
            if i == 2 or i == 3: continue
            hash = (((hash<<15) | (hash >> 1)) & 0xFFFF) + s[i]
            hash &= 0xFFFF
        return hash

    def GenRawSlotFromName(self, name):
        "Generate the exFAT slots set corresponding to a given file name"
        # File Entry part
        # a Stream Extension and a File Name Extension slot are always present
        self.chSecondaryCount = 1 + rdiv(len(name), 15)
        self.wFileAttributes = 0x20
        ctime, cms = self.GetDosDateTimeEx()
        self.dwCTime = self.dwMTime = self.dwATime = ctime
        self.chmsCTime = self.chmsMTime = self.chmsATime = cms
        # Stream Extension part
        self.chSecondaryFlags = 1 # base value, to show the entry could be allocated
        self.chNameLength = len(name)
        name = name.encode('utf-16le')
        self.wNameHash = self.GetNameHash(name)

        self.pack()

        # File Name Extension(s) part
        i = len(name)
        k = 0
        while i:
            b = bytearray(32)
            b[0] = 0xC1
            j = min(30, i)
            b[2:2+j] = name[k:k+j]
            i-=j
            k+=j
            self._buf += b

        #~ logging.debug("GenRawSlotFromName returned:\n%s", hexdump.hexdump(str(self._buf),'return'))

        return self._buf



class Dirtable(object):
    "Manage an exFAT directory table"
    dirtable = {} # {cluster: {'Names':{}, 'Handle':Handle}}

    def __init__(self, boot, fat, startcluster=0, size=0, nofat=0, path='.'):
        if type(boot) == type(HandleType):
            self.handle = boot
            self.boot = self.handle.File.boot
            self.fat = self.handle.File.fat
            self.start = self.handle.File.start
            self.stream = self.handle.File
        else:
            self.boot = boot
            self.fat = fat
            self.start = startcluster
            self.stream = Chain(boot, fat, startcluster, size, nofat)

        self.path = path
        self.lastfreeslot = 0 # last free slot found (to reduce search time)
        if self.start not in Dirtable.dirtable:
            # Names maps lowercased names and Direntry slots
            # Handle contains the unique Handle to the directory table
            Dirtable.dirtable[self.start] = {'Names':{}, 'Handle':None}

    def open(self, name):
        "Open the chain corresponding to an existing file name"
        res = Handle()
        if type(name) == type(''):
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
            res.File = Chain(self.boot, self.fat, e.Start(), e.u64DataLength, nofat=e.IsContig())
            res.Entry = e
            res.Dir = self
        return res

    def opendir(self, name):
        """Open an existing relative directory path beginning in this table and
        return a new Dirtable object or None if not found"""
        name = name.replace('/','\\')
        path = name.split('\\')
        found = self
        for com in path:
            e = found.find(com)
            if e and e.IsDir():
                found = Dirtable(self.boot, self.fat, e.Start(), e.u64ValidDataLength, e.IsContig(), path=os.path.join(found.path, com))
                continue
            found = None
            break
        if found:
            if DEBUG_EXFAT: logging.debug("opened directory table '%s' @0x%X (cluster 0x%X)", found.path, self.boot.cl2offset(found.start), found.start)
            if Dirtable.dirtable[found.start]['Handle']:
                # Opened many, closed once!
                found.handle = Dirtable.dirtable[found.start]['Handle']
                if DEBUG_EXFAT: logging.debug("retrieved previous directory Handle %s", found.handle)
                # We must update the Chain stream associated with the unique Handle,
                # or size variations will be discarded!
                found.stream = found.handle.File
            else:
                res = Handle()
                res.IsValid = True
                res.File = found.stream
                res.Entry = e
                res.Dir = self
                found.handle = res
                Dirtable.dirtable[found.start]['Handle'] = res
        return found

    def _alloc(self, name, clusters=0):
        "Alloc a new Direntry slot (both file/directory)"
        res = Handle()
        res.IsValid = True
        res.File = Chain(self.boot, self.fat, 0)
        if clusters:
            # Force clusters allocation
            res.File.seek(clusters*self.boot.cluster)
            res.File.seek(0)
        b = bytearray(64); b[0] = 0x85; b[32] = 0xC0
        dentry = exFATDirentry(b, -1)
        dentry.GenRawSlotFromName(name)
        dentry._pos = self.findfree(len(dentry._buf))
        dentry.Start(res.File.start)
        dentry.IsContig(res.File.nofat)
        res.Entry = dentry
        return res

    def create(self, name, prealloc=0):
        "Create a new file chain and the associated slot. Erase pre-existing filename."
        e = self.open(name)
        if e.IsValid:
            e.IsValid = False
            self.erase(name)
        handle = self._alloc(name, prealloc)
        self.stream.seek(handle.Entry._pos)
        self.stream.write(handle.Entry.pack())
        handle.Dir = self
        self._update_dirtable(handle.Entry)
        logging.debug("Created new file '%s' @%Xh", name, handle.File.start)
        return handle

    def mkdir(self, name):
        "Create a new directory slot, allocating the new directory table"
        r = self.opendir(name)
        if r:
            if DEBUG_EXFAT: logging.debug("mkdir('%s') failed, entry already exists!", name)
            return r
        # Check if it is a supported name
        if not exFATDirentry.IsValidDosName(name):
            if DEBUG_EXFAT: logging.debug("mkdir('%s') failed, name contains invalid chars!", name)
            return None
        handle = self._alloc(name, 1)
        self.stream.seek(handle.Entry._pos)
        if DEBUG_EXFAT: logging.debug("Making new directory '%s' @%Xh", name, handle.File.start)
        handle.Entry.wFileAttributes = 0x10
        handle.Entry.chSecondaryFlags |= 2 # since initially it has 1 cluster only
        handle.Entry.u64ValidDataLength = handle.Entry.u64DataLength = self.boot.cluster
        self.stream.write(handle.Entry.pack())
        handle.Dir = self
        handle.File.write(bytearray(self.boot.cluster)) # blank table
        self._update_dirtable(handle.Entry)
        # Record the unique Handle to the directory
        Dirtable.dirtable[handle.File.start] = {'Names':{}, 'Handle':handle}
        return Dirtable(handle, None, path=os.path.join(self.path, name))

    def rmtree(self, name=None):
        "Remove a full directory tree"
        if name:
            logging.debug("rmtree:opening %s", name)
            target = self.opendir(name)
        else:
            target = self
        if not target:
            logging.debug("rmtree:target '%s' not found!", name)
            return 0
        for it in target.iterator():
            n = it.Name()
            if it.IsDir():
                target.opendir(n).rmtree()
            logging.debug("rmtree:erasing '%s'", n)
            target.erase(n)
        #~ del target
        if name:
            logging.debug("rmtree:finally erasing '%s'", name)
            self.erase(name)
        return 1

    def close(self, handle):
        "Update a modified entry in the table"
        handle.close()

    def findfree(self, length=0):
        "Return the offset of the first free slot in a directory table"
        s = 1
        told = self.stream.tell()
        self.stream.seek(self.lastfreeslot)
        count = 0
        found = 0
        while s:
            found = self.stream.tell()
            s = self.stream.read(32)
            # if we're at table end...
            if not s or s[0] == 0:
                if DEBUG_EXFAT: logging.debug("Found next free directory slot @%Xh", found)
                break
            # if we search for a specified space, we try also to recycle unused slots
            if length:
                if s[0] & 0x80 != 0x80:
                    count+=32
                    if count == length:
                        found = found - count +32
                        if DEBUG_EXFAT: logging.debug("Found %d unused directory slot(s) @%Xh", count/32, found)
                        break
                    continue
                else:
                    count = 0
        self.stream.seek(told)
        self.lastfreeslot = found
        return found

    def iterator(self):
        told = self.stream.tell()
        buf = bytearray()
        s = 1
        pos = 0
        count = 0
        while s:
            self.stream.seek(pos)
            s = self.stream.read(32)
            pos += 32
            if not s or s[0] == 0: break
            if s[0] & 0x80 != 0x80: continue # unused slot
            if s[0] & 0x7F in (0x5, 0x20): # composite slot
                count = s[1] # slot to collect
                buf += s
                continue
            if count:
                count -= 1
                buf += s
                if count: continue
            else:
                buf += s
            yield exFATDirentry(buf, self.stream.tell()-len(buf))
            buf = bytearray()
            count = 0
        self.stream.seek(told)

    def _update_dirtable(self, it, erase=False):
        if erase:
            del Dirtable.dirtable[self.start]['Names'][it.Name().lower()]
            return
        if DEBUG_EXFAT: logging.debug("updating Dirtable name cache with '%s'", it.Name().lower())
        Dirtable.dirtable[self.start]['Names'][it.Name().lower()] = it

    def find(self, name):
        "Find an entry by name. Returns it or None if not found"
        # Create names cache
        if DEBUG_EXFAT: logging.debug("entering find('%s')", name)
        if not Dirtable.dirtable[self.start]['Names']:
            if DEBUG_EXFAT: logging.debug("building Dirtable dictionary")
            for it in self.iterator():
                if it.type == 5:
                    self._update_dirtable(it)
        name = name.lower()
        return Dirtable.dirtable[self.start]['Names'].get(name)

    def dump(self, n, range=3):
        "Return the n-th slot in the table for debugging purposes"
        self.stream.seek(n*32)
        return self.stream.read(range*32)

    def erase(self, name):
        "Mark a file's slot as erased and free the corresponding clusters"
        if type(name) == DirentryType:
            e = name
        else:
            e = self.find(name)
            if not e:
                return 0
        if e.IsDir():
            it = self.opendir(e.Name()).iterator()
            if next in it:
                if DEBUG_EXFAT: logging.debug("Can't erase non empty directory slot @%d (pointing at #%d)", e._pos, e.Start())
                return 0
        start = e.Start()
        if DEBUG_EXFAT: logging.debug("Erasing slot @%d (pointing at #%d)", e._pos, start)
        if start:
            if e.IsContig():
                # Free Bitmap only
                if DEBUG_EXFAT: logging.debug("Erasing contig run of %d clusters from %Xh", rdiv(e.u64ValidDataLength, self.boot.cluster), start)
                self.boot.bitmap.set(start, rdiv(e.u64ValidDataLength, self.boot.cluster), True)
            else:
                # Free FAT & Bitmap
                if DEBUG_EXFAT: logging.debug("Fragmented contents, freeing FAT chain from %Xh", start)
                self.boot.bitmap.free(start)
        e.Start(0)
        e.chEntryType = 5 # set this, or pack resets to 0x85
        e.u64ValidDataLength = 0
        e.u64DataLength = 0
        self._update_dirtable(e, True)
        for i in range(0, len(e._buf), 32):
            e._buf[i] ^= (1<<7)
        self.stream.seek(e._pos)
        self.stream.write(e._buf)
        #~ self.lastfreeslot = min(e._pos, self.lastfreeslot) # track the lowest offset of a free slot
        if DEBUG_EXFAT: logging.debug("Erased slot '%s' @%Xh (pointing at #%d)", name, e._pos, start)
        return 1

    def rename(self, name, newname):
        "Rename a file or directory slot"
        if type(name) == DirentryType:
            e = name
        else:
            e = self.find(name)
            if not e:
                logging.debug("Can't find file to rename: '%'s", name)
                return 0
        if self.find(newname):
            logging.debug("Can't rename, file exists: '%s'", newname)
            return 0
        # Alloc new slot
        ne = self._alloc(newname)
        if not ne:
            logging.debug("Can't alloc new file slot for '%s'", newname)
            return 0
        # Copy attributes from old to new slot
        for k, v in e._kv.items():
            if k in (1, 0x23, 0x24): continue # skip chSecondaryCount, chNameLength and wNameHash
            setattr(ne.Entry, v[0], getattr(e, v[0]))
        ne.Entry.pack()
        ne.IsValid = False
        e.chEntryType = 5 # set this, or pack resets to 0x85 (Open Handle)
        # Write new entry
        self.stream.seek(ne.Entry._pos)
        self.stream.write(ne.Entry._buf)
        logging.debug("'%s' renamed to '%s'", name, newname)
        self._update_dirtable(ne.Entry)
        self._update_dirtable(e, True)
        # Mark the old one as erased
        for i in range(0, len(e._buf), 32):
            e._buf[i] ^= (1<<7)
        self.stream.seek(e._pos)
        self.stream.write(e._buf)
        return 1

    def shrink(self, report_only=False):
        "Shrink table removing clusters with free space or report unused clusters"
        pos = self.findfree()
        size = self.stream.size
        if report_only:
            return (size-pos)/self.boot.cluster
        if not (size-pos)/self.boot.cluster: # if free space is less than a cluster
            logging.debug("Can't shrink directory table, free space < 1 cluster!")
            return 0
        self.stream.seek(pos)
        self.stream.trunc()
        logging.debug("Shrank directory table from %d to %d bytes freeing %d clusters", size, pos, rdiv(size-pos, self.boot.cluster))
        return 1

    def clean(self, report_only=False):
        "Compact used slots and remove unused ones, or report amount of wasted space"
        pos = self.findfree()
        self.stream.seek(0)
        buf = bytearray()
        s = 1
        while s:
            s = self.stream.read(32)
            if not s or s[0] == 0: break
            if s[0] == 0xE5: continue
            buf += s
        if report_only:
            return pos - len(buf)
        self.stream.seek(0)
        self.stream.write(buf) # write valid slots
        self.lastfreeslot = self.stream.tell()
        unused = pos - self.stream.tell()
        self.stream.write(bytearray(unused)) # blank unused area
        logging.debug("Cleaned directory table freeing %d slots", unused/32)

    @staticmethod
    def _sortby(a, b):
        "Helper function that sorts following the order in a list set by the caller in 'fix' variable."
        if a not in Dirtable._sortby.fix:
            return -1 # Unknown item comes first
        elif b not in Dirtable._sortby.fix:
            return 1
        else:
            return cmp(Dirtable._sortby.fix.index(a), Dirtable._sortby.fix.index(b))

    def sort(self, by_func=None):
        "Sort the slot entries alphabetically or applying by_func, compacting them and removing unused ones"
        d = {}
        for e in self.iterator():
            d[e.Name()] = e

        names = d.keys()
        names.sort(by_func)

        pos = self.findfree()
        self.stream.seek(0)
        for name in names:
            self.stream.write(d[name]._buf)
        self.lastfreeslot = self.stream.tell()
        unused = pos - self.stream.tell()
        self.stream.write(bytearray(unused)) # blank unused area
        logging.debug("Sorted directory table freeing %d slots", unused/32)

    def list(self, bare=False):
        "Simple directory listing, with size and last modification time"
        if bare != 2:
            print "   Directory of", self.path, "\n"
        tot_files = 0
        tot_bytes = 0
        tot_dirs = 0
        files = []
        for it in self.iterator():
            if it.type != 5: continue
            if bare == 2:
                files += [it.Name()]
                continue
            if bare:
                print it.Name()
            else:
                tot_bytes += it.u64DataLength
                if it.IsDir(): tot_dirs += 1
                else: tot_files += 1
                mtime = datetime(*(it.DatetimeParse(it.dwMTime))).isoformat()[:-3].replace('T',' ')
                print "%8s  %s  %s" % ((str(it.u64DataLength),'<DIR>')[it.IsDir()], mtime, it.Name())
        if bare == 2:
            return files
        if not bare:
            print "%18s Files    %s bytes" % (tot_files, tot_bytes)
            print "%18s Directories" % tot_dirs


def opendisk(path, mode='rb'):
    "Open a FAT filesystem returning the root directory Dirtable"
    if os.name =='nt' and len(path)==2 and path[1] == ':':
        path = '\\\\.\\'+path
    d = disk.disk(path, mode)
    bs = d.read(512)
    d.seek(0)
    fstyp = utils.FSguess(boot_fat16(bs)) # warning: if we call this a second time on the same Win32 disk, handle is unique and seek set already!
    if fstyp in ('FAT12', 'FAT16'):
        boot = boot_fat16(bs, stream=d)
    elif fstyp == 'FAT32':
        boot = boot_fat32(bs, stream=d)
    elif fstyp == 'EXFAT':
        boot = boot_exfat(bs, stream=d)
    elif fstyp == 'NTFS':
        print fstyp, "file system not supported. Aborted."
        sys.exit(1)
    else:
        print "File system not recognized. Aborted."
        sys.exit(1)

    fat = FAT(d, boot.fatoffs, boot.clusters(), bitsize={'FAT12':12,'FAT16':16,'FAT32':32,'EXFAT':32}[fstyp], exfat=(fstyp=='EXFAT'))
    root = Dirtable(boot, fat, boot.dwRootCluster)
    for e in root.iterator():
        if e.type == 1: # Find & open Bitmap
            boot.bitmap = Bitmap(boot, fat, e.dwStartCluster, e.u64DataLength)
            break

    return root



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
    #~ if fat.exfat:
        #~ src.bitmap = ...
    target = fat.alloc(count) # possibly defragmented
    dst = Chain(boot, fat, target, boot.cluster*count)
    logging.debug("Copying %s to %s", src, dst)
    s = 1
    while s:
        s = src.read(boot.cluster)
        dst.write(s)
    return target



def fat_copy_tree_inject(base, dest, callback=None, attributes=None, chunk_size=1<<20):
    """Copy recursively files and directories under real 'base' path into
    virtual 'dest' directory table, 'chunk_size' bytes at a time, calling callback function if provided
    and preserving date and times if desired."""

    for root, folders, files in os.walk(base):
        relative_dir = root[len(base)+1:]
        # Split subdirs in target path
        subdirs = []
        while 1:
            pro, epi = os.path.split(relative_dir)
            if pro == relative_dir: break
            relative_dir = pro
            subdirs += [epi]
        subdirs.reverse()

        # Recursively open path to dest, creating directories if necessary
        target_dir = dest
        for subdir in subdirs:
            target_dir = target_dir.mkdir(subdir)

        # Finally, copy files
        for file in files:
            src = os.path.join(root, file)
            fp = open(src, 'rb')
            st = os.stat(src)
            # Create target, preallocating all clusters
            dst = target_dir.create(file, rdiv(st.st_size, dest.boot.cluster))
            if callback: callback(src)
            while 1:
                s = fp.read(chunk_size)
                if not s: break
                dst.write(s)

            if attributes: # bit mask: 1=preserve creation time, 2=last modification, 3=last access
                if attributes & 1:
                    tm = time.localtime(st.st_ctime)
                    dw, ms = exFATDirentry.MakeDosDateTimeEx((tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec))
                    dst.Entry.dwCTime = dw
                    dst.chmsCTime = ms
                if attributes & 2:
                    tm = time.localtime(st.st_mtime)
                    dw, ms = exFATDirentry.MakeDosDateTimeEx((tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec))
                    dst.Entry.dwMTime = dw
                    dst.chmsCTime = ms

                if attributes & 4:
                    tm = time.localtime(st.st_atime)
                    dw, ms = exFATDirentry.MakeDosDateTimeEx((tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec))
                    dst.Entry.dwATime = dw
                    dst.chmsCTime = ms
            dst.close()
