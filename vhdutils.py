# -*- coding: cp1252 -*-
"Utilities to handle VHD disk images"

""" VHD IMAGE FILE FORMAT
A FIXED VHD is a simple RAW image with all disk sectors and a VHD footer
appended.

A DYNAMIC VHD initially contains only the VHD footer in the last sector and
a copy of it in the first, a dynamic disk header in second and third sector
followed by the BAT (Blocks Allocation Table).
Disk is virtually subdivided into blocks of equal size (2 MiB default) with a
corresponding 32-bit BAT index showing the 512-byte sector where the block
resides in VHD file.
Initially, all BAT indexes are present and set to 0xFFFFFFFF; the blocks are
allocated on write and put at image's end, so they appear in arbitrary order.
More BAT space can be allocated at creation time for future size expansion.
Each block starts with one or more sectors containing a bitmap, indicating
which sectors are in use. A zeroed bit means sector is not in use, and zeroed.
The default block requires a 1-sector bitmap since it is 4096 sectors long.

A DIFFERENCING VHD is a dynamic image containing only new or modified blocks
of a parent VHD image (fixed, dynamic or differencing itself). The block
bitmap must be checked to determine which sectors are in use.

Since offsets are represented in sectors, the BAT can address sectors in a range
from zero to 2^32-1 or about 2 TiB.
The disk image itself is shorter due to VHD internal structures (assuming 2^20
blocks of default size, 4 MiB are occupied by the BAT and 512 MiB by bitmap
sectors.

PLEASE NOTE THAT ALL NUMBERS ARE IN BIG ENDIAN FORMAT! """
import utils, struct, uuid, zlib, ctypes, time, os, math

DEBUG = 0
from debug import log



class myfile(file):
	"Wrapper for file object whose read member returns a bytearray"
	def __init__ (self, *args):
		return file.__init__ (self, *args)

	def read(self, size=-1):
		return bytearray(file.read(self, size)) # a.k.a. mutable buffer



class Footer(object):
    "VHD Footer"
    layout = { # { offset: (name, unpack string) }
    0x00: ('sCookie', '8s'), # conectix
    0x08: ('dwFeatures', '>I'), # 0=None, 1=Temporary, 2=Reserved (default)
    0x0C: ('dwFileFormatVersion', '>I'), #0x10000
    0x10: ('u64DataOffset', '>Q'), # absolute offset of next structure, or 0xFFFFFFFFFFFFFFFF for fixed disks
    0x18: ('dwTimestamp', '>I'), # creation time, in seconds since 1/1/2000 12:00 AM UTC
    0x1C: ('dwCreatorApp', '4s'), # creator application, here Py
    0x20: ('dwCreatorVer', '>I'), # its version, here 0x20007 (2.7)
    0x24: ('dwCreatorHost', '4s'), # Wi2k or Mac
    0x28: ('u64OriginalSize', '>Q'), # Initial size of the emulated disk
    0x30: ('u64CurrentSize', '>Q'), # Current size of the emulated disk
    0x38: ('dwDiskGeometry', '4s'), # pseudo CHS
    0x3C: ('dwDiskType', '>I'), # 0=None,2=Fixed,3=Dynamic,4=Differencing
    0x40: ('dwChecksum', '>I'), # footer checksum
    0x44: ('sUniqueId', '16s'), # image UUID
    0x54: ('bSavedState', 'B'), # 1=is in saved state
    # REST IS RESERVED AND MUST BE ZERO
    } # Size = 0x200 (512 byte)

    def __init__ (self, s=None, offset=0, stream=None):
        self._i = 0
        self._pos = offset # base offset
        self._buf = s or bytearray(512)
        self.stream = stream
        self._kv = self.layout.copy()
        self._vk = {} # { name: offset}
        for k, v in self._kv.items():
            self._vk[v[0]] = k
    
    __getattr__ = utils.common_getattr

    def pack(self):
        "Updates internal buffer"
        self.dwChecksum = 0
        for k, v in self._kv.items():
            self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(self, v[0]))
        self._buf[64:68] = mk_crc(self._buf) # updates checksum
        return self._buf

    def __str__ (self):
        return utils.class2str(self, "VHD Footer @%X\n" % self._pos)
    
    def crc(self):
        crc = self._buf[64:68]
        self._buf[64:68] = '\0\0\0\0'
        c_crc = mk_crc(self._buf)
        self._buf[64:68] = crc
        return c_crc

    def isvalid(self):
        if self.sCookie != 'conectix' or self.dwCreatorHost not in ('Wi2k','Mac'):
            return 0
        if self.dwChecksum != struct.unpack(">I", self.crc())[0]:
            if DEBUG&4: log("Footer checksum 0x%X calculated != 0x%X stored", self.dwChecksum, struct.unpack(">I", self.crc())[0])
        return 1



class DynamicHeader(object):
    "Dynamic Disk Header"
    layout = { # { offset: (name, unpack string) }
    0x00: ('sCookie', '8s'), # cxsparse
    0x08: ('u64DataOffset', '>Q'), # 0xFFFFFFFFFFFFFFFF
    0x10: ('u64TableOffset', '>Q'), # absolute offset of Block Table Address
    0x18: ('dwVersion', '>I'), # 0x10000
    0x1C: ('dwMaxTableEntries', '>I'), # entries in BAT (=total disk blocks)
    0x20: ('dwBlockSize', '>I'), # block size (default 2 MiB)
    0x24: ('dwChecksum', '>I'),
    0x28: ('sParentUniqueId', '16s'), # UUID of parent disk in a differencing disk
    0x38: ('dwParentTimeStamp', '>I'), # Timestamp in parent's footer
    0x3C: ('dwReserved', '>I'),
    0x40: ('sParentUnicodeName', '512s'),  # Windows 10 stores the parent's absolute pathname (Big-Endian)
    0x240: ('sParentLocatorEntries', '192s'), # Parent Locators array (see later)
    # REST (256 BYTES) IS RESERVED AND MUST BE ZERO
    } # Size = 0x400 (1024 byte)

    def __init__ (self, s=None, offset=0, stream=None):
        self._i = 0
        self._pos = offset # base offset
        self._buf = s or bytearray(1024)
        self.stream = stream
        self._kv = self.layout.copy()
        self._vk = {} # { name: offset}
        for k, v in self._kv.items():
            self._vk[v[0]] = k
        self.locators = []
        for i in range(8):
            j = 0x240+i*24
            self.locators += [ParentLocator(self._buf[j:j+24])]
    
    __getattr__ = utils.common_getattr

    def pack(self):
        "Updates internal buffer"
        self.dwChecksum = 0
        for k, v in self._kv.items():
            self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(self, v[0]))
        for i in range(8):
            j = 0x240+i*24
            self._buf[j:j+24] = self.locators[i].pack()
        self._buf[0x24:0x28] = mk_crc(self._buf) # updates checksum
        return self._buf

    def __str__ (self):
        return utils.class2str(self, "VHD Dynamic Header @%X\n" % self._pos)

    def crc(self):
        crc = self._buf[0x24:0x28]
        self._buf[0x24:0x28] = '\0\0\0\0'
        c_crc = mk_crc(self._buf)
        self._buf[0x24:0x28] = crc
        return c_crc

    def isvalid(self):
        if self.sCookie != 'cxsparse':
            return 0
        if self.dwChecksum != struct.unpack(">I", self.crc())[0]:
            if DEBUG&4: log("Dynamic Header checksum 0x%X calculated != 0x%X stored", self.dwChecksum, struct.unpack(">I", self.crc())[0])
        return 1



class BAT(object):
    "Implements the Block Address Table as indexable object"
    def __init__ (self, stream, offset, blocks):
        self.stream = stream
        self.size = blocks # total blocks in the data area
        self.offset = offset # relative BAT offset
        self.decoded = {} # {block index: block effective sector}

    def __str__ (self):
        return "BAT table of %d blocks starting @%Xh\n" % (self.size, self.offset)

    def __getitem__ (self, index):
        "Retrieves the value stored in a given block index"
        if index < 0:
            index += self.size
        if DEBUG&8: log("%s: requested to read BAT[0x%X]", self.stream.name, index)
        if not (0 <= index <= self.size-1):
            raise BaseException("Attempt to read a #%d block past disk end"%index)
        slot = self.decoded.get(index)
        if slot: return slot
        pos = self.offset + index*4
        opos = self.stream.tell()
        self.stream.seek(pos)
        slot = struct.unpack(">I", self.stream.read(4))[0]
        self.decoded[index] = slot
        if DEBUG&4: log("%s: got BAT[0x%X]=0x%X @0x%X", self.stream.name, index, slot, pos)
        self.stream.seek(opos) # rewinds
        return slot

    def __setitem__ (self, index, value):
        "Sets the value stored in a given block index"
        if index < 0:
            index += self.size
        self.decoded[index] = value
        dsp = index*4
        pos = self.offset+dsp
        if DEBUG&4: log("%s: set BAT[0x%X]=0x%X @0x%X", self.stream.name, index, value, pos)
        opos = self.stream.tell()
        self.stream.seek(pos)
        value = struct.pack(">I", value)
        self.stream.write(value)
        self.stream.seek(opos) # rewinds



class ParentLocator(object):
    "Element in the Dynamic Header Parent Locators array"
    layout = { # { offset: (name, unpack string) }
    0x00: ('dwPlatformCode', '4s'), # W2ru, W2ku in Windows
    0x04: ('dwPlatformDataSpace', '>I'), # sectors needed to store the locator
    0x08: ('dwPlatformDataLength', '>I'), # locator length in bytes
    0x0C: ('dwReserved', '>I'),
    0x10: ('dwPlatformDataOffset', '>Q'), # absolute file offset where locator is stored
    } # Size = (24 byte)
    
    def __init__ (self, s):
        self._i = 0
        self._pos = 0
        self._buf = s
        self._kv = self.layout.copy()
        self._vk = {} # { name: offset}
        for k, v in self._kv.items():
            self._vk[v[0]] = k
    
    __getattr__ = utils.common_getattr

    def pack(self):
        "Updates internal buffer"
        for k, v in self._kv.items():
            self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(self, v[0]))
        return self._buf

    def __str__ (self):
        return utils.class2str(self, "Parent Locator @%X\n" % self._pos)



class BlockBitmap(object):
    "Handles the block bitmap"
    def __init__ (self, s, i):
        if DEBUG&8: log("inited Bitmap for block #%d", i)
        self.bmp = s
        self.i = i

    def isset(self, sector):
        "Tests if the bit corresponding to a given sector is set"        
        # CAVE! BIT ORDER IS LSB FIRST!
        return (self.bmp[sector/8] & (128 >> (sector%8))) != 0
    
    def set(self, sector, length=1, clear=False):
        "Sets or clears a bit or bits run"
        pos = sector/8
        rem = sector%8
        if DEBUG&8: log("set(%Xh,%d%s) start @0x%X:%d", sector, length, ('',' (clear)')[clear!=False], pos, rem)
        if rem:
            B = self.bmp[pos]
            if DEBUG&8: log("got byte {0:08b}".format(B))
            todo = min(8-rem, length)
            if clear:
                B &= ~(((0xFF<<(8-todo))&0xFF) >> rem)
            else:
                B |= (((0xFF<<(8-todo))&0xFF) >> rem)
            self.bmp[pos] = B
            length -= todo
            if DEBUG&8: log("set byte {0:08b}, left={1}".format(B, length))
            pos+=1
        octets = length/8
        while octets:
            i = min(32768, octets)
            octets -= i
            if clear:
                self.bmp[pos:pos+i] = i*['\x00']
            else:
                self.bmp[pos:pos+i] = i*['\xFF']
            pos+=i
        rem = length%8
        if rem:
            if DEBUG&8: log("last bits=%d", rem)
            B = self.bmp[pos]
            if DEBUG&8: log("got B={0:08b}".format(B))
            if clear:
                B &= ~((0xFF<<(8-rem))&0xFF)
            else:
                B |= ((0xFF<<(8-rem))&0xFF)
            self.bmp[pos] = B
            if DEBUG&8: log("set B={0:08b}".format(B))



class Image(object):
    def __init__ (self, name, mode='rb'):
        self._pos = 0 # offset in virtual stream
        self.size = 0 # size of virtual stream
        self.name = name
        self.stream = myfile(name, mode)
        self._file = self.stream
        self.mode = mode
        self.stream.seek(0, 2)
        size = self.stream.tell()
        self.stream.seek(size-512)
        self.footer = Footer(self.stream.read(512), size-512)
        self.Parent = None
        if not self.footer.isvalid():
            raise BaseException("VHD Image Footer is not valid!")
        if self.footer.dwDiskType not in (2, 3, 4):
            raise BaseException("Unknown VHD Image type!")
        if self.footer.dwDiskType in (3, 4):
            self.stream.seek(0)
            self.footer_copy = Footer(self.stream.read(512))
            if not self.footer_copy.isvalid():
                raise BaseException("VHD Image Footer (copy) is not valid!")
            if self.footer._buf != self.footer_copy._buf:
                raise BaseException("Main Footer and its copy differ!")
            self.header = DynamicHeader(self.stream.read(1024), 512)
            if not self.header.isvalid():
                raise BaseException("VHD Image Dynamic Header is not valid!")
            self.bat = BAT(self.stream, self.header.u64TableOffset, self.header.dwMaxTableEntries)
            self.block = self.header.dwBlockSize
            self.bitmap_size = max(512, (self.block/512)/8) # bitmap sectors size
        if self.footer.dwDiskType == 4: # Differencing VHD
            parent = ''
            loc = None
            for i in range(8):
                loc = self.header.locators[i]
                if loc.dwPlatformCode == 'W2ku': break # prefer absolute pathname
            if not loc:
                for i in range(8):
                    loc = self.header.locators[i]
                    if loc.dwPlatformCode == 'W2ru': break
            if loc:
                    self.stream.seek(loc.dwPlatformDataOffset)
                    parent = self.stream.read(loc.dwPlatformDataLength)
                    parent = str(parent).decode('utf-16le') # This in Windows format!
                    if DEBUG&8: log("%s: init trying to access parent image '%s'", self.name, parent)
                    if os.path.exists(parent):
                        if DEBUG&8: log("Ok, parent image found.")
            if not parent:
                hparent = self.header.sParentUnicodeName.decode('utf-16be')
                hparent = hparent[:hparent.find('\0')]
                raise BaseException("VHD Differencing Image parent '%s' not found!" % hparent)
            self.Parent = Image(parent, "rb")
            if self.Parent.footer.dwTimestamp != self.header.dwParentTimeStamp:
                if DEBUG&8: log("TimeStamps: parent=%d self=%d",  self.Parent.footer.dwTimestamp, self.header.dwParentTimeStamp)
                #~ raise BaseException("Differencing Image timestamp not matched!")
            if self.Parent.footer.sUniqueId != self.header.sParentUniqueId:
                raise BaseException("Differencing Image parent's UUID not matched!")
            self.read = self.read1 # assigns special read and write functions
            self.write = self.write1
        if self.footer.dwDiskType == 2: # Fixed VHD
            self.read = self.read0 # assigns special read and write functions
            self.write = self.write0
        self.size = self.footer.u64CurrentSize
        self.seek(0)

    def cache_flush(self):
        self.stream.flush()

    def flush(self):
        self.stream.flush()

    def seek(self, offset, whence=0):
        # "virtual" seeking, real is performed at read/write time!
        if DEBUG&8: log("%s: seek(0x%X, %d) from 0x%X", self.name, offset, whence, self._pos)
        if not whence:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        else:
            self._pos = self.size + offset
        if self._pos < 0:
            self._pos = 0
        if DEBUG&8: log("%s: final _pos is 0x%X", self.name, self._pos)
        if self._pos >= self.size:
            raise BaseException("%s: can't seek @0x%X past disk end!" % (self.name, self._pos))

    def tell(self):
        return self._pos
    
    def close(self):
        self.stream.close()
        
    def read0(self, size=-1):
        "Reads (Fixed image)"
        if size == -1 or self._pos + size > self.size:
            size = self.size - self._pos # reads all
        self.stream.seek(self._pos)
        self._pos += size
        return self.stream.read(size)

    def read(self, size=-1):
        "Reads (Dynamic, non-Differencing image)"
        if size == -1 or self._pos + size > self.size:
            size = self.size - self._pos # reads all
        buf = bytearray()
        while size:
            block = self.bat[self._pos/self.block]
            offset = self._pos%self.block
            leftbytes = self.block-offset
            if DEBUG&4: log("reading at block %d, offset 0x%X (vpos=0x%X, epos=0x%X)", self._pos/self.block, offset, self._pos, self.stream.tell())
            if leftbytes <= size:
                got=leftbytes
                size-=leftbytes
            else:
                got=size
                size=0
            self._pos += got
            if block == 0xFFFFFFFF:
                if DEBUG&4: log("block content is virtual (zeroed)")
                buf+=bytearray(got*'\0')
                continue
            self.stream.seek(block*512+self.bitmap_size+offset) # ignores bitmap sectors
            buf += self.stream.read(got)
        return buf

    def read1(self, size=-1):
        "Reads (Differencing image)"
        if size == -1 or self._pos + size > self.size:
            size = self.size - self._pos # reads all
        buf = bytearray()
        bmp = None
        while size:
            batind = self._pos/self.block
            sector = (self._pos-batind*self.block)/512
            offset = self._pos%512
            leftbytes = 512-offset
            block = self.bat[batind]
            if DEBUG&4: log("%s: reading %d bytes at block %d, offset 0x%X (vpos=0x%X, epos=0x%X)", self.name, size, batind, offset, self._pos, self.stream.tell())
            if leftbytes <= size:
                got=leftbytes
                size-=leftbytes
            else:
                got=size
                size=0
            self._pos += got
            # Acquires Block bitmap once
            if not bmp or bmp.i != block:
                if block != 0xFFFFFFFF:
                    self.stream.seek(block*512)
                    bmp = BlockBitmap(self.stream.read(self.bitmap_size), block)
            if block == 0xFFFFFFFF or not bmp.isset(sector):
                if DEBUG&4: log("reading %d bytes from parent", got)
                self.Parent.seek(self._pos-got)
                buf += self.Parent.read(got)
            else:
                if DEBUG&4: log("reading %d bytes", got)
                self.stream.seek(block*512+self.bitmap_size+sector*512+offset)
                buf += self.stream.read(got)
        return buf

    def write0(self, s):
        "Writes (Fixed image)"
        if DEBUG&8: log("%s: write 0x%X bytes from 0x%X", self.name, len(s), self._pos)
        size = len(s)
        if not size: return
        self.stream.seek(self._pos)
        self._pos += size
        self.stream.write(s)

    def write(self, s):
        "Writes (Dynamic, non-Differencing image)"
        if DEBUG&8: log("%s: write 0x%X bytes from 0x%X", self.name, len(s), self._pos)
        size = len(s)
        if not size: return
        i=0
        while size:
            block = self.bat[self._pos/self.block]
            offset = self._pos%self.block
            leftbytes = self.block-offset
            if leftbytes <= size:
                put=leftbytes
                size-=leftbytes
            else:
                put=size
                size=0
            if block == 0xFFFFFFFF:
                # allocates a new block at end before writing
                self.stream.seek(-512, 2) # overwrites old footer
                block = self.stream.tell()/512
                self.bat[self._pos/self.block] = block
                if DEBUG&4: log("allocating new block #%d @0x%X", self._pos/self.block, block*512)
                self.stream.write(self.bitmap_size*'\xFF')
                self.stream.seek(self.block, 1)
                self.stream.write(self.footer.pack())
            self.stream.seek(block*512+self.bitmap_size+offset) # ignores bitmap sectors
            if DEBUG&4: log("writing at block %d, offset 0x%X (0x%X), buffer[0x%X:0x%X]", self._pos/self.block, offset, self._pos, i, i+put)
            self.stream.write(s[i:i+put])
            i+=put
            self._pos+=put

    def write1(self, s):
        "Writes (Differencing image)"
        if DEBUG&8: log("%s: write 0x%X bytes from 0x%X", self.name, len(s), self._pos)
        size = len(s)
        if not size: return
        i=0
        bmp = None
        while size:
            block = self.bat[self._pos/self.block]
            offset = self._pos%self.block
            leftbytes = self.block-offset
            if leftbytes <= size:
                put=leftbytes
                size-=leftbytes
            else:
                put=size
                size=0
            if block == 0xFFFFFFFF:
                # allocates a new block at end before writing
                self.stream.seek(-512, 2) # overwrites old footer
                block = self.stream.tell()/512
                self.bat[self._pos/self.block] = block
                if DEBUG&4: log("%s: allocating new block #%d @0x%X", self.name, self._pos/self.block, block*512)
                self.stream.write(self.bitmap_size*'\0') # all sectors initially zeroed and unused
                self.stream.write(self.block*'\0')
                # instead of copying partial sectors from parent, we copy the full block
                #~ self.stream.write(self.bitmap_size*'\xFF')
                #~ self.Parent.seek((self._pos/self.block)*self.block)
                #~ self.stream.write(self.Parent.read(self.block))
                self.stream.write(self.footer.pack())
            if not bmp or bmp.i != block:
                if bmp: # commits bitmap
                    if DEBUG&4: log("%s: flushing bitmap for block #%d before moving", self.name, bmp.i)
                    self.stream.seek(bmp.i*512)
                    self.stream.write(bmp.bmp)
                self.stream.seek(block*512)
                bmp = BlockBitmap(self.stream.read(self.bitmap_size), block)
            def copysect(vpos, sec):
                self.Parent.seek((vpos/512)*512) # src sector offset
                blk = self.bat[vpos/self.block]
                offs = sec*512 # dest sector offset
                if DEBUG&4: log("%s: copying parent sector @0x%X to 0x%X", self.name, self.Parent.tell(), blk*512+self.bitmap_size+offs)
                self.stream.seek(blk*512+self.bitmap_size+offs)
                self.stream.write(self.Parent.read(512))
            start = offset/512
            if offset%512 and not bmp.isset(start): # if middle sector, copy from parent
                copysect(self._pos, start)
                bmp.set(start)
            stop = (offset+put-1)/512
            if (offset+put)%512 and not bmp.isset(stop):
                copysect(self._pos+put-1, stop)
                bmp.set(stop)
            bmp.set(start, stop-start+1) # sets the bitmap range corresponding to sectors written to
            self.stream.seek(block*512+self.bitmap_size+offset)
            if DEBUG&4: log("%s: writing block #%d:0x%X (vpos=0x%X, epos=0x%X), buffer[0x%X:0x%X]", self.name, self._pos/self.block, offset, self._pos, self.stream.tell(), i, i+put)
            self.stream.write(s[i:i+put])
            i+=put
            self._pos+=put
        if DEBUG&4: log("%s: flushing bitmap for block #%d at end", self.name, bmp.i)
        self.stream.seek(bmp.i*512)
        self.stream.write(bmp.bmp)



def mk_chs(size):
    "Given a disk size, computates and returns as a string the pseudo CHS for VHD Footer"
    sectors = size/512
    if sectors > 65535 * 16 * 255:
        sectors = 65535 * 16 * 255
    if sectors >= 65535 * 16 * 63:
        spt = 255
        hh = 16
        cth = sectors / spt
    else:
        spt = 17
        cth = sectors / spt
        hh = (cth+1023)/1024
        if hh < 4: hh = 4
        if cth >= hh*1024 or hh > 16:
            spt = 31
            hh = 16
            cth = sectors / spt
        if cth >= hh*1024:
            spt = 63
            hh = 16
            cth = sectors / spt
    cyls = cth / hh
    return struct.pack('>HBB', cyls, hh, spt)



def mk_crc(s):
    "Computates and returns as a string the CRC for some disk structures"
    crc = 0
    for b in s: crc += b
    return str(struct.pack('>i', ~crc))


def mk_fixed(name, size):
    "Creates an empty fixed VHD or transforms a previous image"
    ft = Footer()
    ft.sCookie = 'conectix'
    ft.dwFeatures = 2
    ft.dwFileFormatVersion = 0x10000
    ft.u64DataOffset = 0xFFFFFFFFFFFFFFFF
    ft.dwTimestamp = time.mktime(time.gmtime())-946681200
    ft.dwCreatorApp = 'Py  '
    ft.dwCreatorVer = 0x20007
    ft.dwCreatorHost = 'Wi2k'
    ft.u64OriginalSize = size
    ft.u64CurrentSize = size
    ft.dwDiskGeometry = mk_chs(size)
    ft.dwDiskType = 2
    ft.sUniqueId = uuid.uuid4().bytes
    
    if os.path.exists(name):
        if DEBUG&4: log("making new Fixed VHD '%s' of %.02f MiB from pre-existant image", name, float(size/(1<<20)))
        f = myfile(name, 'r+b')
        f.seek(size)
        f.truncate()
    else:
        if DEBUG&4: log("making new Fixed VHD '%s' of %.02f MiB", name, float(size/(1<<20)))
        f = myfile(name, 'wb')
        f.seek(size) # quickly allocates space
    f.write(ft.pack()) # stores Footer
    f.flush(); f.close()



def mk_dynamic(name, size, block=(2<<20), upto=0, overwrite='no'):
    "Creates an empty dynamic VHD"
    if os.path.exists(name) and overwrite!='yes':
        raise BaseException("Can't silently overwrite a pre-existing VHD image!")

    ft = Footer()
    ft.sCookie = 'conectix'
    ft.dwFeatures = 2
    ft.dwFileFormatVersion = 0x10000
    ft.u64DataOffset = 512
    ft.dwTimestamp = time.mktime(time.gmtime())-946681200
    ft.dwCreatorApp = 'Py  '
    ft.dwCreatorVer = 0x20007
    ft.dwCreatorHost = 'Wi2k'
    ft.u64OriginalSize = size
    ft.u64CurrentSize = size
    ft.dwDiskGeometry = mk_chs(size)
    ft.dwDiskType = 3
    ft.sUniqueId = uuid.uuid4().bytes
    
    if DEBUG&4: log("making new Dynamic VHD '%s' of %.02f MiB with block of %d bytes", name, float(size/(1<<20)), block)

    f = myfile(name, 'wb')
    f.write(ft.pack()) # stores footer copy
    
    h=DynamicHeader()
    h.sCookie = 'cxsparse'
    h.u64DataOffset = 0xFFFFFFFFFFFFFFFF
    h.u64TableOffset = 1536
    h.dwVersion = 0x10000
    h.dwMaxTableEntries = size/block
    h.dwBlockSize = block
    
    f.write(h.pack()) # stores dynamic header
    bmpsize = max(512, 4*(size/block))
    # Given a maximum virtual size in upto, the BAT is enlarged
    # for future VHD expansion
    if upto > size:
        bmpsize = max(512, 4*(upto/block))
        if DEBUG&4: log("BAT extended to %d blocks, VHD is resizable up to %.02f MiB", bmpsize/4, float(upto/(1<<20)))
    f.write(bmpsize*'\xFF') # initializes BAT
    f.write(ft.pack()) # stores footer
    f.flush(); f.close()



def mk_diff(name, base, overwrite='no'):
    "Creates an empty differencing VHD"
    if os.path.exists(name) and overwrite!='yes':
        raise BaseException("Can't silently overwrite a pre-existing VHD image!")
    ima = Image(base)
    ima.footer.dwTimestamp = time.mktime(time.gmtime())-946681200
    ima.footer.dwDiskType = 4
    ima.footer.dwCreatorApp = 'Py  '
    ima.footer.dwCreatorVer = 0x20007
    ima.footer.dwCreatorHost = 'Wi2k'

    if DEBUG&4: log("making new Differencing VHD '%s' of %.02f MiB", name, float(ima.size/(1<<20)))

    f = myfile(name, 'wb')
    f.write(ima.footer.pack()) # stores footer copy

    rel_base = os.path.relpath(base, os.path.splitdrive(base)[0]).encode('utf-16le')
    if rel_base[0] != '.': rel_base = '.\\'+rel_base
    abs_base = os.path.abspath(base).encode('utf-16le')
    be_base = os.path.abspath(base).encode('utf-16be')+'\0\0'
    
    ima.header.sParentUniqueId = ima.footer.sUniqueId
    ima.header.dwParentTimeStamp = ima.footer.dwTimestamp
    ima.header.sParentUnicodeName = be_base
    
    loc = ima.header.locators

    for i in range(8):
        loc[i].dwPlatformCode = '\0\0\0\0'
        loc[i].dwPlatformDataSpace = 0
        loc[i].dwPlatformDataLength = 0
        loc[i].dwPlatformDataOffset = 0

    bmpsize=((ima.header.dwMaxTableEntries*4+512)/512)*512

    # Windows 10 stores the relative pathname with '.\' for current dir
    # It stores both absolute and relative pathnames, tough it isn't
    # strictly necessary (but disk manager silently fixes this)
    loc[0].dwPlatformCode = 'W2ru'
    loc[0].dwPlatformDataSpace = ((len(rel_base)+512)/512)*512
    loc[0].dwPlatformDataLength = len(rel_base)
    loc[0].dwPlatformDataOffset = 1536+bmpsize

    loc[1].dwPlatformCode = 'W2ku'
    loc[1].dwPlatformDataSpace = ((len(abs_base)+512)/512)*512
    loc[1].dwPlatformDataLength = len(abs_base)
    loc[1].dwPlatformDataOffset = loc[0].dwPlatformDataOffset+loc[0].dwPlatformDataSpace
        
    f.write(ima.header.pack()) # stores dynamic header

    f.write(bmpsize*'\xFF') # initializes BAT

    f.write(rel_base+'\0'*(loc[0].dwPlatformDataSpace-len(rel_base))) # stores relative parent locator sector
    f.write(abs_base+'\0'*(loc[1].dwPlatformDataSpace-len(abs_base))) # stores absolute parent locator sector

    f.write(ima.footer.pack()) # stores footer
    f.flush(); f.close()
