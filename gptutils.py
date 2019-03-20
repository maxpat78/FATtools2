# -*- coding: cp1252 -*-
import utils, struct, uuid, zlib

DEBUG = 0
from debug import log

# Common Windows Partition GUIDs
partition_uuids = {
    uuid.UUID('00000000-0000-0000-0000-000000000000'): None,
    uuid.UUID('C12A7328-F81F-11D2-BA4B-00A0C93EC93B'): 'EFI System Partition',
    uuid.UUID('E3C9E316-0B5C-4DB8-817D-F92DF00215AE'): 'Microsoft Reserved Partition',
    uuid.UUID('EBD0A0A2-B9E5-4433-87C0-68B6B72699C7'): 'Microsoft Basic Data Partition',
    uuid.UUID('DE94BBA4-06D1-4D40-A16A-BFD50179D6AC'): 'Microsoft Windows Recovery Environment',
}



class GPT(object):
    "GPT Header Sector according to UEFI Specs"
    layout = { # { offset: (name, unpack string) }
    0x0: ('sEFISignature', '8s'), # EFI PART
    0x8: ('dwRevision', '<I'), # 0x10000
    0xC: ('dwHeaderSize', '<I'), # 92 <= size <= blksize
    0x10: ('dwHeaderCRC32', '<I'), # CRC32 on dwHeaderSize bytes (this field zeroed)
    0x14: ('dwReserved', '<I'), # must be zero
    0x18: ('u64MyLBA', '<Q'), # LBA of this structure
    0x20: ('u64AlternateLBA', '<Q'), # LBA of backup GPT Header (typically last block)
    0x28: ('u64FirstUsableLBA', '<Q'),
    0x30: ('u64LastUsableLBA', '<Q'),
    0x38: ('u64DiskGUID', '16s'),
    0x48: ('u64PartitionEntryLBA', '<Q'), # LBA of GUID Part Entry array
    0x50: ('dwNumberOfPartitionEntries', '<I'),
    0x54: ('dwSizeOfPartitionEntry', '<I'), # 128*(2**i)
    0x58: ('dwPartitionEntryArrayCRC32', '<I') # The CRC32 of the GUID Partition Entry array
    # REST IS RESERVED AND MUST BE ZERO
    } # Size = 0x200 (512 byte)

    def __init__ (self, s=None, offset=0, stream=None):
        self._i = 0
        self._pos = offset # base offset
        self._buf = s or bytearray(512) # normal MBR size
        self.stream = stream
        self._kv = self.layout.copy()
        self._vk = {} # { name: offset}
        self.partitions = []
        for k, v in self._kv.items():
            self._vk[v[0]] = k
    
    __getattr__ = utils.common_getattr

    def pack(self):
        "Update internal buffer"
        self._crc32()
        for k, v in self._kv.items():
            self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(self, v[0]))
        #~ for i in self.partitions:
            #~ for k, v in i._kv.items():
                #~ self._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(i, v[0]))
        return self._buf

    def __str__ (self):
        return utils.class2str(self, "GPT Header @%X\n" % self._pos)

    def _crc32(self):
        s = self._buf.tolist()
        s[0x10:0x14] = [0,0,0,0]
        crc = zlib.crc32(''.join(map(chr, s[:self.dwHeaderSize])))
        self.dwHeaderCRC32 = crc
        if DEBUG&1: log("_crc32 returned %08X on GPT Header", crc)
        return crc
    
    def parse(self, s):
        "Parse the GUID Partition Entry Array"
        for i in range(self.dwNumberOfPartitionEntries):
            j = i*self.dwSizeOfPartitionEntry
            k = j + self.dwSizeOfPartitionEntry
            self.partitions += [GPT_Partition(s[j:k], index=i)]


class GPT_Partition(object):
    "Partition entry in GPT Array (128 bytes)"
    layout = { # { offset: (name, unpack string) }
    0x00: ('sPartitionTypeGUID', '16s'),
    0x10: ('sUniquePartitionGUID', '16s'),
    0x20: ('u64StartingLBA', '<Q'),
    0x28: ('u64EndingLBA', '<Q'),
    0x30: ('u64Attributes', '<Q'),
    0x38: ('sPartitionName', '72s')
    # REST IS RESERVED AND MUST BE ZERO
    } # Size = 0x80 (128 byte)

    def __init__ (self, s=None, offset=0, index=0):
        self.index = index
        self._i = 0
        self._pos = offset # base offset
        self._buf = s or bytearray(512)
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
        return utils.class2str(self, "GPT Partition Entry #%d\n" % self.index)

    def gettype(self):
        return uuid.UUID(bytes_le=self.sPartitionTypeGUID)
    
    def uuid(self):
        return uuid.UUID(bytes_le=self.sUniquePartitionGUID)
        
    def name(self):
        name = self.sPartitionName.decode('utf-16le')
        name = name[:name.find('\x00')]
        return name
