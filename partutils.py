"""
La numerazione CHS 24-bit inizia di regola dal settore 1. I tre byte nell'ordine:

        H (8 bit)     S (6 bit)   C (8+2 bit)
        |             |           |
    HHHHHHHH -+- CC SSSSSS -+- CCCCCCCC

e, quindi, i valori massimi sono: C=1024 (0-1023), H=256 (0-255), S=63 (1-63).

Fino a MS-DOS 7.10 le testine erano limitate a 255!

Se l'indirizzo LBA non e' rappresentabile in CHS (vale a dire, il disco eccede
ca. 8 GB), si usa la terna (1023, 254, 63) o FE FF FF per il settore finale.
EFI adotta nel MBR di protezione la terna (1023, 255, 63) o FF FF FF."""

sectors_per_track = 63
heads_per_cylinder = 255

def chs(LBA):
    "Calcola la tupla (Cilindro,Testina,Settore) a partire da un indirizzo LBA"
    cylinder = LBA / (heads_per_cylinder * sectors_per_track)
    temp = LBA % (heads_per_cylinder * sectors_per_track)
    head = temp / sectors_per_track
    sector = temp % sectors_per_track + 1
    return cylinder, head, sector

def raw_chs(t):
    "A partire da una tupla (C,H,S) calcola i 3 byte nell'ordine registrato nel Master Boot Record"
    c,h,s = t
    lba = c*heads_per_cylinder*sectors_per_track + h*sectors_per_track + s - 1
    
    if lba > 1024*heads_per_cylinder*sectors_per_track + heads_per_cylinder*sectors_per_track + sectors_per_track:
        B1, B2, B3 = 254, 255, 255
    else:
        B1, B2, B3 = h, (c&768)>>2|s, c&255
    #~ print "DEBUG: MBR bytes for LBA %d (%Xh): %02Xh %02Xh %02Xh"%(lba, lba, B1, B2, B3)
    return '%c%c%c' % (B1, B2, B3)

def raw_2_chs(t):
    h,s,c = t
    return ((s & 192) << 2) | c, h, s & 63



class MBR(object):
    "Master Boot Record Sector (usually disk sector #0)"
    layout = { # { offset: (name, unpack string) }
    0x1BE: ('bStatus', 'B'), # 80h=bootable, 00h=not bootable, other=invalid
    0x1BF: ('sFirstSectorCHS', '3s'), # CHS address of 1st sector in this partition
    0x1C2: ('bType', 'B'), # partition type: 7=NTFS, exFAT; C=FAT32 LBA; E=FAT16 LBA
    0x1C3: ('sLastSectorCHS', '3s'), # CHS address of last sector in this partition (or FE FF FF if >8GB)
    0x1C6: ('dwFirstSectorLBA', '<I'), # LBA address of 1st sector in this partition
    0x1CA: ('dwTotalSectors', '<I'), # number of sectors in this partition
    # 3 identical 16-byte groups corresponding to the other 3 primary partitions follow
    # Modern Windows prefer dwFirstSectorLBA and dwTotalSectors
    0x1FE: ('wBootSignature', '<H') # 55 AA
    } # Size = 0x200 (512 byte)

    def __init__ (self, s=None, offset=0, stream=None):
        self._i = 0
        self._pos = offset # base offset
        self._buf = s or bytearray(512) # normal MBR size
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
        return utils.class2str(self, "MBR Sector @%X\n" % self._pos)

