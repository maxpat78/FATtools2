from ctypes import *
from zlib import crc32
import sys

""" Windows NT short name generation scheme:
- use Win 95 scheme for first four collisions, appending ~1...4 to the
  contracted name;
- then switch to XXHHHH~N.XXX (where XX=first 2 chars from base name,
  HHHH=16-bit hash, N=1..9 and XXX=first 3 chars from extension;
- if such an alias is in use, increase tilded index up to 999999 reducing the
  base short name, like Win 95 does.

The advantage is the high probability of generating an unused alias at first
attempt, and an alias mathematically bound to its long name.
Windows NT 3.51/4, 2000/XP/Vista and 7-8-8.1-10 use different 16-bit hash
algorithms.
But a real CRC would be more efficient: it would provide the minimum 
collisions. However, Windows 7 hash is closer to CRC results."""

def RtlGenerate8dot3Name(s, iterations=5):
    "Returns the first short name bound to a long one by directly calling NTDLL RtlGenerate8dot3Name"
    
    class GENERATE_NAME_CONTEXT(Structure):
        _fields_ = [("Checksum", c_ushort), ("ChecksumInserted", c_byte), ("NameLength", c_byte),
        ("NameBuffer", c_wchar*8),("ExtensionLength", c_uint),("ExtensionBuffer", c_wchar*4),("LastIndexValue", c_uint)]

    class UNICODE_STRING(Structure):
        _fields_ = [("Length", c_ushort), ("MaximumLength", c_ushort), ("Buffer", c_wchar_p)]

    ctx = GENERATE_NAME_CONTEXT()
    
    us = UNICODE_STRING()
    us.Length = len(s)*2
    us.Buffer = c_wchar_p(s)
    
    us1 = UNICODE_STRING()
    us1.MaximumLength = 26
    z = create_string_buffer(26)
    us1.Buffer = addressof(z)

    for i in range(iterations):
        windll.NTDLL.RtlGenerate8dot3Name(byref(us), 0, byref(ctx), byref(us1))

    return us1.Buffer


def hash_7(name):
    "Computes the Windows 7+ long file name checksum"
    name = bytearray(name)
    crc = 0 # 16-bit
    for c in name:
        crc = crc*0x25 + c
    crc &= 0xFFFF
    t = crc * 314159269 # 32-bit
    t &= 0xFFFFFFFF
    if t > 0x7FFFFFFF: # if negative, change into positive
        t *= -1
        t &= 0xFFFFFFFF
    d = ((t * 1152921497) >> 60) * 1000000007
    d &= 0xFFFFFFFF
    t -= d
    t &= 0xFFFF
    return ((t&0xF000)>>12)|((t&0xF00)>>4)|((t&0xF0)<<4)|((t&0xF)<<12)
    
def hash_NT(name, variant='2k'):
    "Computes the Windows NT long file name checksum"
    name = bytearray(name)
    if variant == '2k': # 2000/XP/Vista
        sum = ((name[0]<<8) + name[1]) & 0xFFFF
    else: # NT 3.51, 4.0
        sum = (name[0] << (8+name[1])) & 0xFFFF
    i = 2
    while i < len(name):
        sum = (0, 0x8000)[sum&1] + (sum>>1) + (name[i]<<8)
        sum &= 0xFFFF
        if i+1 < len(name):
            sum += name[i+1] & 0xFFFF
        i+=2
    return ((sum&0xF000)>>12)|((sum&0xF00)>>4)|((sum&0xF0)<<4)|((sum&0xF)<<12)



if __name__ == '__main__':
    for i in range(256):
        s = 'File name %d.txt' % i
        print s
        print "LFN Windows short=%s, LFN hash: Seven=%04X, 2K=%04X, NT=%04X" % (RtlGenerate8dot3Name(s), hash_7(s), hash_NT(s), hash_NT(s,'nt'))

    #~ sys.exit(1)

#~ Performances with 65537*16 file names - (max, avg) collisions:
#~ hashNT nt (max, avg): 328, 42.655
#~ hashNT 2k (max, avg): 163, 39.653
#~ hash7 (max, avg): 63, 22.912
#~ crc32 (max, avg): 29, 16.000
    names = ('File Name %d.txt', 'First File %d.log', 'Filled Doc %d.bin', 'File%06d.raw')
    for hash in (lambda x: hash_NT(x,'nt'), hash_NT, hash_7, lambda x: crc32(x)&0xFFFF):
        d={}
        for i in range(65537*16):
            s = names[i%4] % i
            c = hash(s)
            v = d.get(c)
            if v:
                d[c] = v+1
            else:
                d[c] = 1
        print "Collisions for 65537*16 LFNS with hash %s (max, avg): %d, %.3f" % (hash, max(d.values()), sum(d.values())/float(len(d.values())))
