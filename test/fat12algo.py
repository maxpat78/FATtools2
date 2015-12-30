# -*- coding: mbcs -*-

#~ Compare FAT12 algorithms using a real FAT took from a full floppy
#~ (all clusters sequentially allocated)

import struct, random

s = bytearray(open('SAMPLE_200h_1200h.bin', 'rb').read())

z = bytearray(len(s))
z[:3] = (0xF0, 0xFF, 0xFF)


def alter(i, s, z, v=None):
    pos = (i*12)/8
    orig = struct.unpack('<H', s[pos:pos+2])[0]
    if i % 2: # odd index
        slot = orig >> 4
    else:
        slot = orig & 0x0FFF
    try:
        assert slot == (i+1, 0xFFF)[i==2848]
    except:
        print "error, fat[%d] != %d+1" % (i,i)

    if v:
        n = v
    else:
        n = (i+1, 0xFFF)[i==2848]
    if i % 2: # odd cluster
        # Value's 12 bits moved to top ORed with original bottom 4 bits
        value = (n << 4) | (orig & 0xF)
    else:
        # Original top 4 bits ORed with value's 12 bits
        value = (orig & 0xF000) | n

    z[pos:pos+2] = struct.pack('<H', value)


def check(s, z):
    print "Comparing buffers..."
    for i in xrange(len(s)):
        if s[i] != z[i]:
            print "%08X: %02X  %02X" % (i, s[i], z[i])
    print "Done."


print "Write #1"
for i in range(2, 2849):
    alter(i, s, z)
check(s, z)

print "Write #2"
for i in range(2, 2849):
    alter(i, s, z)
check(s, z)

print "Write even"
for i in range(2, 2849, 2):
    alter(i, s, z)
check(s, z)

print "Write odd"
for i in range(3, 2849, 2):
    alter(i, s, z)
check(s, z)

print "Write randomly"
for i in range(2, 2849):
    j = random.randint(3, 2847)
    alter(j, s, z)
    alter(i, s, z)
check(s, z)

print "Write 0xFFF"
for i in range(2, 2849):
    alter(i, s, z, 0xFFF)
for i in range(2, 2849):
    alter(i, s, z)
check(s, z)
