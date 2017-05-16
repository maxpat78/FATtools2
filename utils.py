# -*- coding: mbcs -*-
import struct

def class2str(c, s):
	"Enumera in tabella nomi e valori dal layout di una classe"
	keys = c._kv.keys()
	keys.sort()
	for key in keys:
		o = c._kv[key][0]
		v = getattr(c, o)
		if type(v) in (type(0), type(0L)):
			v = hex(v)
		s += '%x: %s = %s\n' % (key, o, v)
	return s

def common_getattr(c, name):
	"Decodifica e salva un attributo in base al layout di classe"
	i = c._vk[name]
	fmt = c._kv[i][1]
	cnt = struct.unpack_from(fmt, c._buf, i+c._i) [0]
	setattr(c, name,  cnt)
	return cnt

# Use hasattr to determine is value was previously unpacked, or avoid repacking?
def pack(c):
	"Update internal buffer"
	for k in c._kv.keys():
		v = c._kv[k]
		c._buf[k:k+struct.calcsize(v[1])] = struct.pack(v[1], getattr(c, v[0]))
	return c._buf

def common_setattr(c, name, value):
	"Imposta e codifica un attributo in base al layout di classe"
	object.__setattr__(c, name,  value)
	i = c._vk[name]
	fmt = c._kv[i][1]
	struct.pack_into(fmt, c._buf, i+c._i, value)

def FSguess(boot):
    "Try to guess the file system type between FAT12/16/32, exFAT and NTFS examining the boot sector"
    if boot.chOemID.startswith('NTFS'):
        return 'NTFS'
    if boot.wBytesPerSector == 0:
        return 'EXFAT'
    if boot.wMaxRootEntries == 0:
        return 'FAT32'
    if boot.sFSType in('FAT12', 'FAT16'):
        return boot.sFSType
    if boot.wMaxRootEntries < 512:
        return 'FAT12'
    return 'FAT16'
