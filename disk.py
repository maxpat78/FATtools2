# -*- coding: mbcs -*-
import os, sys, atexit
import logging
from ctypes import *
#~ import hexdump

class win32_disk(object):
	"Handles a Win32 disk"
	open_handles = {}

	def __init__(self, name, mode='rb', buffering=0):
		status = c_int(0)
		# Open a new write handle
		if name in win32_disk.open_handles:
			handle = win32_disk.open_handles[name]
		else:
			handle = windll.kernel32.CreateFileA(name, 0xC0000000, 3, 0, 3, 0x80000000|0x10000000|0x20000000, 0)
		assert handle != -1
		win32_disk.open_handles[name] = handle
		# Dismount volume, gaining exclusive access with FSCTL_DISMOUNT_VOLUME (0x90020)
		# Dismount volume, locking it for exclusive access with FSCTL_LOCK_VOLUME (0x90018)
		# IOCTL_DISK_GET_LENGTH_INFO = 0x7405C
		# THIS IS MANDATORY FOR WRITE ACCESS! (Windows Vista+)
		# Also it could require Admin rights for non-removable volumes!
		# Suggest: open NON-CACHED
		#  To read or write to the last few sectors of the volume, you must call DeviceIoControl and specify FSCTL_ALLOW_EXTENDED_DASD_IO
		#~ if windll.kernel32.DeviceIoControl(handle, 0x90020, 0, 0, 0, 0, byref(status), 0):
		if windll.kernel32.DeviceIoControl(handle, 0x90020, 0, 0, 0, 0, byref(status), 0):
			# 5 = ACCESS DENIED 6= INVALID HANDLE
			if windll.kernel32.GetLastError():
				assert 1
		if windll.kernel32.DeviceIoControl(handle, 0x90018, 0, 0, 0, 0, byref(status), 0):
			# 5 = ACCESS DENIED 6= INVALID HANDLE
			if windll.kernel32.GetLastError():
				assert 2
		GET_LENGTH_INFORMATION = c_ulonglong(0)
		if windll.kernel32.DeviceIoControl(handle,  0x7405C, 0, 0, byref(GET_LENGTH_INFORMATION), 8, byref(status), 0):
			self.size = GET_LENGTH_INFORMATION.value
		else:
			self.size = 0
		self.handle = handle
		self.name = name
		self.mode = mode
		self.buf = create_string_buffer(4096)
		logging.debug("Successfully opened HANDLE to Win32 Disk %s (size %d MB) for exclusive access", name, self.size/(1<<20))

	def seek(self, offset, whence=0):
		if offset != windll.kernel32.SetFilePointer(self.handle, offset, 0, whence):
			print "seek failed with error", windll.kernel32.GetLastError()
			sys.exit(1)

	def tell(self):
		offset = windll.kernel32.SetFilePointer(self.handle, 0, 0, 1)
		if windll.kernel32.GetLastError():
			print "tell failed with error", windll.kernel32.GetLastError()
			sys.exit(1)
		return offset

	def readinto(self, buf):
		assert len(buf) > 0
		n = c_int(0)
		if len(buf) <= 4096:
			s = self.buf
		else:
			s = create_string_buffer(len(buf))
		if not windll.kernel32.ReadFile(self.handle, s, len(buf), byref(n), 0):
			print "readinto failed with error %d" % windll.kernel32.GetLastError()
			sys.exit(1)
		buf[0:len(buf)] = s.raw[0:len(buf)] # is there a more direct method???

	def read(self, size=-1):
		assert size != -1
		n = c_int(0)
		if len(buf) <= 4096:
			s = self.buf
		else:
			s = create_string_buffer(len(buf))
		if not windll.kernel32.ReadFile(self.handle, buf, size, byref(n), 0):
			print "read(%d) failed with error %d" % (size, windll.kernel32.GetLastError())
			sys.exit(1)
		return buf

	def write(self, s):
		if not len(s): return
		# This type-conversion code is required on Windows 10 amd64
		# (Python x86 WOW64), NOT IN x86
		if type(s) == type(bytearray()):
			s = str(s)
		elif type(s) == type(''):
			pass
		else: #memoryview
			s = s.tobytes()
		if not windll.kernel32.WriteFile(self.handle, s, len(s), 0, 0):
			print "write() failed with error", windll.kernel32.GetLastError()
			sys.exit(1)



class disk(object):
	"""Access a Windows NT+ disk like a regular file. It works with plain files, too.
	PLEASE NOTE THAT: 1) read, write and seek MUST be aligned at sector (512 bytes) offsets
	2) seek from disk's end does not work; 3) seek past disk's end followed by a read
	returns no error."""

	def __init__(self, name, mode='rb', buffering=0):
		self.pos = 0 # linear pos in the virtual stream
		self.si = 0 # disk sector index
		self.so = 0 # sector offset
		self.lastsi = 0 # last sector read from *disk*
		self.buf = None # read buffer
		self.blocksize = 512 # fixed sector size
		# Cache only small 512 sectors
		self.rawcache = bytearray(512<<10) # 512K cache buffer
		self.cache = memoryview(self.rawcache)
		self.cache_index = 0 # offset of next cache slot
		self.cache_hits = 0 # sectors retrieved from cache
		self.cache_misses = 0 # sectors not retrieved
		self.cache_extras = 0 # direct, non-cacheable I/O
		self.cache_dirties = {} # dirty sectors
		self.cache_table = {} # { sector: cache offset }
		self.cache_tableR = {} # reversed: { cache offset:sector }
		if os.name == 'nt' and '\\\\.\\' in name:
			self._file = win32_disk(name, mode, buffering)
			self.size = self._file.size
		else:
			self._file = open(name, mode, buffering)
			self.size = os.stat(name).st_size
		atexit.register(self.cache_flush)

	def seek(self, offset, whence=0):
		if whence == 1:
			self.pos += offset
		elif whence == 2:
			if self.size and offset < self.size:
				self.pos = self.size - offset
			elif self.size and offset >= self.size:
				self.pos = 0
		else:
			self.pos = offset
		self.si = self.pos / self.blocksize
		self.so = self.pos % self.blocksize
		#~ logging.debug("disk pointer to set @%Xh", self.si*self.blocksize)
		self._file.seek(self.si*self.blocksize)
		#~ logging.debug("si=%Xh lastsi=%Xh so=%Xh", self.si,self.lastsi,self.so)

	def tell(self):
		return self.pos

	def cache_stats(self):
		logging.debug("Cache items/hits/misses: %d/%d/%d", len(self.cache_table), self.cache_hits, self.cache_misses)

	def cache_flush(self, sector=None):
		if not self.cache_dirties:
			return
		if sector != None: # assume it is called by cache_retrieve only, with the right sector #
			self._file.seek(sector*self.blocksize)
			i = self.cache_table[sector]
			self._file.write(self.cache[i:i+self.blocksize])
			del self.cache_dirties[sector]
			#~ logging.debug("%s: flushed sector #%d from cache[%d]", self, sector,i/512)
			return
		#~ logging.debug("%s: flushing %d dirty sectors from cache", self, len(self.cache_dirties))
		# Should writes be ordered and merged if possible?
		for sec in self.cache_dirties:
			self._file.seek(sec*self.blocksize)
			i = self.cache_table[sec]
			self._file.write(self.cache[i:i+self.blocksize])
		#~ logging.debug("Cache items/hits/misses: %d/%d/%d", len(self.cache_table), self.cache_hits, self.cache_misses)
		self.cache_dirties = {}
		self.cache_table = {}
		self.cache_tableR = {}

	def cache_retrieve(self):
		"Retrieve a sector from cache. Returns True if hit, False if missed."
		# If we are retrieving a single block...
		if self.asize ==self.blocksize:
			if self.si not in self.cache_table:
				self.cache_misses += 1
				#~ logging.debug("%s: cache_retrieve missed %d", self, self.si)
				return False
			self.cache_hits += 1
			i = self.cache_table[self.si]
			self.buf = self.cache[i:i+self.asize]
			#~ logging.debug("%s: cache_retrieve hit %d", self, self.si)
			return True

		# If we are retrieving multiple blocks...
		for i in range(self.asize/self.blocksize):
			# If one block is not cached...
			if self.si+i not in self.cache_table:
				#~ logging.debug("%s: cache_retrieve (multisector) miss-not cached %d", self, self.si+i)
				self.cache_misses += 1
				continue
			# If one block is dirty, first flush it...
			if self.si+i in self.cache_dirties:
				#~ logging.debug("%s: cache_retrieve (multisector) flush %d", self, self.si+i)
				self.cache_flush(self.si+i)
				#~ logging.debug("%s: seeking back @%Xh after flush", self, self.pos)
				self.seek(self.pos)
				continue
		return False # consider a miss

	def cache_readinto(self):
		# If we should read beyond the cache's end...
		if self.cache_index + self.asize > len(self.cache):
			# Free space, flushing dirty sectors & updating cache index
			self.cache_flush()
			self.cache_index = 0
			self.seek(self.pos)
		pos = self.cache_index
		#~ logging.debug("loading disk sector #%d into cache[%d] from offset %Xh", self.si, pos/512, self._file.tell())
		#~ logging.debug("loading disk sector #%d into cache[%d]", self.si, pos/512)
		self._file.readinto(self.cache[pos:pos+self.asize])
		self.buf = self.cache[pos:pos+self.asize]
		#~ if self.si == 50900:
			#~ logging.debug("Loaded sector #50900:\n%s", hexdump.hexdump(self.cache[pos:pos+self.asize],'return'))
		self.cache_index += self.asize
		# Update dictionary of cached sectors and their position
		# Invalidate accordingly if we are recycling pool from zero?
		k = self.si
		v = pos
		# If a previously cached sector is pointing to the same buffer,
		# unlink it
		if v in self.cache_tableR:
			del self.cache_table[self.cache_tableR[v]]
		self.cache_table[k] = v
		self.cache_tableR[v] = k
		return pos

	def read(self, size=-1):
		#~ logging.debug("read(%d) bytes @%Xh", size, self.pos)
		self.seek(self.pos)
		# If size is negative
		if size < 0:
			size = 0
			if self.size: size = self.size
		# If size exceeds disk size
		if self.size and self.pos + size > self.size:
			size = self.size - self.pos
		se = (self.pos+size)/self.blocksize
		if (self.pos+size)%self.blocksize:
			se += 1
		self.asize = (se - self.si) * self.blocksize # full sectors to read in
		# If sectors are already cached...
		if self.cache_retrieve():
			#~ logging.debug("%d bytes read from cache", self.asize)
			self.lastsi = self.si
			self.pos += size
			return self.buf[self.so : self.so+size]
		# ...else, read them from disk...
		# if larger than cache limit, read directly into a new buffer
		if self.asize > self.blocksize:
			self.buf = bytearray(self.asize)
			self._file.seek(self.si*self.blocksize)
			#~ logging.debug("reading %d bytes directly from disk @%Xh", self.asize, self._file.tell())
			self._file.readinto(self.buf)
			# Direct read (bypass) DON'T advance lastsi? Or file pointer?
			self.si += self.asize/self.blocksize # 11.01.2016: fix mkexfat flaw
			self.pos += size
			self.cache_extras += 1
			return self.buf[self.so : self.so+size]
		# ...else, update the cache
		self.cache_readinto()
		self.lastsi = self.si
		self.pos += size
		return self.buf[self.so : self.so+size]

	def write(self, s): # s MUST be of type bytearray/memoryview
		#~ logging.debug("request to write %d bytes @%Xh", len(s), self.pos)
		if len(s) == 0: return
		# If we have to complete a sector...
		if self.so:
			j = min(self.blocksize - self.so, len(s))
			#~ logging.debug("writing middle sector %d[%d:%d]", self.si, self.so, self.so+j)
			self.asize = 512
			if not self.cache_retrieve():
				self.cache_readinto()
			# We assume buf is pointing to rawcache
			self.buf[self.so : self.so+j] = s[:j]
			s = s[j:] # slicing penalty if buffer?
			#~ logging.debug("len(s) is now %d", len(s))
			self.cache_dirties[self.si] = True
			self.pos += j
			self.seek(self.pos)
		# if we have full sectors to write...
		if len(s) > self.blocksize:
			full_blocks = len(s)/512
			#~ logging.debug("writing %d sector(s) directly to disk", full_blocks)
			# Directly write full sectors to disk
			#~ self._file.seek(self.si*self.blocksize)
			# Invalidate eventually cached data
			for si in xrange(self.si, self.si+full_blocks):
				if si in self.cache_table:
					#~ logging.debug("removing sector #%d from cache", si)
					Ri = self.cache_table[si]
					del self.cache_tableR[Ri]
					del self.cache_table[si]
					if si in self.cache_dirties:
						#~ logging.debug("removing sector #%d from dirty sectors", si)
						del self.cache_dirties[si]
			self._file.write(s[:full_blocks*512])
			self.pos += full_blocks*512
			self.seek(self.pos)
			s = s[full_blocks*512:] # slicing penalty if buffer?
			#~ logging.debug("len(s) is now %d", len(s))
		if len(s):
			#~ logging.debug("writing sector %d[%d:%d] from start", self.si, self.so, self.so+len(s))
			self.asize = 512
			if not self.cache_retrieve():
				self.cache_readinto()
			self.buf[self.so : self.so+len(s)] = s
			self.cache_dirties[self.si] = True
			self.pos += len(s)
		self.seek(self.pos)
		self.lastsi = self.si


if __name__ == '__main__':
	import logging
	logging.basicConfig(level=logging.DEBUG, filename='PYTEST.LOG', filemode='w')

	from random import randint, shuffle

	open('TESTIMAGE.BIN', 'wb').write(bytearray(4<<20))

	d = disk('TESTIMAGE.BIN', 'r+b')
	d.rawcache = bytearray(4<<20)
	d.cache = memoryview(d.rawcache)

	print "Testing cached random writes & reads..."
	# Blank first 10K
	d.write(bytearray(16*512))
	sectors = range(16)
	shuffle(sectors)
	values = [randint(0,255) for i in range(16)]
	offsets = [randint(0,511) for i in range(16)]
	# Write a random byte at a random position in 16 sectors
	# write them in random order; then read full block
	print "sectors", sectors
	print "offsets", offsets
	print "values", values
	for i in sectors:
		d.seek(i*512+offsets[i])
		d.write(chr(values[i]))
	d.seek(0)
	s = bytearray(d.read(16*512))
	for i in sectors:
		assert chr(s[i*512+offsets[i]]) == chr(values[i])
	# Overwrite area with F8, write full block then read sectors
	d.seek(0)
	d.write(bytearray(16*512*'\xF8'))
	d.seek(0)
	d.write(s)
	for i in sectors:
		d.seek(i*512+offsets[i])
		assert bytearray(d.read(1)) == chr(values[i])
	print "Caching tests passed!"

	print "Testing sequential writing & reading byte-for-byte of 2 sectors..."
	d.seek(0)
	for i in range(1024):
		d.write(chr(i&0xFF))
	d.seek(0)
	for i in range(1024):
		assert chr(i&0xFF) == bytearray(d.read(1))
	print "Test passed"

	print "Testing cross sector writing & reading..."
	for i in range(16):
		d.seek((i+1)*512-3)
		d.write('ABCDEF')
		d.seek((i+1)*512-3)
		assert bytearray(d.read(6)) == 'ABCDEF'
	print "Test passed!"

	d.seek(0)
	d.write(16*512*2*'\xF1')
	d.seek(0)
	import struct
	for i in range(16*512):
		orig = bytearray(d.read(2))
		try:
			assert orig == bytearray('\xF1\xF1')
		except:
			print i, "'%s' differ from F1F1h at %Xh" % (orig, d.tell()-2)
			assert 0
		d.seek(-2, 1)
		d.write(struct.pack('<H', i))
		d.seek(-2, 1)
		assert struct.pack('<H', i) == str(bytearray(d.read(2)))
		d.seek(-2, 1)
		d.write(orig)
		d.seek(-2, 1)
		assert orig == bytearray(d.read(2))

	print "Testing sequential writing & reading 2byte-for-2byte of 2 sectors..."
	d.seek(3)
	for i in range(512):
		d.write(2*chr(i&0xFF))
	d.seek(3)
	for i in range(512):
		assert bytearray(2*chr(i&0xFF)) == bytearray(d.read(2))
	print "Test passed"

