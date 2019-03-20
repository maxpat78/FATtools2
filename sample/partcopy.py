""" Scans a GPT partition table and offers to save a dump of each one in current directory. """
import struct, os, sys, uuid, time, disk
import ctypes

from gptutils import *

try:
    import lz4f
except:
    lz4f = None

class lz4file(file):
    "Filter with LZ4 compression"
    def __init__ (p, name, mode):
        p.ctx = lz4f.createCompContext()
        p.outf = open(name+'.lz4', mode)
        p.outf.write(lz4f.compressBegin(p.ctx, None))
    def write(p, s):
        cs = lz4f.compressUpdate(str(s), p.ctx)
        p.outf.write(cs)
    def close(p):
        p.outf.write(lz4f.compressEnd(p.ctx))
        lz4f.freeCompContext(p.ctx)


# Common Windows Partition GUIDs
partition_uuids = {
    uuid.UUID('00000000-0000-0000-0000-000000000000'): None,
    uuid.UUID('C12A7328-F81F-11D2-BA4B-00A0C93EC93B'): 'EFI System Partition',
    uuid.UUID('E3C9E316-0B5C-4DB8-817D-F92DF00215AE'): 'Microsoft Reserved Partition',
    uuid.UUID('EBD0A0A2-B9E5-4433-87C0-68B6B72699C7'): 'Microsoft Basic Data Partition',
    uuid.UUID('DE94BBA4-06D1-4D40-A16A-BFD50179D6AC'): 'Microsoft Windows Recovery Environment',
}

def get_size(disk):
    "Returns the size in bytes of a disk or partition"
    if 'linux' in sys.platform:
        proc = '/sys/class/block/%s/size' % disk[4:] # /dev/sda --> sda
        return int(open(proc,'rb').read()) * 512
    else:
        handle = ctypes.windll.kernel32.CreateFileA(disk, 0xC0000000, 3, 0, 3, 0x80000000|0x10000000|0x20000000, 0)
        GET_LENGTH_INFORMATION = ctypes.c_ulonglong(0)
        status = ctypes.c_int(0)
        ctypes.windll.kernel32.DeviceIoControl(handle,  0x7405C, 0, 0, ctypes.byref(GET_LENGTH_INFORMATION), 8, ctypes.byref(status), 0)
        ctypes.windll.kernel32.CloseHandle(handle)
        return GET_LENGTH_INFORMATION.value

def mk_vhd_fixed_footer(size):
    "Generates a VHD footer to append to a fixed disk image"
    def mk_chs(size):
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
        crc = 0
        for b in s: crc += b
        return str(struct.pack('>i', ~crc))

    # 946681200=seconds elapsed since 1/1/2000
    s = struct.pack('>8sIIQI4sI4s2Q4sII16sB', 'conectix', 2, 0x10000, 0xFFFFFFFFFFFFFFFF,\
    time.time()-946681200, 'py  ', 0x00020007, 'Wi2k',\
    size, size, mk_chs(size), 2, 0, uuid.uuid4().bytes, 0)
    s = bytearray(s+427*'\x00')
    s[64:68] = mk_crc(s)
    return s

def print_size(bytes_size):
    "Pretty print a bytes quantity"
    sizes = {0:'B', 10:'KiB',20:'MiB',30:'GiB',40:'TiB',50:'EiB'}
    k = 0
    for k in sorted(sizes):
        if (bytes_size / (1<<k)) < 1024: break
    return '%.02f %s' % (float(bytes_size)/(1<<k), sizes[k])

def print_progress(start_time, u_total, u_todo):
	"Prints a progress string"
	pct_done = 100*float(u_total-u_todo)/float(u_total)
	if (time.time() - print_progress.last_print) < 1: return
	print_progress.last_print = time.time()
	avg_secs_remaining = (print_progress.last_print - start_time) / pct_done * 100 - (print_progress.last_print - start_time)
	avg_secs_remaining = int(avg_secs_remaining)
	if avg_secs_remaining < 61:
		s = '%d secs' % avg_secs_remaining
	else:
		s = '%d:%02d mins' % (avg_secs_remaining/60, avg_secs_remaining%60)
	print_progress.fu('%d%% done, %s left          \r' % (pct_done, s))

print_progress.last_print = 0
if 'linux' in sys.platform:
	def fu(s):
		sys.stdout.write(s)
		sys.stdout.flush()
	print_progress.fu = fu
else:
	print_progress.fu = sys.stdout.write


def copy_sectors(src, dest, first, last):
    start_time = time.time()
    todo = (last-first+1)*512
    u_total = todo
    src.seek(first*512)
    n = (8<<20)
    while todo:
        dest.write(src.read((n, todo)[todo<n]))
        todo -= min(n, todo)
        print_progress(start_time, u_total, todo)


def save_disk(drive, sectors=0, flags=0):
    src = open(drive, 'rb')
    if lz4f and (flags&1):
        dst = lz4file('diskimage.bin', 'wb')
    else:
        dst = open('diskimage.bin', 'wb')
    if not sectors:
        sectors = get_size(drive) / 512
    print "Saving image of disk '%s', %d sectors (%s)" % (drive, sectors, print_size(sectors*512))
    copy_sectors(src, dst, 0, sectors-1)
    if flags&2:
        dst.write(mk_vhd_fixed_footer(sectors*512))
    src.close()
    dst.close()


def scan_gpt_table2(path, mode, flags=2):
    if os.name =='nt' and len(path)==2 and path[1] == ':':
        path = '\\\\.\\'+path
    d = disk.disk(path, mode)
    d.seek(512)
    blk = d.read(512)
    gpt = GPT(blk)
    d.seek(gpt.u64PartitionEntryLBA*512)
    blk = d.read(gpt.dwNumberOfPartitionEntries * gpt.dwNumberOfPartitionEntries)
    gpt.parse(blk)
    for i in range(gpt.dwNumberOfPartitionEntries):
        if not gpt.partitions[i].u64StartingLBA: break
        blocks = gpt.partitions[i].u64EndingLBA - gpt.partitions[i].u64StartingLBA + 1
        print "Found a %s GPT partition:\n- Name: %s\n- Type: %s\n- Offset: 0x%08X" % (print_size(blocks*512), gpt.partitions[i].name(), partition_uuids[gpt.partitions[i].gettype()], gpt.partitions[i].u64StartingLBA)
        # Bit 0: system partition; bit 63: no auto mount (MS)
        print "- Attributes: 0x%X" % gpt.partitions[i].u64Attributes
        print "Save it? [y=Yes q=QUIT] ",
        C = raw_input().lower()
        if C == 'q':
            sys.exit(1)
        if C == 'y':
            first = gpt.partitions[i].u64StartingLBA
            last = gpt.partitions[i].u64EndingLBA
            #~ src = disk.partition(d, first*512, blocks*512)
            #~ src.seek(0)
            if lz4f and (flags&1): # compress with lz4 if available
                dst = lz4file('Partition@%08Xh.bin'%first, 'wb')
            else:
                dst = open('Partition@%08Xh.%s'%(first, ('bin','vhd')[flags&2!=0]), 'wb')
            print "Saving partition of %d sectors (%s) at %X" % (last-first+1, print_size((last-first+1)*512), first)
            copy_sectors(d, dst, first, last)
            if flags&2: # append a fixed VHD footer
                dst.write(mk_vhd_fixed_footer((last-first+1)*512))
        print ""
    

if __name__ == '__main__':
    # Physical drive access requires *ALWAYS* Administrator rights in Windows Vista+!
    # Also, it requires root rights in Linux
    scan_gpt_table2('\\\\.\\PhysicalDrive0', 'rb')
    #~ scan_gpt_table('\\\\.\\PhysicalDrive0')
    #scan_gpt_table('/dev/sda')
    #~ save_disk('\\\\.\\PhysicalDrive2', flags=2)
