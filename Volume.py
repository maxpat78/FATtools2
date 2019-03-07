import os, time
import disk, utils, FAT, exFAT

DEBUG = 0

from debug import log

def rdiv(a, b):
    "Divide a by b eventually rounding up"
    if a % b:
        return a/b + 1
    else:
        return a/b

def opendisk (path, mode='rb'):
    "Opens a FAT/exFAT filesystem returning the root directory Dirtable"
    if os.name =='nt' and len(path)==2 and path[1] == ':':
        path = '\\\\.\\'+path
    d = disk.disk(path, mode)
    bs = d.read(512)
    d.seek(0)
    fstyp = utils.FSguess(FAT.boot_fat16(bs)) # warning: if we call this a second time on the same Win32 disk, handle is unique and seek set already!
    if DEBUG&2: log("FSguess guessed FS type: %s", fstyp)

    if fstyp in ('FAT12', 'FAT16'):
        boot = FAT.boot_fat16(bs, stream=d)
    elif fstyp == 'FAT32':
        boot = FAT.boot_fat32(bs, stream=d)
    elif fstyp == 'EXFAT':
        boot = exFAT.boot_exfat(bs, stream=d)
    elif fstyp == 'NTFS':
        print "NTFS file system not supported. Aborted."
        sys.exit(1)
    else:
        print "File system not recognized. Aborted."
        sys.exit(1)

    fat = FAT.FAT(d, boot.fatoffs, boot.clusters(), bitsize={'FAT12':12,'FAT16':16,'FAT32':32,'EXFAT':32}[fstyp], exfat=(fstyp=='EXFAT'))

    if DEBUG&2:
        log("Inited BOOT object: %s", boot)
        log("Inited FAT object: %s", fat)

    if fstyp == 'EXFAT':
        mod = exFAT
    else:
        mod = FAT
        
    root = mod.Dirtable(boot, fat, boot.dwRootCluster)
    
    if fstyp == 'EXFAT':
        for e in root.iterator():
            if e.type == 1: # Find & open Bitmap
                boot.bitmap = exFAT.Bitmap(boot, fat, e.dwStartCluster, e.u64DataLength)
                break

    return root


def copy_tree_in(base, dest, callback=None, attributes=None, chunk_size=1<<20):
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
                    if target_dir.fat.exfat:
                        dw, ms = exFAT.exFATDirentry.MakeDosDateTimeEx((tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec))
                        dst.Entry.dwCTime = dw
                        dst.chmsCTime = ms
                    else:
                        dst.Entry.wCDate = FAT.FATDirentry.MakeDosDate((tm.tm_year, tm.tm_mon, tm.tm_mday))
                        dst.Entry.wCTime = FAT.FATDirentry.MakeDosTime((tm.tm_hour, tm.tm_min, tm.tm_sec))


                if attributes & 2:
                    tm = time.localtime(st.st_mtime)
                    if target_dir.fat.exfat:
                        dw, ms = exFAT.exFATDirentry.MakeDosDateTimeEx((tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec))
                        dst.Entry.dwMTime = dw
                        dst.chmsCTime = ms
                    else:
                        dst.Entry.wMDate = FAT.FATDirentry.MakeDosDate((tm.tm_year, tm.tm_mon, tm.tm_mday))
                        dst.Entry.wMTime = FAT.FATDirentry.MakeDosTime((tm.tm_hour, tm.tm_min, tm.tm_sec))


                if attributes & 4:
                    tm = time.localtime(st.st_atime)
                    if target_dir.fat.exfat:
                        dw, ms = exFAT.exFATDirentry.MakeDosDateTimeEx((tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec))
                        dst.Entry.dwATime = dw
                        dst.chmsCTime = ms
                    else:
                        dst.Entry.wADate = FAT.FATDirentry.MakeDosDate((tm.tm_year, tm.tm_mon, tm.tm_mday))
                        dst.Entry.wATime = FAT.FATDirentry.MakeDosTime((tm.tm_hour, tm.tm_min, tm.tm_sec))
            dst.close()



def copy_tree_out(base, dest, callback=None, attributes=None, chunk_size=1<<20):
    """Copy recursively files and directories under virtual 'base' Dirtable into
    real 'dest' directory, 'chunk_size' bytes at a time, calling callback function if provided
    and preserving date and times if desired."""
    for root, folders, files in base.walk():
        for file in files:
            src = os.path.join(root, file)
            dst = os.path.join(dest, src[len(base.path)+1:])
            if base.path == os.path.dirname(src):
                fpi = base.open(file)
            else:
                fpi = base.opendir(os.path.dirname(src)[len(base.path)+1:]).open(file)
            assert fpi.IsValid != False
            try:
                os.makedirs(os.path.dirname(dst))
            except:
                pass
            fpo = open(dst, 'wb')
            if callback: callback(src)
            while True:
                s = fpi.read(chunk_size)
                if not s: break
                fpo.write(s)
            fpo.close()
            fpi.close() # If closing is deferred to atexit, massive KeyError exceptions are generated by disk.py in cache_flush: investigate!

            if attributes: # bit mask: 1=preserve creation time, 2=last modification, 3=last access
                if attributes & 1:
                    CTime = fpi.Entry.ParseDosDate(fpi.Entry.wCDate) + fpi.Entry.ParseDosTime(fpi.Entry.wCTime) + (0,0,0)
                    pass # utime does not support this
                if attributes & 2:
                    MTime = fpi.Entry.ParseDosDate(fpi.Entry.wMDate) + fpi.Entry.ParseDosTime(fpi.Entry.wMTime) + (0,0,0)
                    os.utime(dst, (0, time.mktime(MTime)))
                if attributes & 4:
                    ATime = fpi.Entry.ParseDosDate(fpi.Entry.wADate) + fpi.Entry.ParseDosTime(fpi.Entry.wATime) + (0,0,0)
                    os.utime(dst, (time.mktime(ATime), 0))
    