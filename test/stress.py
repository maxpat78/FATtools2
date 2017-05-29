# -*- coding: windows-1252 -*-
from random import *
import sys, os, hashlib, logging, optparse

from Volume import opendisk


class Stress(BaseException):
    pass

def GenObjName(obj='file'):
    if obj == 'file':
        model = 'File Name %%0%dd.txt' % randint(1,32)
    elif obj == 'dir':
        model = 'Directory %%0%dd' % randint(1,10)
    else:
        raise Stress("Bad object type specified! You must use 'dir' or 'file'.")
    GenObjName.Index += 1
    return model % GenObjName.Index
GenObjName.Index = 0

def GenRandTree():
    tree = []
    for i in range(randint(1,8)):
        L = []
        for i in range(randint(2,8)):
            L += [GenObjName('dir')]
        tree += ['\\'.join(L)]
    return tree

def GetRandSubpath(tree):
    p = choice(tree)
    L = p.split('\\')
    q = choice(L)
    return '\\'.join(L[:L.index(q)]) or L[0]


class RandFile(object):
    Buffer = None
    
    def __init__(self, root, path, name, maxsize, hash=0):
        if not RandFile.Buffer:
            RandFile.Buffer = bytearray(128<<10)
            j=0
            for i in range(256):
                RandFile.Buffer[j:j+512] = 512*chr(i)
                j+=512
            RandFile.Buffer*=64 # 8M buf
        self.root = root
        self.path = path
        self.name = name
        # Random file size
        self.size = randint(1, maxsize)

        if hash:
            self.sha1 = hashlib.sha1(RandFile.Buffer[:self.size]).hexdigest()
        
        # Random segments to write
        ops = randint(1,32)
        maxc = self.size/ops
        self.indexes = [0]
        for i in range(ops):
            # Indexes MUST be unique!
            j=0
            while j in self.indexes:
                j = randint(self.indexes[-1], self.indexes[-1]+maxc)
            self.indexes += [j]
        
        self.i = 0 # next segment to write
        self.IsWritten = 0
    
    def create(self):
        self.dirtable = self.root.opendir(self.path)
        self.fp = self.dirtable.create(self.name)
        
    def write(self):
        if self.IsWritten: return 0
        try:
            j = self.indexes[self.indexes.index(self.i)+1]
            self.fp.write(RandFile.Buffer[self.i:j])
            self.i = j
        except IndexError:
            self.fp.write(RandFile.Buffer[self.i:self.size])
            self.IsWritten = 1
        return 1



def stress(opts, args):
    "Randomly populates and erases a tree of random files and directories (for test purposes)"
    root = opendisk(args[0], 'r+b')
    
    dirs_made, files_created, files_erased = 0,0,0
    
    tree = GenRandTree()
    for pattern in tree:
        obj = root
        for subdir in pattern.split('\\'):
            if len(os.path.join(obj.path,subdir))+2 > 260: continue # prevents invalid pathnames
            obj = obj.mkdir(subdir)
            dirs_made+=1

    print "Random tree of %d directories generated" % dirs_made

    free_bytes = root.getdiskspace()[1]
    threshold = (root.boot.clusters() * root.boot.cluster) * (1.0-opts.threshold/100.0)
    files_set = []

    def rand_populate(root, files_set, tree, free_bytes):
        while 1:
            fpath = GetRandSubpath(tree)
            fname = GenObjName()
            if len(os.path.join(fpath,fname))+2 > 260: continue # prevents invalid pathnames
            o = RandFile(root, fpath, fname, min(opts.file_size, free_bytes), opts.sha1)
            if (free_bytes-o.size) < threshold: break
            files_set += [o]
            free_bytes -= o.size
        return free_bytes

    def rand_erase(files_set):
        n = randint(1, len(files_set)/2)
        cb = 0
        for i in range(n):
            f = choice(files_set)
            f.fp.close()
            f.dirtable.erase(f.name)
            cb += f.size
            del files_set[files_set.index(f)]
        return n, cb

    def rand_truncate(files_set):
        n = randint(1, len(files_set)/2)
        for i in range(n):
            f = choice(files_set)
            j = randint(f.fp.File.size/6, f.fp.File.size/2)
            f.fp.ftruncate(j, 1)
            if hasattr(f, 'sha1'):
                f.sha1 = hashlib.sha1(RandFile.Buffer[:j]).hexdigest()
        return n

    cb = rand_populate(root, files_set, tree, free_bytes)
    print "Step 1"
    print "Generated %d random files for %d bytes" % (len(files_set), free_bytes-cb)

    print "Creating their handles..."
    map(lambda x: x.create(), files_set)

    print "Randomly writing their contents..."
    while 1:
        L = map(lambda x: x.write(), files_set)
        if 1 not in L: break

    print "Step 2"
    print "Randomly erasing some files..."
    n, cb = rand_erase(files_set)
    print "Erased %d bytes in %d files" % (cb, n)

    print "Step 3"
    print "Randomly truncating some files..."
    n = rand_truncate(files_set)
    print "Truncated %d files" % (n)

    free_bytes = root.getdiskspace()[1]
    fset = []
    cb = rand_populate(root, fset, tree, free_bytes)
    files_set += fset
    print "Step 4"
    print "Generated other %d random files for %d bytes" % (len(fset), free_bytes-cb)

    print "Creating their handles..."
    map(lambda x: x.create(), fset)

    print "Randomly writing their contents..."
    while 1:
        L = map(lambda x: x.write(), fset)
        if 1 not in L: break
    
    print "Done."

    if opts.sha1:
        # Saves SHA-1 for all files, even erased ones!
        fp = root.create('hashes.sha1')
        cb=0
        for h in files_set:
            a = os.path.join(h.path, h.name)
            # D:\some 256-character path string<NUL>
            if len(a)+4 > 260:
                cb+=1
            fp.write(bytearray('%s *%s\n' % (h.sha1.encode('ascii'), a.encode('ascii'))))
        print "Saved SHA-1 hashes (using %d clusters more) for %d generated files" % (fp.File.size/root.boot.cluster, len(files_set))
        fp.close()
        if cb:
            print "WARNING: %d files have pathnames >260 chars!" % cb



if __name__ == '__main__':
    help_s = """
    %prog [options] <drive>
    """
    par = optparse.OptionParser(usage=help_s, version="%prog 1.0", description="Stress a FAT/exFAT file system randomly creating, filling and erasing files.")
    par.add_option("-t", "--threshold", dest="threshold", help="limit the stress test to a given percent of the free space. Default: 99%", metavar="PERCENT", default=99, type="float")
    par.add_option("-s", "--filesize", dest="file_size", help="set the maximum size of a random generated file. Default: 1M", metavar="FILESIZE", default=1<<20, type="int")
    par.add_option("--debug", action="store_true", dest="debug", help="turn on debug logging to stress.log (may be VERY slow!). Default: OFF", metavar="DEBUG_LOG", default=False)
    par.add_option("--sha1", action="store_true", dest="sha1", help="turn on generating an hash list of generated files. Default: OFF", metavar="HASH_LOG", default=False)
    par.add_option("--fix", action="store_true", dest="fix", help="use a fixed random seed. Default: OFF", metavar="FIX_RAND", default=False)
    opts, args = par.parse_args()

    if not args:
        print "You must specify a drive to test!\n"
        par.print_help()
        sys.exit(1)

    if opts.debug:
        logging.basicConfig(level=logging.DEBUG, filename='stress.log', filemode='w')

    if opts.fix:
        seed(78) # so it repeates the same "random" sequences at every call

    stress(opts, args)
