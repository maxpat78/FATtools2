# -*- coding: windows-1252 -*-
from random import *
import sys, os, hashlib, logging, optparse
import sqlite3

from Volume import opendisk


def GenRandStr(length, case=0):
 "Genera una stringa eufonica casuale, anche con maiuscole"
 Cons, dblCons = 'bcdfghklmnprstvz', 'bdfglmnpstz'
 Voc = 'aeiou'
 k, s = 0, ''
 for i in xrange(length):
  r = randint(0,100)
  if s and not k and r<15:
   c = choice(dblCons)*2
   i+=1; k=1
  elif not k and r<95:
   c, k = choice(Cons), 1
  else:
   c, k = choice(Voc), 0
  if case:
   if randint(0,1): c = c.upper()
  s += c
 if len(s) > length:
  s = s[:length]
 return s

def PwGenRand(length, num=3, case=0, alt=0, mix=0):
 """Genera una password eufonica casuale, con lettere (opzionalmente anche
maiuscole) e numeri (di regola: 3). Eventualmente, inserisce <alt> caratteri
speciali o mescola questi e i numeri nella stringa casuale."""
 pw = GenRandStr(length-num, case)
 for i in xrange(num):
  pw += str(randint(0,9))
 if alt:
  pw = list(pw)
  for i in xrange(alt):
   pw[ randint(0,len(pw)-1) ] = choice(',.-;:_<@#§[]*+?=()/&%$£"!|<')
  pw = ''.join(pw)
 if mix == 1:
  pw = list(pw)
  for i in xrange(randint(0,100)):
   j, k = randint(0,len(pw)-1), randint(0,len(pw)-1)
   c = pw[j]
   pw[j] = pw[k]
   pw[k] = c
  pw = ''.join(pw)
 elif mix == 2:
  pw = list(pw)
  for i in xrange(num):
   c = pw.pop(randint(-num, -1))
   num -= 1
   pw.insert(randint(0,len(pw)-1-num), c)
  pw = ''.join(pw)
 return pw



def stress(opts, args):
    "Randomly populates and erases a tree of random files and directories (for test purposes)"
    hashes = []
    hash_table = {}
    hash_table2 = {} # {hash_on_pathname: hash_on_contents}
    
    root = opendisk(args[0], 'r+b')
    
    free_clusters, free_bytes = root.getdiskspace()
    threshold = (root.boot.clusters() * root.boot.cluster) * (1.0 - opts.threshold/100.0)
    
    print "Starting: %d bytes in %d cluster(s) free" % (free_bytes, free_clusters)
    print "Threshold fixed to %d bytes" % threshold

    if opts.sha1:
        db = sqlite3.connect(':memory:')
        #~ db = sqlite3.connect('stress.db')
        db.execute("CREATE TABLE hashes(hash STRING, pathname STRING, pathname_hash STRING);")

    TOTDIRS, TOTFILES, TOTERASED = 0, 0, 0
    while free_bytes > threshold:
        BASE = 'DIRs/' # ROOT folder
        for nn in range(randint(2,9)):
            BASE += PwGenRand(randint(8,24), mix=3, case=1) + '/'
            TOTDIRS += 1
        subdir = root
        for elm in BASE.split('/')[:-1]:
            subdir = subdir.mkdir(elm)
        s = ''
        gens = []
        for i in range(randint(16,64)):
            s = PwGenRand(1024, case=1, mix=1)
            s *= randint(4,4096)
            fp = subdir.create(PwGenRand(randint(8,24), mix=3, case=1)+'.txt')
            gens += [fp.Entry.Name()]
            if opts.sha1:
                db.execute("INSERT INTO hashes VALUES(?,?,?);", (hashlib.sha1(s).hexdigest(),
                os.path.join(subdir.path, gens[-1]),
                hashlib.sha1(os.path.join(subdir.path, gens[-1])).hexdigest()))
                #~ db.commit()
            fp.write(s)
            fp.close()  # explicit close avoids corruption!
            TOTFILES += 1
            if i == 48:
             for i in range(randint(1,len(gens)/5)):
                 id = randint(0,len(gens)-1)
                 subdir.erase(gens[id])
                 TOTERASED+=1
                 if opts.sha1:
                    s = hashlib.sha1(os.path.join(subdir.path, gens[id])).hexdigest()
                    db.execute("DELETE FROM hashes WHERE pathname_hash='%s';"%s)
                 del gens[id]
        for i in range(randint(1,len(gens)/5)):
            id = randint(0,len(gens)-1)
            subdir.erase(gens[id])
            TOTERASED+=1
            if opts.sha1:
                s = hashlib.sha1(os.path.join(subdir.path, gens[id])).hexdigest()
                db.execute("DELETE FROM hashes WHERE pathname_hash='%s';"%s)
            del gens[id]
        free_clusters, free_bytes = root.getdiskspace()
        print "Running: %d bytes in %d cluster(s) free" % (free_bytes, free_clusters)
    free_clusters, free_bytes = root.getdiskspace()
    print "Ending: %d bytes in %d cluster(s) free" % (free_bytes, free_clusters)
    print "Generated %d files in %d dirs, erased %d" % (TOTFILES, TOTDIRS, TOTERASED)
    
    if opts.sha1:
        # Saves SHA-1 for all files, even erased ones!
        fp = root.create('hashes.sha1')
        res = db.execute('SELECT * FROM hashes;').fetchall()
        for h in res:
            fp.write(bytearray('%s *%s\n' % (h[0].encode('ascii'), h[1].encode('ascii'))))
        print "Saved SHA-1 hashes (using %d clusters more) for %d generated files" % (fp.File.size/root.boot.cluster, len(res))
        fp.close()



if __name__ == '__main__':
    help_s = """
    %prog [options] <drive>
    """
    par = optparse.OptionParser(usage=help_s, version="%prog 1.0", description="Stress a FAT/exFAT file system randomly creating, filling and erasing files.")
    par.add_option("-t", "--threshold", dest="threshold", help="limit the stress test to a given percent of the free space. Default: 99%", metavar="PERCENT", default=99, type="int")
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
