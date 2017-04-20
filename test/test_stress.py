# -*- coding: windows-1252 -*-
from random import *
import sys, os

import logging
logging.basicConfig(level=logging.DEBUG, filename='PYTEST.LOG', filemode='w')

#~ from exFAT import *
from FAT import *

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


if __name__ == '__main__':
    "Randomly populates and erases a tree of random files and directories (for test purposes)"
    #~ DEBUG_EXFAT=1
    root = opendisk('G:', 'r+b')
    #~ root = opendisk('G.ima', 'r+b')
    
    free_clusters, free_bytes = root.getdiskspace()
    threshold = root.boot.clusters() * root.boot.wBytesPerSector * root.boot.uchSectorsPerCluster / 100
    
    print "Starting: %d bytes in %d cluster(s) free" % (free_bytes, free_clusters)
    
    TOTDIRS, TOTFILES, TOTBYTES = 0, 0, 0
    #~ for i in range(randint(3,16)):
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
        for i in range(64):
            s = PwGenRand(1024, case=1, mix=1)
            s *= randint(64,1536)
            fp = subdir.create(PwGenRand(randint(8,24), mix=3, case=1)+'.txt')
            gens += [fp.Entry.Name()]
            fp.write(s)
            fp.close()  # explicit close avoids corruption!
            TOTFILES += 1
            TOTBYTES += len(s)
            free_bytes -= len(s)
            if i == 48:
             for i in range(16):
                 id = randint(0,len(gens)-1)
                 subdir.erase(gens[id])
                 del gens[id]
        for i in range(24):
            id = randint(0,len(gens)-1)
            subdir.erase(gens[id])
            del gens[id]
        free_clusters, free_bytes = root.getdiskspace()
        print "Running: %d bytes in %d cluster(s) free" % (free_bytes, free_clusters)
