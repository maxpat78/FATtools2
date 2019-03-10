# -*- coding: cp1252 -*-
import sys, glob

import logging
logging.basicConfig(level=logging.DEBUG, filename='test_extract.log', filemode='w')

import Volume
Volume.DEBUG = 255
Volume.FAT.DEBUG = 255

from Volume import *

def printn(s):
 print s
 
for ima in glob.glob('C:\\OLDDOS\\*.img'):
 print 'Extracting from', ima
 root = opendisk(ima, 'rb')
 copy_tree_out(root, 'OLDDOS', printn, 2)
