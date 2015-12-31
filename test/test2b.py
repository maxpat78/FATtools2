# -*- coding: mbcs -*-
import sys

#~ import logging
#~ logging.basicConfig(level=logging.DEBUG, filename='PYTEST.LOG', filemode='w')

from FAT import *

root = opendisk('G:', 'r+b')
#~ root = opendisk('G.IMA', 'r+b')

base = r'C:\Python27'
fat_copy_tree_inject(base, root.mkdir('Python27'), lambda x:sys.stdout.write("Copying %s\n"% x), True)
