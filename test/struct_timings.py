# cProfile x
import struct

# Struct packing of an integer is about 3x slower than simple assignment
def fu(x):
    #~ return x #~1.8"
    return struct.pack('<I', x) #~ about 5.8"
a=0
for i in xrange(1000000):
    a = fu(i)
print a
