print "Stringifying NODOS code (to put at offset 5Ah)"
s = open('nodos','rb').read()
L = ['\\x%02X' % ord(c) for c in s]
print ''.join(L)
