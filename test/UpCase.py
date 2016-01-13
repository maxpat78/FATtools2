import struct
from mkexfatnew import gen_upcase

def gen_upcase_compressed():
    tab = []
    run = -1
    for i in xrange(65536):
        u = unichr(i)
        U = unichr(i).upper()
        if u != U:
            print "Unequal mapping at %04X" % i
            if run > -1 and (i-run) > 2:
                print "Setting range of %04X chars from %04X to %04X" % (i-run,run,i)
                # Replace chars with range
                del tab[len(tab)-(i-run):]
                tab += [unichr(0xFFFF), unichr(i-run)]
            run = -1
        else:
            if run < 0:
                run = i
                print "Equal mapping at %04X" % i
        tab += [U]

    return u''.join(tab).encode('utf-16le')


def upcase_expand(s):
    i = 0
    expanded_i = 0
    tab = []
    print "Processing compressed table of %d bytes" % len(s)
    while i < len(s):
        word = struct.unpack('<H', s[i:i+2])[0]
        if word == 0xFFFF and i+2 < len(s):
            print "Found compressed run at 0x%X (%04X)" % (i, expanded_i)
            word = struct.unpack('<H', s[i+2:i+4])[0]
            print "Expanding range of %04X chars from %04X to %04X" % (word, expanded_i, expanded_i+word)
            for j in xrange(expanded_i, expanded_i+word):
                tab += [struct.pack('<H', j)]
            i += 4
            expanded_i += word
        else:
            print "Decoded uncompressed char at 0x%X (%04X)" % (i, expanded_i)
            tab += [s[i:i+2]]
            i += 2
            expanded_i += 1
    return bytearray().join(tab)



if __name__ == '__main__':
    #~ open('upcase_expanded.bin','wb').write(gen_upcase())
    #~ open('upcase_compressed.bin','wb').write(gen_upcase_compressed())
    #~ s = open('upcase.bin','rb').read()
    #~ open('upcaseMS-exp.bin', 'wb').write(upcase_expand(s))
    b = gen_upcase()
    open('test_upcase_base.bin', 'wb').write(b)

    a = gen_upcase_compressed()
    open('test_upcase_compressed.bin', 'wb').write(a)

    print "**************************************************************************"

    c = upcase_expand(a)
    open('test_upcase_deflated.bin', 'wb').write(c)
    assert c == b
