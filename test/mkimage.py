fssize = 256<<20 # MB
#~ fssize = 5<<30 # GB

# Create a blank image file, quickly allocating slack space
f = open('G.IMA', 'wb')
f.seek(fssize)
f.truncate()
f.close()
