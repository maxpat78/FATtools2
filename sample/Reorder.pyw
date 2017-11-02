""" Reorder.pyw     V. 0.08

Visually alters a FAT/FAT32 directory table order. 

Very useful with micro Hi-Fi supporting USB keys but low of memory and bad
in browsing."""

import sys, os

if sys.version_info >= (3,0): 
    from tkinter import *
else:
    from Tkinter import *
    import tkMessageBox as messagebox

from Volume import *

DEBUG = 0

if DEBUG:
    import logging
    logging.basicConfig(level=logging.DEBUG, filename='reorder_gui.log', filemode='w')
    DEBUG = 2
    FAT.DEBUG = 4


class Manipulator(Tk):
    def __init__ (p):
        Tk.__init__(p)
        p.disk = ''
        p.title("Reorder a FAT/FAT32 directory table")
        p.geometry('640x510')
        frame = Frame(p, width=640, height=510)
        scroll = Scrollbar(frame, orient=VERTICAL)
        p.list = Listbox(frame, selectmode=EXTENDED, yscrollcommand=scroll.set, width=600, height=25)
        p.list.bind('<Double-Button-1>', p.on2click)
        scroll.config(command=p.list.yview)
        scroll.pack(side=RIGHT, fill=Y)
        p.list.pack()
        frame.pack()
        frame2 = Frame(frame)
        b = Button(frame2, text="UP", command=p.move_up)
        b.pack(side=LEFT)
        b = Button(frame2, text="DN", command=p.move_down)
        b.pack(side=LEFT)
        b = Button(frame2, text="T", command=p.move_top)
        b.pack(side=LEFT)
        b = Button(frame2, text="Scan", command=p.scan)
        b.pack(side=LEFT, padx=5)
        p.scan_button = b
        b = Button(frame2, text="Apply", command=p.apply)
        b.pack(side=LEFT, padx=5)
        b = Button(frame2, text="Quit", command=p.quit)
        b.pack(side=LEFT, padx=5)
        b = Button(frame2, text="Help", command=p.help)
        b.pack(side=LEFT, padx=40)
        b = Label(frame, text="Device/file to open: ")
        b.pack()
        p.drive_to_open = StringVar(frame, value='')
        b = Entry(frame, width=80, textvariable=p.drive_to_open)
        b.bind('<Return>', lambda x: p.scan())
        b.pack()
        p.tbox1 = b
        b = Label(frame, text="Path to reorder: ")
        b.pack()
        p.path_to_sort = StringVar(frame, value='.')
        b = Entry(frame, width=80, textvariable=p.path_to_sort)
        b.bind('<Return>', lambda x: p.scan())
        b.pack()
        p.tbox2 = b
        frame2.pack()

    def on2click(p, evt):
        w = evt.widget
        index = int(w.curselection()[0])
        value = w.get(index)
        if value == '.': return
        #~ if DEBUG:
            #~ messagebox.showinfo('Debug', 'You selected item %d: "%s"' % (index, value))
        r = p.tbox2.get()
        if value != '..':
            r += '\\'
            p.path_to_sort.set(os.path.join(r, value))
        else:
            p.path_to_sort.set(r[:r.rfind('\\')])
        p.scan_button.invoke() # but we don't know if it is a directory...

    def move_up(p):
        for i in p.list.curselection():
            it = p.list.get(i)
            p.list.delete(i)
            p.list.insert(i-1, it)
            p.list.selection_set(i-1)

    def move_down(p):
        for i in p.list.curselection():
            it = p.list.get(i)
            p.list.delete(i)
            p.list.insert(i+1, it)
            p.list.selection_set(i+1)
            
    def move_top(p):
        sel = p.list.curselection()
        for i, j in zip(range(len(sel)), sel):
            it = p.list.get(j)
            p.list.delete(j)
            p.list.insert(i, it)
            
    def scan(p):
        root_object = p.tbox1.get()
        if not root_object: return
        if root_object[-1] == os.sep:
            root_object = root_object[:-1]
        if p.disk != root_object.lower(): # opening the same drive multiple times is actually unsupported!
            if DEBUG:
                print "DEBUG: volume to open", root_object
            p.disk = root_object.lower()
            try:
                p.root = opendisk(p.disk, 'r+b')
            except:
                messagebox.showerror('Error', 'Could not open volume "%s"!' % (root_object))
                return
        relapath = p.path_to_sort.get()
        if DEBUG:
            print "DEBUG: internal path to access:", relapath
        if relapath == '.':
            fold = p.root
        else:
            fold = p.root.opendir(relapath[2:]) # truncates .\
        if not fold:
            messagebox.showerror('Error', "\"%s\" is not a directory!" % (relapath))
            p.path_to_sort.set(relapath[:relapath.rfind('\\')])
            return
        p.fold = fold
        if DEBUG:
            print "DEBUG: scanning", fold.path
        p.list.delete(0, END)
        for it in p.fold.iterator():
            p.list.insert(END, it.Name())

    def apply(p):
        li = p.list.get(0, END)
        if not li:
            return
        if DEBUG:
            print "DEBUG: captured order:", li
        p.fold._sortby.fix = li
        p.fold.sort(p.fold._sortby)

    def quit(p):
        root.destroy()

    def help(p):
        messagebox.showinfo('Quick Help', '''To edit a directory table order in a FAT/FAT32 disk:
        
- specify the device or image file containing the filing system in the first text box below
- specify the path to edit in the second box (default is root, '.')
- press 'Enter' or select 'Scan' to (re)scan the directory table specified and show the on-disk order in the upper list box
- select one or more items and move them up, down or to the top with the 'UP', 'DN' and 'T' buttons (double click to enter a directory)
- press 'Apply' to write the newly ordered table back to the disk
- use 'Quit' when done''')

root = Manipulator()
root.mainloop()
