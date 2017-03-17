""" Reorder.pyw

Visually alters a FAT/FAT32 directory table order. 

Very useful with micro Hi-Fi supporting USB keys but low of memory and bad
in browsing. """

import sys, os

if sys.version_info >= (3,0): 
    from tkinter import *
else:
    from Tkinter import *
    import tkMessageBox as messagebox

from FAT import *



class Manipulator(Tk):
    def __init__ (p):
        Tk.__init__(p)
        p.title("Reorder a FAT/FAT32 directory table")
        p.geometry('640x500')
        frame = Frame(p, width=640, height=500)
        scroll = Scrollbar(frame, orient=VERTICAL)
        p.list = Listbox(frame, selectmode=EXTENDED, yscrollcommand=scroll.set, width=600, height=25)
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
        b = Button(frame2, text="Apply", command=p.apply)
        b.pack(side=LEFT, padx=5)
        b = Button(frame2, text="Quit", command=p.quit)
        b.pack(side=LEFT, padx=5)
        b = Button(frame2, text="Help", command=p.help)
        b.pack(side=LEFT, padx=40)
        b = Label(frame, text="Drive/Folder to reorder: ")
        b.pack()
        b = Entry(frame, width=80)
        b.pack()
        p.tbox = b
        frame2.pack()

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
        p.list.delete(0, END)
        drive, folder = os.path.split(p.tbox.get())
        # Mmmmm... what behavior when the same disk is opened multiple times?
        try:
            p.root = opendisk(drive, 'r+b')
            if folder:
                p.root = p.root.opendir(folder)
        except:
            messagebox.showerror('Error', 'Could not open the requested directory table!')
            return
        for it in p.root.iterator():
            p.list.insert(END, it.Name())

    def apply(p):
        li = p.list.get(0, END)
        if not li:
            return
        p.root._sortby.fix = li
        p.root.sort(p.root._sortby)

    def quit(p):
        root.destroy()

    def help(p):
        messagebox.showinfo('Quick Help', '''To edit a directory table order in a FAT/FAT32 disk:
        
- insert the drive or the full directory path in the text box below
- select 'Scan' to (re)scan the directory table specified and show the on-disk order in the upper list box
- select one or more items and move them up, down or to the top with the 'UP', 'DN' and 'T' buttons
- press 'Apply' to write the newly ordered table back to the disk
- use 'Quit' when done''')

root = Manipulator()
root.mainloop()
