import tkinter as tk
from tkinter import TclError, ttk, Frame, SUNKEN, END, Tk, Label, Text, Button
from turtle import back
from interface_lv import Interface_ListView


class MyWindow:
    def __init__(self, win):
        self.preloaded_query_frame = Frame(master = win)
        self.query_frame = Frame(master = win)
        self.tree_frame = Frame(master = win,background="orange", relief = SUNKEN, borderwidth = 5)
        annotation_frame = Frame(master = win, relief = SUNKEN, borderwidth = 5)
        
        win.rowconfigure(1, weight=2)
        win.columnconfigure(2, weight=1)
        win.rowconfigure(2, weight=0,uniform=1)
        self.preloaded_query_frame.grid(column=1, row=1, sticky=tk.NSEW)
        self.query_frame.grid(column=1,row=2,sticky=tk.NSEW)
        self.tree_frame.grid(column=2, row=1, rowspan=2, sticky=tk.NSEW)

        self.query_frame.columnconfigure(0, weight=1)
        self.query_frame.rowconfigure(0, weight=1)
        self.lblQuery = Label(self.query_frame, text="Query")
        self.tbQuery = Text(self.query_frame,height=10)
        self.query_frame.columnconfigure(0, weight=1)
        self.query_frame.rowconfigure(0, weight=1)
        self.lblQuery.grid(column=1,row=1,sticky=tk.E)
        self.tbQuery.grid(column=2,row=1,sticky=tk.W)

        btnOffsetFrame = Frame(master=self.query_frame)
        btnOffsetFrame.columnconfigure(0,weight=1)
        btnOffsetFrame.rowconfigure(0,weight=1)
        btnSubmitQuery = Button(master=btnOffsetFrame, text="Submit Query", command=self.command_submit_query)
        btnSubmitQuery.grid(column=2,row=1)
        btnOffsetFrame.grid(column=2,row=2,sticky=tk.NSEW, padx=5, pady=5)
        self.listview = Interface_ListView(self.preloaded_query_frame, self.preload_query)
        self.listview.seed_default()

        self.listview_scrollbar = ttk.Scrollbar(self.preloaded_query_frame, orient=tk.VERTICAL, command=self.listview.lv.yview)
        self.listview.lv.configure(yscroll=self.listview_scrollbar.set)
        self.listview_scrollbar.grid(row=1, column=2, sticky='ns')
        
    def preload_query(self, event):
        for selected_item in self.listview.lv.selection():
            item = self.listview.lv.item(selected_item)
            if not item['text'] in self.listview.parent_map.keys():
                self.tbQuery.delete("0.0",tk.END)
                self.tbQuery.insert(tk.END,item['tags'])

    def command_submit_query(self):
        query=(self.tbQuery.get("0.0", tk.END)).strip()
        #pass query to preprocessing
        pass
    
    def visualiseTree(self):
        pass
    

window=Tk()
width= window.winfo_screenwidth() / 2
height= window.winfo_screenheight() / 2
window.geometry("%dx%d" % (width, height))
mywin=MyWindow(window)
window.title('Query Plan Processing')

window.mainloop()


