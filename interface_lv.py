import tkinter as tk
from tkinter import ttk, Frame, SUNKEN, Tk
from tkinter.messagebox import showinfo

class Interface_ListView:
    def __init__(self, master, item_select_callback = None):
        columns = ["Pre-loaded Queries"]
        self.master = master
        self.lv = ttk.Treeview(master, columns=columns)
        self.iid = 0
        self.parent_map = {}
        self.parent_content_map = {}

        if item_select_callback:
            self.item_selected = item_select_callback
        self.lv.bind('<<TreeviewSelect>>', self.item_selected)

        self.build_columns(columns)
        self.master.rowconfigure(1,weight=1)
        self.master.columnconfigure(1,weight=1)
        self.lv.grid(row=1, column=1, sticky=tk.NSEW)

            
    def build_columns(self,columns):
        for i in range(0,len(columns)):
            self.lv.heading(f'#{str(i)}',text=columns[i], anchor=tk.W)
        self.lv.column("#0", minwidth=400) 

    def item_selected(self, event):
        for selected_item in self.lv.selection():
            item = self.lv.item(selected_item)
            if not item['text'] in self.parent_map.keys():
                showinfo(title='Information', message=item['tags'])

    def append(self, parent, content, tag):
        if not content:
            return
        
        if not parent in self.parent_map.keys():
            self.parent_map[parent] = self.iid
            self.lv.insert(parent='', index='end',text=parent, iid=self.iid, open=False)
            self.iid += 1

        if not parent in self.parent_content_map.keys():
            self.parent_content_map[parent] = 0
        
        self.lv.insert(parent='', index='end',text=content, iid=self.iid, open=False, tags=tag)
        self.lv.move(self.iid, self.parent_map[parent], self.parent_content_map[parent])
        self.parent_content_map[parent] = self.parent_content_map[parent] + 1 
        self.iid += 1

    def seed(self,data):
        if not data:
            return
        if self.lv:
            for i in data:
                self.append(i["table"], i["description"],i["sql"])

class Interface_ListView_Schema:
    def __init__(self, master, item_select_callback = None):
        columns = ["Schema Name"]
        self.master = master
        self.lv = ttk.Treeview(master, columns=columns, height=50)
        self.iid = 0
        self.selected_schema = None

        if item_select_callback:
            self.item_selected = item_select_callback
        self.lv.bind('<<TreeviewSelect>>', self.item_selected)

        self.build_columns(columns)
        self.master.rowconfigure(1,weight=1)
        self.master.columnconfigure(1,weight=1)
        self.lv.grid(row=1, column=1, sticky=tk.NSEW)

            
    def build_columns(self,columns):
        for i in range(0,len(columns)):
            self.lv.heading(f'#{str(i)}',text=columns[i], anchor=tk.W)
        self.lv.column("#0", minwidth=400) 

    def item_selected(self, event):
        for selected_item in self.lv.selection():
            item = self.lv.item(selected_item)
            print(item['text'])

    def append(self, content):
        if not content:
            return

        self.lv.insert(parent='', index='end',text=content, iid=self.iid, open=False)
        self.iid += 1

    def seed(self, data):
        if not data:
            return

        if self.lv:
            for i in data:
                self.append(i)