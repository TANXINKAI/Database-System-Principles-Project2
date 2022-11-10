import tkinter as tk
from tkinter import TclError, ttk, Frame, SUNKEN, END, Tk, Label, Text, Button
from interface_lv import Interface_ListView
import preprocessing, annotation
import graphviz
from PIL import ImageTk, Image

class MyWindow:
    def __init__(self, win, config):
        self.config = config
        self.preloaded_query_frame = Frame(master = win)
        self.query_frame = Frame(master = win)
        self.tree_frame = Frame(master = win, relief = SUNKEN, borderwidth = 5)
        
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

        self.tree_frame.columnconfigure(0, weight=1)
        self.tree_frame.rowconfigure(2, weight=1)
        self.img_handle = None
        self.james = None
        self.tbAnnotation = Text(self.tree_frame,height=20, bg="lightgrey")
        self.tbAnnotation.insert(tk.END,"Annotations will be loaded here after query is submitted")
        self.tbAnnotation.grid(column=0,row=3,sticky=tk.EW)
        
    def preload_query(self, event):
        for selected_item in self.listview.lv.selection():
            item = self.listview.lv.item(selected_item)
            if not item['text'] in self.listview.parent_map.keys():
                self.tbQuery.delete("0.0",tk.END)
                text = " ".join([str(d) for d in item['tags']])
                self.tbQuery.insert(tk.END,text)

    def command_submit_query(self):
        query=(self.tbQuery.get("0.0", tk.END)).strip()
        query = query.replace("date ':1'","date '1/1/1970'")
        query = query.replace("date ':2'","date '1/1/1970'")
        query = query.replace("date ':3'","date '1/1/1970'")
        query = query.replace("date ':4'","date '1/1/1970'")
        query = query.replace(":","")
        self.visualiseTree(query)
        #pass query to preprocessing
    
    def visualiseTree(self,query):
        annotate_dict = {}
        qm = preprocessing.Query_Manager(self.config["Database_Credentials"])
        optimal_qep_tree = qm.get_query_tree(qm.get_query_plan(query))
        g = graphviz.Digraph('G')
        g.edge_attr['dir'] = 'back'
        first = True
        queue = [optimal_qep_tree.head]
        node_count = 0
        t_queue = [optimal_qep_tree.head]
        while len(t_queue) > 0:
            curNode = t_queue.pop(0)
            node_count += 1
            if curNode.left:
                t_queue.append(curNode.left)
            if curNode.right:
                t_queue.append(curNode.right)
                
        id = node_count
        def get_node_text(node):
            text = f"[{str(node.id)}] " + node.get_aqps(self.config,query)["Optimal"]["Node Type"] + " "
            if len(node.query_clause) > 0:
                text = text + f"\n{node.query_clause[0]}"
            return text

        while len(queue) > 0:
            curNode = queue.pop(0)
            if curNode.id == -1:
                curNode.id = id
                id-=1
            if first:
                g.edge("Completed Query", get_node_text(curNode))
                annotate_dict[get_node_text(curNode)] = curNode.get_aqps(self.config,query)
                first = False
            if curNode.left:
                curNode.left.id = id
                id-=1
                g.edge(get_node_text(curNode), get_node_text(curNode.left))
                annotate_dict[get_node_text(curNode.left)] = curNode.left.get_aqps(self.config,query)
                queue.append(curNode.left)
            if curNode.right:
                curNode.right.id = id
                id-=1
                g.edge(get_node_text(curNode), get_node_text(curNode.right))
                annotate_dict[get_node_text(curNode.right)] = curNode.right.get_aqps(self.config,query)
                queue.append(curNode.right)

        g.render("tmp",format="jpg", view=False)
        self.tree_frame.update()
        if self.img_handle:
            self.img_handle.close()
            self.img_handle = None
        self.img_handle = Image.open("./tmp.jpg")
        resize_width = self.img_handle.width
        resize_height = self.img_handle.height
        
        if self.img_handle.height > self.tree_frame.winfo_height() - 320 and self.tree_frame.winfo_height() - 320 > 0:
            resize_height = self.tree_frame.winfo_height() - 320
        if self.img_handle.width > self.tree_frame.winfo_width():
            resize_width = self.tree_frame.winfo_width()
        if resize_width != self.img_handle.width or resize_height != self.img_handle.height:
            img = ImageTk.PhotoImage(self.img_handle.resize((resize_width, resize_height), Image.ANTIALIAS))
        else:
            img = ImageTk.PhotoImage(self.img_handle)
        if self.james:
            self.james.image = ""
            self.james.photo = ""
            self.james.destroy()
        self.james = Label(self.tree_frame, image=img)
        self.james.photo = img
        self.james.grid(column=0,row=1)
        self.james.bind("<Button-1>", lambda e:self.img_handle.show())
        self.tree_frame.update()

        self.tbAnnotation.delete("0.0",tk.END)
        text = "Click on image for full size. Scroll down for more annotations (if available).\n" + annotation.get_annotations(node_count,annotate_dict)
        self.tbAnnotation.insert(tk.END,text)


