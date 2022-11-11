import tkinter as tk
from tkinter import ttk, Frame, SUNKEN, Label, Text, Button
from interface_lv import Interface_ListView
import preprocessing, annotation
import graphviz
from PIL import ImageTk, Image
import re

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
        self.imgGraph = None
        self.tbAnnotation = Text(self.tree_frame,height=20, bg="lightgrey")
        self.tbAnnotation.insert(tk.END,"Annotations will be loaded here after query is submitted")
        self.tbAnnotation.tag_add("instructions", "1.0", "2.0")
        self.tbAnnotation.tag_config("instructions", foreground='red')
        self.tbAnnotation.grid(column=0,row=3,sticky=tk.EW)
        
    def preload_query(self, event):
        """
        Event handler for selecting an item in the listView
        Loads the selected query from the listView into the query textbox
        """
        for selected_item in self.listview.lv.selection():
            item = self.listview.lv.item(selected_item)
            if not item['text'] in self.listview.parent_map.keys():
                self.tbQuery.delete("0.0",tk.END)
                text = " ".join([str(d) for d in item['tags']])
                self.tbQuery.insert(tk.END,text)

    def command_submit_query(self):
        """
        Event handler for "Submit Query" button click
        """
        query=(self.tbQuery.get("0.0", tk.END)).strip()

        # This bunch of replace is just to 'fix' the SQL parameters for TPC-H queries.
        # Replacing them with dummy values or omitting them
        query = query.replace("date ':1'","date '1/1/1970'") 
        query = query.replace("date ':2'","date '1/1/1970'")
        query = query.replace("date ':3'","date '1/1/1970'")
        query = query.replace("date ':4'","date '1/1/1970'")
        query = query.replace("day (3)","")
        query = query.replace(":","")

        #regex = r'(create view (.*?) as) (.*) (drop view (.*?);)'
        #matches = re.match(regex, query)
        #if matches and len(matches.groups())>0:
        #    query= matches.groups()[2]

        if "create view" in query:
            tk.messagebox.showerror(title="Not Supported", message="Queries with views are not supported")
            return

        self.visualiseTree(query)
        #pass query to preprocessing
    
    def visualiseTree(self,query):
        try:
            qm = preprocessing.Query_Manager(self.config["Database_Credentials"])
            optimal_qep_tree = qm.get_query_tree(qm.get_query_plan(query))
        except Exception as e:
            tk.messagebox.showerror(title="Error trying to get query plan", message=str(e))
            return

        #BFS to get number of nodes in the tree since we don't keep track of n
        node_count = self.bfs_get_tree_node_count(optimal_qep_tree.head)

        #Exit if node counting failed
        if node_count == -1:
            return

        #Generate the tree image and generate dictionary used for annotation generation
        annotate_dict = self.bfs_generate_tree_visual(optimal_qep_tree.head,query,node_count)
        

        #If annotate_dict is None, something went wrong in graph generation. Just ignore (shhhh)
        if not annotate_dict:
            return

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
        if self.imgGraph:
            self.imgGraph.image = ""
            self.imgGraph.photo = ""
            self.imgGraph.destroy()
        self.imgGraph = Label(self.tree_frame, image=img)
        self.imgGraph.photo = img
        self.imgGraph.grid(column=0,row=1)
        self.imgGraph.bind("<Button-1>", lambda e:self.img_handle.show())
        self.tree_frame.update()

        self.tbAnnotation.delete("0.0",tk.END)
        text = "Click on image for full size. Scroll down for more annotations (if available).\n" + annotation.get_annotations(node_count,annotate_dict)
        self.tbAnnotation.insert(tk.END,text)
        self.tbAnnotation.tag_add("instructions", "1.0", "2.0")
        self.tbAnnotation.tag_config("instructions", foreground='red')
        self.tbAnnotation.update()


    def bfs_get_tree_node_count(self, head):
        """
        Conducts a BFS on QEP_Tree

        Returns total number of nodes
        """
        node_count = 0
        try:
            t_queue = [head]
            while len(t_queue) > 0:
                curNode = t_queue.pop(0)
                node_count += 1
                if curNode.left:
                    t_queue.append(curNode.left)
                if curNode.right:
                    t_queue.append(curNode.right)
        except Exception as e:
            tk.messagebox.showerror(title="An error has occurred when generating graph", message=str(e))
            node_count = -1
        return node_count

    def bfs_generate_tree_visual(self, head, query, node_count):
        """
        Generates the tree image and saves it to tmp.jpg

        Returns dictionary for annotations generation
        """
        annotate_dict = {}

        def get_node_text(node):
            """
            Retrieve the key representative of this node in the format of 
            "[Node Id]" + " Node Type"  
            """
            text = f"[{str(node.id)}] " + node.get_aqps(self.config,query)["Optimal"]["Node Type"] + " "
            if len(node.query_clause) > 0:
                text = text + f"\n{node.query_clause[0]}"
            return text

        try:
            g = graphviz.Digraph('G')
            g.edge_attr['dir'] = 'back'
            first = True
            queue = [head]
            id = node_count
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
        except Exception as e:
            annotate_dict = None
            tk.messagebox.showerror(title="An error has occurred when generating graph", message=str(e))
        return annotate_dict