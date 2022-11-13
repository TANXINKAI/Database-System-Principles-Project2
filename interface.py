import tkinter as tk
from tkinter import ttk, Frame, SUNKEN, Label, Text, Button, Entry
from interface_lv import Interface_ListView, Interface_ListView_Schema
import preprocessing, annotation
import graphviz
from PIL import ImageTk, Image
import psycopg2

class MainWindow:
    def __init__(self, win, config):
        self.config = config
        self.query_frame = Frame(master = win)
        self.tree_frame = Frame(master = win, relief = SUNKEN, borderwidth = 5)
        
        win.columnconfigure(2, weight=1)
        win.rowconfigure(1, weight=1)
        self.query_frame.grid(column=1,row=1,sticky=tk.NSEW)
        self.tree_frame.grid(column=2, row=1, rowspan=2, sticky=tk.NSEW)

        self.query_frame.columnconfigure(2, weight=1)
        self.query_frame.rowconfigure(3, weight=1)
        self.lblQuery = Label(self.query_frame, text="Query")
        self.tbQuery = Text(self.query_frame,height=10)
        self.lblQuery.grid(column=1,row=1,sticky=tk.E)
        self.tbQuery.grid(column=2,row=1,sticky=tk.W)
        
        btnSubmitQuery = Button(master=self.query_frame, text="Submit Query", command=self.command_submit_query)
        btnSubmitQuery.grid(column=2,row=2, sticky=tk.E, padx=5, pady=2)

        self.tbAnnotation = Text(self.query_frame,height=30, bg="lightgrey")
        self.tbAnnotation.insert(tk.END,"Annotations will be loaded here after query is submitted")
        self.tbAnnotation.tag_add("instructions", "1.0", "2.0")
        self.tbAnnotation.tag_config("instructions", foreground='red')
        self.tbAnnotation.grid(column=1,row=4,sticky=tk.NSEW, columnspan=3,pady=20)


        self.tree_frame.columnconfigure(1, weight=1)
        self.tree_frame.rowconfigure(1, weight=1)
        self.img_handle = None
        self.imgGraph = None
        
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

        if query.strip() == "":
            tk.messagebox.showerror(title="Invalid Query", message="SQL Query cannot be blank")
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
        
        if self.img_handle.height > self.tree_frame.winfo_height() and self.tree_frame.winfo_height() > 0:
            resize_height = self.tree_frame.winfo_height()
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
        self.imgGraph.grid(column=1,row=1)
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
            tk.messagebox.showerror(title="Error when getting node count", message=str(e))
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
            tk.messagebox.showerror(title="Error when generating graph", message=str(e))
        return annotate_dict



class ConnectionWindow:
    def __init__(self, win):
        self.master = win
        self.master.columnconfigure(2, weight=1)
        self.master.rowconfigure(7, weight=1)
        self.schema_frame = None
        self.selected_schema = None

        padding_vertical = 0.2
        padding_horizontal = 5
        self.lblAddress = Label(self.master, text="Server Address: ")
        self.lblAddress.grid(column=1,row=1,sticky=tk.E, pady=padding_vertical)
        self.lblPort = Label(self.master, text="Port: ")
        self.lblPort.grid(column=1,row=2,sticky=tk.E, pady=padding_vertical)
        self.lblDatabase = Label(self.master, text="Database: ")
        self.lblDatabase.grid(column=1,row=3,sticky=tk.E, pady=padding_vertical)
        self.lblUsername = Label(self.master, text="Username: ")
        self.lblUsername.grid(column=1,row=4,sticky=tk.E, pady=padding_vertical)
        self.lblPassword = Label(self.master, text="Password: ")
        self.lblPassword.grid(column=1,row=5,sticky=tk.E, pady=padding_vertical)

        self.tbAddress = Entry(self.master, width=100)
        self.tbAddress.grid(column=2,row=1,sticky=tk.W, padx=padding_horizontal)
        self.tbPort = Entry(self.master, width=60)
        self.tbPort.grid(column=2,row=2,sticky=tk.W, padx=padding_horizontal)
        self.tbDatabase = Entry(self.master, width=100)
        self.tbDatabase.grid(column=2,row=3,sticky=tk.W, padx=padding_horizontal)
        self.tbUsername = Entry(self.master, width=100)
        self.tbUsername.grid(column=2,row=4,sticky=tk.W, padx=padding_horizontal)
        self.tbPassword = Entry(self.master, width=100, show = '•')
        self.tbPassword.grid(column=2,row=5,sticky=tk.W, padx=padding_horizontal)
        self.btnConnect = Button(self.master, text="Load Schemas", command=self.command_submit_connect)
        self.btnConnect.grid(column=2,row=6,sticky=tk.E, pady=padding_vertical, padx=padding_horizontal)
    
        self.tbAddress.insert(0, "localhost")
        self.tbPort.insert(0, "5432")
        self.tbDatabase.insert(0, "postgres")
        self.tbUsername.insert(0, "postgres")
        self.tbPassword.insert(0, "dbms")

    def command_submit_connect(self):
        """
        Event handler for "Connect" button click
        """
        DB_credentials = {}
        DB_credentials["DB_HOST"] = self.tbAddress.get()
        DB_credentials["DB_PORT"] = self.tbPort.get()
        DB_credentials["DB_NAME"] = self.tbDatabase.get()
        DB_credentials["DB_USER"] = self.tbUsername.get()
        DB_credentials["DB_PASS"] = self.tbPassword.get()

        def test_connect(DB_credentials):
            connection = psycopg2.connect(database=DB_credentials["DB_NAME"],
                                user=DB_credentials["DB_USER"],
                                password=DB_credentials["DB_PASS"],
                                host=DB_credentials["DB_HOST"],
                                port=DB_credentials["DB_PORT"],connect_timeout=3)
            return connection

        self.collapse()

        try:
            con = test_connect(DB_credentials)
            if(con is not None):
                self.cursor = con.cursor()
            self.cursor.execute("select nspname from pg_namespace;")
            query_results = self.cursor.fetchall()
            schemas = [schema[0] for schema in query_results]
            self.configure_schema_list()
            self.listview.seed(schemas)
        except Exception as e:
            tk.messagebox.showerror(title="Error", message=str(e))
        pass

    def collapse(self):
        if self.schema_frame:
            self.schema_frame = None

        self.master.geometry('300x155')

    def expand(self):
        self.master.geometry('300x285')

    def schema_selected(self, event):
        for selected_item in self.listview.lv.selection():
            item = self.listview.lv.item(selected_item)
            choice = tk.messagebox.askquestion("Use this schema", f"Use schema '{item['text']}'?\n\nYou will have to restart the program to change your selection afterwards.", icon='question')
            if choice == 'yes':
                self.selected_schema = item['text']
                self.master.destroy()
            else:
                self.selected_schema = None
            return

    def configure_schema_list(self):
        if self.schema_frame:
            self.schema_frame = None

        self.schema_frame = Frame(self.master, height=4, background='orange')
        self.schema_frame.grid(column=1, row=7, sticky=tk.NSEW, columnspan=2, pady=1)
        self.listview = Interface_ListView_Schema(self.schema_frame,self.schema_selected)
        self.listview_scrollbar = ttk.Scrollbar(self.schema_frame, orient=tk.VERTICAL, command=self.listview.lv.yview)
        self.listview.lv.configure(yscroll=self.listview_scrollbar.set)
        self.listview_scrollbar.grid(row=1, column=2, sticky='ns')
        self.expand()