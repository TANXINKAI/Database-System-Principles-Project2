from tkinter import *
from tkinter import ttk



class MyWindow:
    def __init__(self, win):
        query_frame = Frame(master = win, relief = SUNKEN, borderwidth = 5)
        tree_frame = Frame(master = win, relief = SUNKEN, borderwidth = 5)
        annotation_frame = Frame(master = win, relief = SUNKEN, borderwidth = 5)

        self.querylabel = Label(master = win, text = "Query")
        self.querylabel.place(relx=0.25, rely=0.02)
        
        self.queryinputlabel=Label(master = win, text='Please input query:')
        self.queryinputlabel.place(x=100, y=50)
        
        
        self.qinputbox=Entry(bd=3)
        self.qinputbox.place(x=250, y=50, width = 300, height = 100)
        
        
        self.querylabel=Label(win, text='Query:')
        self.querylabel.place(x=100, y=200)

        self.displayquery=Entry()
        self.displayquery.place(x=250, y=200,width = 300, height = 100)
        
        self.processbutton=Button(win, text='Process Query',fg = "blue",relief = SUNKEN, command=self.process)
        self.processbutton.place(x=250, y=150)

        separator = ttk.Separator(win, orient='vertical')
        separator.place(relx=0.5, rely=0, relwidth=0, relheight=1)

        self.treelabel = Label(master = win, text = "Operator Tree")
        self.treelabel.place(relx=0.25, rely=0.5)
        

        self.annotationlabel = Label(master = win, text = "Annotations")
        self.annotationlabel.place(relx=0.75, rely=0.02)
        
        
        
    def process(self):
        self.displayquery.delete(0, 'end')
        query=(self.qinputbox.get())
        result=query
        self.displayquery.insert(END, str(result))

    def visualiseTree(self):
        pass
    

window=Tk()
width= window.winfo_screenwidth()
height= window.winfo_screenheight()
window.geometry("%dx%d" % (width, height))
mywin=MyWindow(window)
window.title('Query Plan Processing')

window.mainloop()


