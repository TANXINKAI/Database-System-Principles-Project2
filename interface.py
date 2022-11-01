from tkinter import *
from preprocessing import *


class MyWindow:
    def __init__(self, win):
        self.lbl1=Label(win, text='Please input query:')
        self.lbl3=Label(win, text='Result')
        self.t1=Entry(bd=3)
        self.t3=Entry()
        self.btn1 = Button(win, text='Process query')
        self.lbl1.place(x=100, y=50, width = 180, height = 100)
        
        self.t1.place(x=250, y=50)
        self.b1=Button(win, text='Process', command=self.process)
        self.b1.place(x=100, y=150)
        self.lbl3.place(x=100, y=200)
        self.t3.place(x=250, y=200)
    def process(self):
        self.t3.delete(0, 'end')
        num1=int(self.t1.get())
        result=num1
        self.t3.insert(END, str(result))

    def visualiseTree(self):
        pass
    

window=Tk()
mywin=MyWindow(window)
window.title('Query Plan Processing')
window.geometry("400x300+10+10")
window.mainloop()


