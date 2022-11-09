from interface import MyWindow
from tkinter import  Tk, Canvas
import yaml
from yaml.loader import SafeLoader
from turtle import Turtle,Screen
import preprocessing, annotation 

'''
Execution code from preprocessing
TODO: clean up the printing of the tree, (does the cost need to be shown too?), embed the tree into the tkinter window
'''

with open('config.yaml') as f:
    config = yaml.load(f,Loader = SafeLoader)

qm = preprocessing.Query_Manager(config["Database_Credentials"])
query = "select * FROM orders O, customer C WHERE O.o_custkey = C.c_custkey"
optimal_qep_tree = qm.get_query_tree(qm.get_query_plan(query))
data = preprocessing.post_order_wrap(optimal_qep_tree.head, config, query)
print(annotation.get_annotations(data))

screen = Screen()

turt = Turtle()
turt.radians()  # to accommodate acos()
turt.right(1.5807)

preprocessing.tree(turt, preprocessing.height(optimal_qep_tree.head)-1, (0, 0),optimal_qep_tree.head)

screen.mainloop()

'''
Execution code from interface
'''
window = Tk()
width= window.winfo_screenwidth() / 2
height= window.winfo_screenheight() / 2
window.geometry("%dx%d" % (width, height))
mywin=MyWindow(window)
window.title('Query Plan Processing')

window.mainloop()

