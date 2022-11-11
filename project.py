from interface import MainWindow, ConnectionWindow
from tkinter import  Tk
import yaml
from yaml.loader import SafeLoader
import os
import sys
from sys import platform

'''
Execution code from preprocessing
TODO: clean up the printing of the tree, (does the cost need to be shown too?), embed the tree into the tkinter window
'''

with open('config.yaml') as f:
    config = yaml.load(f,Loader = SafeLoader)

if platform == "linux" or platform == "linux2" or platform == "darwin":
    print("I haven't programmed for these platforms. Graphviz binaries that i've included are compiled for WINDOWs (.exe) only - Austin")
    sys.exit(0)
elif platform == "win32":
    graphviz_path = os.path.join(os.getcwd(),"graphviz_bin") + ";"
    os.environ["PATH"] = os.environ["PATH"] + graphviz_path

'''
Execution code from interface
'''


master = Tk()
width= 300
height= 135

pos = "+%d+%d" % ((master.winfo_screenwidth() / 2) - (width/2),(master.winfo_screenheight() / 2) - (height/2))

master.geometry("%dx%d%s" % (width, height, pos))
con_window=ConnectionWindow(master)
master.title('Connect To Server')

master.mainloop()
if con_window.selected_schema:
    config['Database_Credentials']['DB_SCHEMA'] = con_window.selected_schema
    window = Tk()
    width= window.winfo_screenwidth() * 0.75
    height= window.winfo_screenheight() * 0.75
    pos = "+%d+%d" % ((window.winfo_screenwidth() / 2) - (width/2),(window.winfo_screenheight() / 2) - (height/2))
    window.geometry("%dx%d%s" % (width, height,pos))
    mywin=MainWindow(window, config)
    window.title('Query Plan Processing')

    window.mainloop()

## Cleanup, removes any temporary image file that was generated to keep the folders clean
if os.path.exists('./tmp'):
    os.remove('./tmp')
if os.path.exists('./tmp.jpg'):
    os.remove('./tmp.jpg')