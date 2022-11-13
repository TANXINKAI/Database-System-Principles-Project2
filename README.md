# **Database-System-Principles-Project2**

## Installation Guide

### Operating Environment Requirements
* Windows 10 32-bit/64-bit
    * Graphviz windows portable binaries are included, unable to run .exe on Linux/OSX
* Python >= 3.4
    * F-strings are used in code
Dependencies are based off Python 3 packages
* Pip3 (Python 3 pip)
    * For installation of python dependencies

### Python Dependencies
* Before attempting to run the python code, dependencies should be installed through the following command from the project root folder:  
```
pip install -r requirements.txt
```

### Note
Running on Linux/OSX
* Follow installation instructions of Graphviz from (https://graphviz.org/download/)
* Ensure Graphviz is in system environment / path
* Comment `sys.exit(0)` from line 19 of *project.py*
* Execute from the project root folder: *python project.py*

In this guide, users would be required to install pgAdmin and set up their own local database (PostgreSQL) and import the tpc-h data. Postgresql version should at least be 14.0 for the program to work(else enable_memoize error will occur). To change the default connection fields that are populated when the UI is loaded, modify the ‘config.yaml’ file before starting the program.
The fields can also be modified during runtime (non-persistent) on the connection interface. 

![image](https://user-images.githubusercontent.com/81215661/201454552-28019bb2-12ec-43c1-bc2a-5ae98a080389.png)

**Fig 1.1 Postgres Database Credentials**


### Run the application
To **execute** the application, users would be required to run the `project.py` file. 

If any errors are encountered, please refer to the next section.

### Troubleshooting
  1. If no file or directory ‘config.yaml’ is encountered, please add your config file directory in project.py


![image](https://user-images.githubusercontent.com/81215661/201454569-18ca2c32-e335-4cf1-983b-2aea56cd0672.png)

**Fig 1.2 Error unable to find config.yaml after running project.py**


![image](https://user-images.githubusercontent.com/81215661/201454653-aca6883e-c1bd-4676-8072-96fd6d48ba76.png)

**Fig 1.3 Editing Config File Path in project.py**

  2. If Local host connectivity is denied, please start your postgresql server (start postgresql in services.msc).


![image](https://user-images.githubusercontent.com/81215661/201454878-6d7db180-e682-4c9b-8561-8d05c9d790c2.png)

**Fig 1.4 Postgres Local Server Connection Failure**

  3. If an authentication error is encountered, please ensure your postgre credentials are accurate in the config.yaml file. In particular “DB_NAME” and “DB_PASS” refers to the database name and your pgadmin master password respectively.


![image](https://user-images.githubusercontent.com/81215661/201454889-bbb9fcf4-5bd8-4f5f-939b-5e6c56e54af8.png)

**Fig 1.5 Postgres Authentication Failure**


Error graphviz not in PATH

After Pip installing all the required dependencies and if the user still obtains an error “obtaining graph” after entering a Query in “public” database. Please copy the file path of “Your_file_directory/Database-System-Principles-Project2/graphviz_bin” into your system PATH.
Restart the device and environment after adding it into system PATH.

 ![image](https://user-images.githubusercontent.com/81215661/201504515-fe010456-af82-4323-b87d-39146ff96012.png)

**Fig 1.6 Graphviz not in System Path**



## Software UI Guide
  1. Run the Project.py file

  2. Input your Database Information and Credentials

 ![image](https://user-images.githubusercontent.com/81215661/201504563-8512c850-e653-4aeb-9407-2b7ce1c74506.png)

**Fig 2.1 Database connection**
  
  3. Load Schemas and Select the appropriate Schema Name (Usually schemas are in “public”)

 ![image](https://user-images.githubusercontent.com/81215661/201504576-c8ac67f5-6855-42a4-94ca-fab307e5ad6d.png)

**Fig 2.2 Select the Appropriate Schema Name**

 4. Enter your queries as shown in the next section


### Query Plan Visualization

 ![image](https://user-images.githubusercontent.com/81215661/201504544-0158a061-0feb-4daa-ab86-10941cda6ebf.png)

**Fig 2.3 Query Execution Plan Visualization**

In the example above, a query is entered into the text box before being submitted with the “Submit Query” button.
The generated tree is visualized on the right half of the UI and at the same time, annotated at the bottom left of the UI.
Each annotation is labeled with a number that corresponds to the representative step indicated by [X] (where X is an integer) in the visual representation of the generated tree.

