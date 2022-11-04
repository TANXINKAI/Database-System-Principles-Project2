import re
import psycopg2
import yaml
from yaml.loader import SafeLoader
import sqlvalidator
import sqlparse
from turtle import Turtle,Screen
from math import acos

class DB_Connection:
    """
    init: creates a new DB connection and cursor to the Database with DB Credentials.
    DB Credentials can be found and edited on credentials.yaml
    """
    def __init__(self,DB_credentials):
        self.connection = self.connect_to_DB(DB_credentials)
        if(self.connection is not None):
            self.cursor = self.connection.cursor()
    """
    connect_to_DB: connect to DB with DB Credentails, using psycopg2.connect.
    """
    def connect_to_DB(self,DB_credentials):
        try:
            connection = psycopg2.connect(database=DB_credentials["DB_NAME"],
                                user=DB_credentials["DB_USER"],
                                password=DB_credentials["DB_PASS"],
                                host=DB_credentials["DB_HOST"],
                                port=DB_credentials["DB_PORT"])
            return connection
        except AttributeError:
            print("Could not connect to Database, check credentials specified in credentials.yaml")
            return None
    """
    execute: Use the cursor to execute query, returns the query_results.
    """
    def execute(self, SQL_query:str):
        self.cursor.execute(SQL_query)
        try:
            query_results = self.cursor.fetchall()
            return query_results
        except:
            return None

    """
    close: Close connection
    """
    def close(self):
        self.cursor.close()
        self.connection.close()


class Query_Manager:
    """
    init: Create a DB connection with DB_Connection Class
    """
    def __init__(self, DB_Credentials):
        self.DB_conn = DB_Connection(DB_Credentials)
    
    # TODO Check Query to see if it valid: check format, check if where, groupby and order by clauses have valid relations 

    # def check_query(self, query:str):
    #     try:
    #         formatted_query = sqlvalidator.format_sql(query)
    #         print(formatted_query)
    #         parsed_query = sqlvalidator.parse(formatted_query)
    #         if not parsed_query.is_valid():
    #             print(parsed_query.errors)
    #         else:
    #             return formatted_query
    #     except ParsingError:
    #         print("Invalid Query.")


    def get_query_plan(self,query:str):
        """
        get_query_plan: pass in a query to execute query with EXPLAIN to output a query_plan
        """
        self.disable_memoization()
        query_plan_prefix = "EXPLAIN(ANALYZE,VERBOSE,FORMAT JSON) "
        query_plan = self.DB_conn.execute(query_plan_prefix+ query)
        return query_plan[0][0][0]["Plan"]
    
    def disable_method(self, method:str):
        """
        disable_method: disable methods for AQP generation.
        """
        self.DB_conn.execute("SET enable_"+method+" = OFF;")

    def enable_method(self, method:str):
        """
        enable_method: enable methods for AQP generation.
        """
        self.DB_conn.execute("SET enable_"+method+" = ON;")
    
    def disable_parallelism(self):
        """
        disable_parallelism: disables multiple workers 
        """
        self.DB_conn.execute("SET max_parallel_workers_per_gather = 0;")
    
    def enable_parallelism(self):
        """
        enable_parallelism: sets the number of multipe workers back to the default value, 2.
        """
        self.DB_conn.execute("SET max_parallel_workers_per_gather = 0;")
    
    def disable_memoization(self):
        """
        disable_memoization: removes cache use
        """
        self.DB_conn.execute("SET enable_memoize = OFF;")
    

    def get_query_tree(self,query_plan:str):
        """
        get_query_tree: uses the query plan to output a binary query tree using the QEP_Tree class.
        """
        tree = QEP_Tree(query_plan)
        return tree
    
    def get_aqp(self,query,method,type):
        """
        get_aqp: given a query, a method, and a type, returns the AQP using that method. 

        Ex: 
        query: query = "select * from customer C,orders O where C.c_custkey = O.o_custkey"
        method: "nestloop"
        type: "Join"

        Output: Query Plan of AQP that uses nested loop to perform the join function. 
        """
        scan_methods = ["seqscan","bitmapscan","indexscan"]
        join_methods = ["mergejoin", "hashjoin", "nestloop"]
        if(type == "Scan"):
            for m in scan_methods:
                if(m != method):
                    self.disable_method(m)
            self.disable_memoization()
            qp = self.get_query_plan(query)
            for m in scan_methods:
                if(m != method):
                    self.enable_method(m)
            return qp
        elif (type == "Join"):
            for m in join_methods:
                if(m != method):
                    self.disable_method(m)
            self.disable_memoization()
            qp = self.get_query_plan(query)
            for m in join_methods:
                if(m != method):
                    self.enable_method(m)
            return qp


class QEP_Node():
    """
    init: Builds a QEP Node based on query input, recursively builds children QEP Nodes based on the "Plans" field in the QEP.
    Each QEP_Node has the attributes:
    self.left: left child in binary query execution tree
    self.right: right child in binary query execution tree
    self.parallel_execution: check if node is performed with parallelism 
    self.query_clause: Sorted array of conditions(fiter, join condition, relations for scan nodes)for each node. 
    query_clause is used for comparison to check which nodes work on the same part of the query to compare AQPs.
    self.query_result: Contains all meta data about the query_result, without "Plans" 
    """
    def __init__(self,query_result):
        self.query_clause = []
        if("Plans" in query_result.keys()):
            if(len(query_result["Plans"]) == 2):
                self.left = QEP_Node(query_result["Plans"][0])
                self.right = QEP_Node(query_result["Plans"][1])
            elif(len(query_result["Plans"]) == 1):
                self.left = QEP_Node(query_result["Plans"][0])
                self.right = None
        else:
            self.left = None
            self.right = None
        
        if("Workers" in  query_result.keys()):
            self.parallel_execution = True
        else:
            self.parallel_execution = False 
        
        for key in query_result.keys():
            if(key.endswith("Cond")):
                clauses = re.findall('\({1,}(.*?)\)', query_result[key])
                self.query_clause = self.query_clause + clauses
            elif(key == 'Filter'):
                clauses = re.findall('\({1,}(.*?)\)', query_result[key])
                self.query_clause = self.query_clause + clauses
            elif(query_result["Node Type"].endswith("Scan") and "Relation Name" in key):
                self.query_clause.append("Scan "+ query_result["Relation Name"])
        
        self.query_clause.sort()

        query_result.pop("Plans",None) 
        self.query_result = query_result
        
    def get_aqp_cost(self, credentials, query):
        """
        get_aqp_cost: For each node:
        1) Find Node Type 
        2) Based on Node Type, get AQP Trees for different methods
        3) For each AQP Tree, find the Node that should be considered by comparing the query clauses
        4) Get meta data for Node. 
        5) Return dictionary that contains the meta data of the optimal QEP and opther AEP 
        """
        qm = Query_Manager(credentials)         
        cost = {}
        scan_methods = ["seqscan","bitmapscan","indexscan"]
        join_methods = ["mergejoin", "hashjoin", "nestloop"]
        if("Scan" in self.query_result["Node Type"]):
            if("Seq" in self.query_result["Node Type"]):
                method = "seqscan"
            elif("Bitmap" in self.query_result["Node Type"]):
                method = "bitmapscan"
            elif("Index" in self.query_result["Node Type"]):
                method = "indexscan"
            cost[method] = self.query_result
            for m in scan_methods:
                if(m != method):
                    aqp_tree = qm.get_query_tree(qm.get_aqp(query, m, "Scan"))
                    node_details = aqp_tree.get_node_with_clause(self.query_clause, self.query_result)
                    cost[m] = node_details
            return cost
        elif("Join" in self.query_result["Node Type"]):
            if("Hash" in self.query_result["Node Type"]):
                method = "hashjoin"
            elif("Merge" in self.query_result["Node Type"]):
                method = "mergejoin"
            elif("Nested Loop" in self.query_result["Node Type"]):
                method = "nestloop"
            cost[method] = self.query_result
            for m in join_methods:
                if(m != method):
                    if(not self.parallel_execution):
                        qm.disable_parallelism()
                    aqp_tree = qm.get_query_tree(qm.get_aqp(query, m, "Join"))
                    node_details = aqp_tree.get_node_with_clause(self.query_clause, self.query_result)
                    cost[m] = node_details
                    qm.enable_parallelism()
            return cost
        

    
class QEP_Tree():
    """
    init: Contains the head node of a binary query tree
    """
    def __init__(self, query_result):
        self.head = QEP_Node(query_result)
    
    def get_node_with_clause(self,clause,query_result):
        """
        get_node_with_clause: Does a bfs over the binary query tree, checks if node is target node by checking clauses and QEP query result. 
        TODO: Deal with clauses that are the same but have differing order: ['c.c_custkey = o.o_custkey'] != ['o.o_custkey = c.c_custkey']
        TODO: Deal with scans that have a filter
        """
        queue = []
        queue.append(self.head)
        while(len(queue) > 0):
            node = queue.pop(0)
            if(self.check_clauses(node,clause,query_result)):
                return node.query_result
            else:
                if(node.left is not None):
                    queue.append(node.left)
                if(node.right is not None):
                    queue.append(node.right)
    
    def check_clauses(self,node,clause,query_result):
        """
        check_clauses: checks if the query clauses of 2 different nodes are the same. 
        """
        if(node.query_clause == clause):
            #print("Direct Clause Match")
            return True
        elif(node.query_result["Node Type"] == "Nested Loop" and node.query_result["Output"] == query_result["Output"]):
            #print("Nested Loop Match")
            return True
        elif("Scan" in node.query_result["Node Type"] and "Scan" in query_result["Node Type"]):
            node_scan = None
            target_scan = None
            for i in range(0,len(node.query_clause)):
                if("Scan" in node.query_clause[i]):
                    node_scan = node.query_clause[i]
            for i in range(0, len(clause)):
                if("Scan" in clause[i]):
                    target_scan = clause[i]
            if(node_scan == target_scan):
                #print("Scan Match")
                return True
        else:
            node_clauses = []
            for i in range(0, len(node.query_clause)):
                c = re.split("\s+", node.query_clause[i])
                c.sort()
                node_clauses.append(c)
            target_clauses = []
            for i in range(0, len(clause)):
                c = re.split("\s+", clause[i])
                c.sort()
                target_clauses.append(c)
            #print("Reorder Match",node_clauses,target_clauses,node_clauses == target_clauses)
            return node_clauses == target_clauses     

               
def traverse_node_tree(head,credentials, query):
    queue = []
    queue.append(head)
    while(len(queue) > 0):
        node = queue.pop(0)
        d = node.get_aqp_cost(credentials, query)
        print(node.query_result["Node Type"],node.query_clause)
        if(d is not None):
            for key in d.keys():
                print(key, d[key]["Startup Cost"] + d[key]["Total Cost"])
        if(node.left is not None):
            queue.append(node.left)
        if(node.right is not None):
            queue.append(node.right)

#traverse_node_tree(optimal_qep_tree.head,credentials["Database_Credentials"], query)

##################################################################################################



def height(root):
    '''
    height: Returns the height of the binary tree
    '''
 
    # Check if the binary tree is empty
    if root is None:
        # If TRUE return 0
        return 0 
    # Recursively call height of each node
    leftAns = height(root.left)
    rightAns = height(root.right)
 
    # Return max(leftHeight, rightHeight) at each iteration
    return max(leftAns, rightAns) + 1


DOT_DIAMETER = 30
GENERATION_DISTANCE = 75
def tree(turtle, d, origin,node):
    '''
    tree: function to draw the binary operator tree using turtle
    '''

    turtle.penup()
    turtle.setposition(origin)
    if (node):
        
        turtle.write(node.query_result["Node Type"], move = False, font = ("Arial",12,"normal"))
    if d == 0:  # base case
        return

    distance = (GENERATION_DISTANCE**2 + (2**d * DOT_DIAMETER / 2)**2)**0.5
    angle = acos(GENERATION_DISTANCE / distance)

    if node.right:
        turtle.pendown()
    turtle.left(angle)
    turtle.forward(distance)
    right = turtle.position()
    turtle.right(angle)

    turtle.penup()
    turtle.setposition(origin)
    if node.left:
        turtle.pendown()
    turtle.right(angle)
    turtle.forward(distance)
    left = turtle.position()
    turtle.left(angle)

    tree(turtle, d - 1, right,node.right)  # recurse right branch
    tree(turtle, d - 1, left,node.left)  # recurse left branch



