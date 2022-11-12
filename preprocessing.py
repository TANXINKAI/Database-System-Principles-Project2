import re
import psycopg2

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
            option = f"-c search_path={DB_credentials['DB_SCHEMA']}"
            connection = psycopg2.connect(database=DB_credentials["DB_NAME"],
                                user=DB_credentials["DB_USER"],
                                password=DB_credentials["DB_PASS"],
                                host=DB_credentials["DB_HOST"],
                                port=DB_credentials["DB_PORT"], options=option)
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
    
    def get_query_plan(self,query:str):
        """
        get_query_plan: pass in a query to execute query with EXPLAIN to output a query_plan
        """
        query_plan_prefix = "EXPLAIN(VERBOSE, FORMAT JSON)"
        query_plan = self.DB_conn.execute(query_plan_prefix + query)
        return query_plan[0][0][0]["Plan"]
        
    def get_query_tree(self,query_plan:str):
        """
        get_query_tree: uses the query plan to output a binary query tree using the QEP_Tree class.
        """
        tree = QEP_Tree(query_plan)
        return tree

    def get_qep_information(self, query:str):
        """
        get_qep_information: analyses a qep to output query data that is useful for aqp generation.
        Output: A dictionary containing
         - all the scan methods used in the query 
         - all the join methods used in the query
         - whether the qep uses parallel queries 
        """
        plan = self.get_query_plan(query)
        query_tree = self.get_query_tree(plan)
        queue = []
        queue.append(query_tree.head)
        parallel_query = False
        scan_methods = []
        join_methods = []
        while(len(queue) > 0):
            current_node = queue.pop(0)
            if(current_node.parallel_execution):
                parallel_query = True
            
            node_type = current_node.query_result["Node Type"]
            
            if("Scan" in node_type and node_type not in scan_methods):
                scan_methods.append(node_type)
            
            elif("Join" in node_type or "Nested Loop" in node_type and node_type not in join_methods):
                join_methods.append(node_type)
            
            if(current_node.left is not None):
                queue.append(current_node.left)
            if(current_node.right is not None):
                queue.append(current_node.right)
        
        qep_information = {"parallel_query": parallel_query, "join_methods": join_methods, "scan_methods": scan_methods} 
        return qep_information
    
    def set_parallelize(self, qep_information, parallelize_config):
        """
        set_parallelize: takes in qep information to set the max number of workers
        For postgres, the default value is 2. 

        """
        parallel_query = qep_information["parallel_query"]
        if(parallel_query):
            self.DB_conn.execute("SET "+parallelize_config+" = 2")
        else:
            self.DB_conn.execute("SET "+parallelize_config+" = 0")
    
    def disable_method(self, method_config:str):
        """
        disable_method: disable methods for AQP generation.
        """
        self.DB_conn.execute("SET "+method_config+" = OFF;")

    def enable_method(self, method_config:str):
        """
        enable_method: enable methods for AQP generation.
        """
        self.DB_conn.execute("SET "+method_config+" = ON;")

    def set_aqp_parameters(self, qp_scan_params, qp_join_params, scan_params, join_params):
        """
        set_aqp_parameters: enables or disables scan or join methods based on aqp requirements 
        """
        for param in scan_params.keys():
            if(param not in qp_scan_params):
                self.disable_method(scan_params[param])
            else:
                self.enable_method(scan_params[param])
        
        for param in join_params.keys():
            if(param not in qp_join_params):
                self.disable_method(join_params[param])
            else:
                self.enable_method(join_params[param])
    
    def get_num_rows(self, relation):
        """
        get_num_rows: Gets the number of rows a relational table has. 
        Used to calculate selectivity for the leaf nodes of the QEP
        """
        row_num = self.DB_conn.execute("SELECT COUNT(*) FROM "+ relation)
        return row_num[0][0]
    
    def get_index(self, relation):
        """
        get_index: Gets the indexes of a relation. 
        Used to see if the relational table is indexed over the columns used in the QEP  
        """
        indexes = []
        indexes_tuple = self.DB_conn.execute("select C.COLUMN_NAME "\
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS T "\
            "JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C "\
            "ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME "\
            "WHERE C.TABLE_NAME LIKE "+ "'{}'".format(relation) + " AND T.constraint_type LIKE 'PRIMARY KEY'")
        
        if(len(indexes_tuple) == 0):
            return indexes
        for i in range(0, len(indexes_tuple[0])):
            indexes.append(indexes_tuple[0][i])
        return indexes

    
class QEP_Node():
    """
    init: Builds a QEP Node based on query input, recursively builds children QEP Nodes based on the "Plans" field in the QEP.
    Each QEP_Node has the attributes:

    self.left: left child in binary query execution tree
    self.right: right child in binary query execution tree
    Note: Subplans are ignored. 
    self.parallel_execution: check if node is performed with parallelism 
    self.query_clause: Sorted array of conditions(filters, join conditions, relations for scan nodes) for each node. 
    query_clause is used for comparison to check which nodes work on the same part of the query to compare AQPs.
    self.query_result: Contains all meta data about the query_result, excluding "Plans"
    """
    def __init__(self,query_result):
        self.query_clause = []
        self.id = -1
        if("Plans" in query_result.keys()):
            non_subplan_nodes = []
            for plan in query_result["Plans"]:
                if("SubPlan" not in plan["Parent Relationship"]):
                    non_subplan_nodes.append(plan)
            if(len(non_subplan_nodes) == 2):
                self.left = QEP_Node(non_subplan_nodes[0])
                self.right = QEP_Node(non_subplan_nodes[1])
            elif(len(non_subplan_nodes) == 1):
                self.left = QEP_Node(non_subplan_nodes[0])
                self.right = None
            else:
                self.left = None
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
            elif(key == "Filter"):
                clauses = re.findall('\({1,}(.*?)\)', query_result[key])
                self.query_clause = self.query_clause + clauses
            elif(query_result["Node Type"].endswith("Scan") and "Relation Name" in key):
                self.query_clause.append("Scan "+ query_result["Relation Name"])
        
        self.query_clause.sort()

        query_result.pop("Plans", None) 
        self.query_result = query_result
        
    def get_aqps(self, config, query):
        """
        get_aqp_cost: For each node:
        1) Find Node Type (Scan/Join)
        2) Get Node Specifics (selectivity, columns used, sort keys, extimated number of rows)
        3) Based on Node Type, get AQP Trees for different scan/join methods 
        4) For each AQP Tree, find the Node that should be considered by comparing the query clauses
        5) Get meta data for Node. 
        6) Return dictionary that contains the meta data of AQPs and Node Specifics. 
        """
        qm = Query_Manager(config["Database_Credentials"])         
        qep_data = {}
        aqp_data = {}
        node_type = self.query_result["Node Type"]
        query_plan = qm.get_qep_information(query)
        qm.set_parallelize(query_plan, config["Supported_Query_Plan_Parameters"]["Parallelize"])
        
        qep_data["Optimal"] = self.query_result

        if("Join" in node_type or "Nested Loop" in node_type):
            qep_data["left_num_rows"] = self.left.query_result["Plan Rows"]
            qep_data['right_num_rows'] = self.right.query_result["Plan Rows"]

            qep_data["left_is_sorted_on"] = self.is_sorted_on(self.left)
            qep_data["right_is_sorted_on"] = self.is_sorted_on(self.right)

            index = query_plan["join_methods"].index(node_type)
            for param in config["Supported_Query_Plan_Parameters"]["Joins"]:
                if(param != node_type):
                    query_plan["join_methods"][index] = param
                    qm.set_aqp_parameters(query_plan["scan_methods"], query_plan["join_methods"], config["Supported_Query_Plan_Parameters"]["Scans"], config["Supported_Query_Plan_Parameters"]["Joins"])
                    alternate_query = qm.get_query_plan(query)
                    alternate_query_tree = qm.get_query_tree(alternate_query)
                    aqp_txt = alternate_query_tree.get_node_with_clause(self.query_clause, self.query_result)
                    if(aqp_txt and aqp_txt["Node Type"] == param):
                        aqp_data[param] = aqp_txt
        
        elif("Scan" in node_type):
            num_rows_after_predicate = self.query_result["Plan Rows"]
            if(self.left is not None):
                num_rows_before_predicate = self.left.query_result["Plan Rows"]
            else:
                num_rows_before_predicate = qm.get_num_rows(self.query_result["Relation Name"])

            qep_data["num_rows_after_predicate"] = num_rows_after_predicate
            qep_data["num_rows_before_predicate"] = num_rows_before_predicate

            columns_used = []
            indexes = qm.get_index(self.query_result["Relation Name"])
            for i in range(0, len(self.query_clause)):
                s = re.split("\s+", self.query_clause[i])
                if(s[0] != "Scan" and s[0] not in columns_used):
                    columns_used.append(s[0])
            
            for i in range(0,len(indexes)):
                indexes[i] = str(self.query_result["Relation Name"])+"."+indexes[i]

            qep_data["indexes"] = indexes
            qep_data["columns_used"] = columns_used

            index = query_plan["scan_methods"].index(node_type)
            for param in config["Supported_Query_Plan_Parameters"]["Scans"]:
                if(param != node_type):
                    query_plan["scan_methods"][index] = param
                    qm.set_aqp_parameters(query_plan["scan_methods"], query_plan["join_methods"], config["Supported_Query_Plan_Parameters"]["Scans"], config["Supported_Query_Plan_Parameters"]["Joins"])
                    alternate_query = qm.get_query_plan(query)
                    alternate_query_tree = qm.get_query_tree(alternate_query)
                    aqp_txt = alternate_query_tree.get_node_with_clause(self.query_clause, self.query_result)
                    if(aqp_txt["Node Type"] == param):
                        aqp_data[param] = aqp_txt   
        qep_data["aqp_data"] = aqp_data
        return qep_data

    def is_sorted_on(self,head):
        """
        is_sorted_on: returns an array fo columns that a certain node's output table is sorted on 
        Based on Sort Nodes below current node.     
        """
        queue = []
        sorted_on = []
        queue.append(head)
        while(len(queue) > 0):
            node = queue.pop(0)
            if(node.query_result["Node Type"] == "Sort"):
                sorted_on.append(*node.query_result["Sort Key"])
            if(node.left is not None):
                queue.append(node.left)
            if(node.right is not None):
                queue.append(node.right)
        return sorted_on

            
        

class QEP_Tree():
    """
    init: Contains the head node of a binary query tree
    """
    def __init__(self, query_result):
        self.head = QEP_Node(query_result)
        self.node_count = 1
    
    def get_node_with_clause(self,clause,query_result):
        """
        get_node_with_clause: Does a bfs over the binary query tree, checks if node is target node by checking clauses and QEP query result. 
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
            # Direct Clause Match
            return True
        elif(node.query_result["Node Type"] == "Nested Loop" and node.query_result["Output"] == query_result["Output"]):
            # Nested Loop Match
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
                # Scan Match
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
            # Reorder Match 
            return node_clauses == target_clauses


def height(root):
    '''
    height: Returns the height of the binary tree
    '''
    if root == None:
        return 0 
    return max(height(root.left), height(root.right)) + 1

