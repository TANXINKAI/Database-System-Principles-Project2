import yaml
from preprocessing import Query_Manager,traverse_node_tree

'''
what we have in preprocessing
scan_methods = ["seqscan","bitmapscan","indexscan"]
join_methods = ["mergejoin", "hashjoin", "nestloop"]

Things to account for in our rules:
scans: 
    table scans: Seq,index,bitmap
    other scans: function, value, result
joins:
    nested loop, merge, hash
others:
    sort, aggregate, unique, limit
    subplan
'''







with open('/Users/sidhaarth/Desktop/Database-System-Principles-Project2/credentials.yaml') as f:
    credentials = yaml.load(f,Loader = yaml.SafeLoader)

qm = Query_Manager(credentials["Database_Credentials"])
query = "select * FROM orders O, customer C WHERE O.o_custkey = C.c_custkey"
optimal_qep_tree = qm.get_query_tree(qm.get_query_plan(query))

traverse_node_tree(optimal_qep_tree.head,credentials["Database_Credentials"], query)