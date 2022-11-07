import yaml
from preprocessing import Query_Manager, post_order_traverse_node_tree

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


(a) index scan is the optimal access path for low selectivity whereas sequential scan performs better in high selectivity [2]; (b) merge join is preferred if the join inputs are large and are sorted on their join attributes [1]; (c) nested-loop join is ideal when one join input is small (e.g., fewer than 10 rows) and the other join input is large and indexed on its join attributes [1]; (d) hash join is efficient for processing large, un- sorted and non-indexed inputs compared to other join types

Factors:
    Join: input size, indexing, sorting
    Scans: selectivity level, input size
'''







with open('/Users/sidhaarth/Desktop/Database-System-Principles-Project2/credentials.yaml') as f:
    credentials = yaml.load(f,Loader = yaml.SafeLoader)

qm = Query_Manager(credentials["Database_Credentials"])
query = "select * FROM orders O, customer C WHERE O.o_custkey = C.c_custkey"
optimal_qep_tree = qm.get_query_tree(qm.get_query_plan(query))

post_order_traverse_node_tree(optimal_qep_tree.head,credentials["Database_Credentials"], query)