import yaml
from preprocessing import Query_Manager, post_order_traverse_node_tree

'''
what we have in preprocessing
scan_methods = ["seqscan","bitmapscan","indexscan"]
join_methods = ["mergejoin", "hashjoin", "nestloop"]

Things to account for in our rules:
scans: 
    table scans: Seq,index,bitmap
    
    SHOULD WE INCLUDE THESE? other scans: function, value, result
joins:
    nested loop, merge, hash
others:
    sort, aggregate, unique, limit
    subplan


(a) index scan is the optimal access path for low selectivity whereas sequential scan performs better in high selectivity [2]; (b) merge join is preferred if the join inputs are large and are sorted on their join attributes [1]; (c) nested-loop join is ideal when one join input is small (e.g., fewer than 10 rows) and the other join input is large and indexed on its join attributes [1]; (d) hash join is efficient for processing large, un- sorted and non-indexed inputs compared to other join types

Factors:
    Join: input size, indexing, sorting
    Scans: selectivity level, input size

Currently output from the postorder traversal tree:
    Seq Scan ['Scan orders']
    seqscan 40897.0
    bitmapscan 20000040897.0
    indexscan 20000040897.0
    Seq Scan ['Scan customer']
    seqscan 5066.0
    bitmapscan 4.87
    indexscan 9115.87
    Hash Join ['o.o_custkey = c.c_custkey']
    hashjoin 118632.61
    mergejoin 773924.8999999999
    nestloop 714707.13


Functions to be made:

    1)
    Postorder traversal of the tree: store output into a dictionary of list of lists(?)
        Example of 1 entry in the dictionary below based on the current printed output above
        {[Seq Scan,'Scan orders'] : [[seqscan, 40897.0],[bitmapscan,20000040897.0],[indexscan,20000040897.0]]}

    2)
    Detector function: to detect if the current node is a scan or a join. 
    takes in a string (from the dictionary)
    returns either a string, scan or join 

    3)
    Scan processor: take in existing string as input,
    Add "For", the scanning clause, ".Tables are read using " , The type of scan.

    Get prestored reason why, and add that in too. 

    Return the updated string.

    4)
    Join processor: take in existing string as input,
    Add "For", the scanning clause, ".The join is implemented using " , The type of join, "as", the names of the
    other 2 joins, "increase the cost by x and y respectively."

    x and y can be calculated based on the costs stored in the dictionary

    Get prestored reason why, and add that in too. (This will be based on input size, indexing and sorting. and can be
    built in based on the respective join that was selected.)

    Return the updated string.

    5) Overall annotation processor: To generate the annotation
    Takes in the dictionary from the postorder traversal
    has a empty string variable to start with

    steps through each item in the dictionary
    
        - detector function
        - join/scan processor function (pass in the string variable)

    returns the completed annotation string variable
        
    
    
'''







with open('/Users/sidhaarth/Desktop/Database-System-Principles-Project2/credentials.yaml') as f:
    credentials = yaml.load(f,Loader = yaml.SafeLoader)

qm = Query_Manager(credentials["Database_Credentials"])
query = "select * FROM orders O, customer C WHERE O.o_custkey = C.c_custkey"
optimal_qep_tree = qm.get_query_tree(qm.get_query_plan(query))

post_order_traverse_node_tree(optimal_qep_tree.head,credentials["Database_Credentials"], query)
