import re
# import yaml
# from yaml.loader import SafeLoader
# import preprocessing, interface

def state_qp_step(count,optimal_data):
    """
    state_qp_step: Takes in a node from the optimal QEP and outputs text that describes the actions of the node.
    """
    output_txt = ""
    filter_conditions = []

    for key in optimal_data.keys():
        if(key.endswith("Cond")):
            clauses = re.findall('\({1,}(.*?)\)', optimal_data[key])
            for clause in clauses:
                if(clause not in filter_conditions):
                    filter_conditions.append(clause)

        elif(key == "Filter"):
            clauses = re.findall('\({1,}(.*?)\)', optimal_data[key])
            for clause in clauses:
                if(clause not in filter_conditions):
                    filter_conditions.append(clause)

    if("Scan" in optimal_data["Node Type"]):
        if(len(filter_conditions) > 0):
            output_txt += "{}) A {} is performed on {} with the filter(s) {}.\n".format(count, optimal_data['Node Type'], optimal_data["Relation Name"], filter_conditions)
        else:
            output_txt += "{}) A {} is performed on {}.\n".format(count, optimal_data['Node Type'], optimal_data["Relation Name"])
    else:
        if(len(filter_conditions) > 0):
            output_txt += "{}) A {} is performed over the condition(s) {}.\n".format(count, optimal_data['Node Type'], filter_conditions)
        else:
            if("Hash" in optimal_data["Node Type"]):
                hash_output = optimal_data["Output"][0]
                hash_relation = re.findall("(\w+)", hash_output)[0]
                output_txt += "{}) A {} is performed on {}.\n".format(count, optimal_data['Node Type'], hash_relation)
            elif("Sort" in optimal_data["Node Type"]):
                sort_key = optimal_data["Sort Key"]
                output_txt += "{}) A {} is performed with key {}.\n".format(count, optimal_data['Node Type'], sort_key)
            else:
                output_txt += "{}) A {} is performed.\n".format(count, optimal_data['Node Type'])
    return output_txt

def explain_scan(count, scan_dict):
    """
    explain_scan: Explains why a certain scan was selected.
    Uses selectivity of rows, and check if columns used to scan are indexes of the relation
    """
    output_txt = ""
    output_txt = output_txt + state_qp_step(count, scan_dict["Optimal"])
    col_not_index = []
    col_is_index = []

    if(scan_dict["num_rows_before_predicate"] > 0):
        selectivity = scan_dict["num_rows_after_predicate"]/scan_dict["num_rows_before_predicate"]
    else:
        selectivity = None

    if(len(scan_dict["indexes"]) == 0):
        output_txt += "The table ({}) is not indexed over any columns.\n".format(scan_dict["Optimal"]["Relation Name"])
    else:
        output_txt += "The table ({}) is indexed over the columns: {}.\n".format(scan_dict["Optimal"]["Relation Name"], scan_dict["indexes"])
        for column in scan_dict["columns_used"]:
            if(column in scan_dict["indexes"]):
                col_is_index.append(column)
            else:
                col_not_index.append(column)
        if(len(col_is_index) == 0 and len(col_not_index) == 0):
            output_txt += "There are no filter columns for this scan.\n"
        elif(len(col_is_index) > 0):
            output_txt += "The table ({}) is indexed over some/all of the columns that are used to filter the rows ({}).\n".format(scan_dict["Optimal"]["Relation Name"], col_is_index)
        elif((len(col_is_index) == 0) and len(col_not_index) > 0):
            output_txt += "The table ({}) is not indexed over any of the columns that are used to filter the rows ({}).\n".format(scan_dict["Optimal"]["Relation Name"], col_not_index)
    
    output_txt += "The table ({}) is estimated to have had {} rows before this step.\n".format(scan_dict["Optimal"]["Relation Name"], scan_dict["num_rows_before_predicate"])
    output_txt += "The table ({}) is estimated to have {} after this step\n".format(scan_dict["Optimal"]["Relation Name"], scan_dict["num_rows_after_predicate"])
    
    if(selectivity is None):
        output_txt += "There is no selectivity for this scan.\n"
    elif(selectivity <= 0.25):
        output_txt += "The estimated selectivity for this scan is {}, which means that the filters used in the scan are highly selective.\n".format(selectivity)
    elif(selectivity < 1):
        output_txt += "The estimated selectivity for this scan is {}, which means that the filters used in the scan are not as selective.\n".format(selectivity)
    else:
        output_txt += "The estimated selectivity for this scan is {}, which means that all the rows have been selected.\n".format(selectivity)
    
    output_txt += "Based on the selectivity and/or the filter columns, {} was used.\n".format(scan_dict["Optimal"]["Node Type"])

    output_txt += "The estimated cost for {} is {}.\n".format(scan_dict["Optimal"]["Node Type"], scan_dict["Optimal"]["Total Cost"])

    if(len(scan_dict["aqp_data"]) != 0 and scan_dict["Optimal"]["Total Cost"] >= 0):
        output_txt += "Alternate Plans:\n"
        for aqp in scan_dict["aqp_data"].values():
            output_txt += "{}: {} ({:.1f} times slower)\n".format(aqp["Node Type"], aqp["Total Cost"], (aqp["Total Cost"])/(scan_dict["Optimal"]["Total Cost"]))

    return output_txt

def explain_join(count, join_dict):
    """
    explain_join: Explains why a certain join was selected.
    Uses number of rows each input table has, and checks whether any if the input tables are sorted on the join condition.

    """
    output_txt = ""
    output_txt = output_txt + state_qp_step(count, join_dict["Optimal"])
    output_txt += "The left table is estimated to have {} rows".format(join_dict["left_num_rows"])
    if(len(join_dict["left_is_sorted_on"]) == 0):
        output_txt += " and is not sorted.\n"
    else:
        output_txt += " and is sorted on {}.\n".format(join_dict['left_is_sorted_on'])
    
    output_txt += "The right table is estimated to have {} rows ".format(join_dict["right_num_rows"])
    if(len(join_dict["right_is_sorted_on"]) == 0):
        output_txt += "and is not sorted.\n"
    else:
        output_txt += "and is sorted on {}.\n".format(join_dict['right_is_sorted_on'])
    
    output_txt += "Estimated number of rows in the smaller table: {}\n".format(min(join_dict["left_num_rows"],join_dict["right_num_rows"]))

    if("Nested Loop" in join_dict["Optimal"]["Node Type"]):
        output_txt += "{} was chosen as the number of rows of at least 1 of the tables is small enough to fit into memory.\n".format(join_dict["Optimal"]["Node Type"])

    elif("Merge Join" in join_dict["Optimal"]["Node Type"]):
        left = False
        right = False
        for column in join_dict["left_is_sorted_on"]:
            if(column in join_dict["Optimal"]["Merge Cond"]):
                left = True
                output_txt += "The left table is sorted on {}, which is part of the merge condition.\n".format(column)
        
        for column in join_dict["right_is_sorted_on"]:
            if(column in join_dict["Optimal"]["Merge Cond"]):
                right = True
                output_txt += "The right table is sorted on {}, which is part of the merge condition.\n".format(column)
        
        if(left or right):
            output_txt += "Since at least 1 of the input tables is sorted on the merge condition, {} is used.\n".format(join_dict["Optimal"]["Node Type"])
        else:
            output_txt += "Since the number of rows for both tables is large, {} is used.\n".format(join_dict["Optimal"]["Node Type"])
    
    elif("Hash Join" in join_dict["Optimal"]["Node Type"]):
        output_txt += "{} was chosen as the size of both tables is large and as both tables are unsorted.\n".format(join_dict["Optimal"]["Node Type"])

    output_txt += "The estimated cost for {} is {}.\n".format(join_dict["Optimal"]["Node Type"], join_dict["Optimal"]["Total Cost"])
   
    if(len(join_dict["aqp_data"]) != 0 and join_dict["Optimal"]["Total Cost"] >= 0):
        output_txt += "Alternate Plans:\n"
        for aqp in join_dict["aqp_data"].values():
            output_txt += "{}: {} ({:.1f} times slower)\n".format(aqp["Node Type"], aqp["Total Cost"], (aqp["Total Cost"])/(join_dict["Optimal"]["Total Cost"]))
    
    return output_txt

def get_annotations(n, data):
    """
    get_annotations: Function to get annotations, called by interface.
    Uses bfs to scan through nodes and get their annotations.
    Each node that is enccountered in the bfs has it's annotations inserted at the beginning, to get procedural annotations. 
    """
    count = n
    output_txt = []
    for key in data.keys():
        if("Scan" in data[key]["Optimal"]["Node Type"]):
            output_txt.insert(0,explain_scan(count, data[key]))
        elif("Join" in data[key]["Optimal"]["Node Type"] or "Nested Loop" in data[key]["Optimal"]["Node Type"]):
            output_txt.insert(0,explain_join(count, data[key]))
        else:
            output_txt.insert(0,state_qp_step(count, data[key]["Optimal"]))
        count -= 1
        output_txt.insert(0,"\n")
    
    first_value = list(data.values())[0]
    output_txt.insert(0,"The selected Query Plan has an estimated total cost of {}".format(first_value["Optimal"]["Total Cost"]))
    return "".join(output_txt)

# with open('config.yaml') as f:
#     config = yaml.load(f,Loader = SafeLoader)

# qm = preprocessing.Query_Manager(config["Database_Credentials"])
# q_test = "select * FROM bookings where booked_for < 4;"
# q1 = "select * FROM region WHERE r_name LIKE 'A%'"
# q2 = "select * FROM orders O, customer C WHERE O.o_custkey = C.c_custkey"
# q3 = "select l_orderkey, sum( l_extendedprice *( 1-l_discount )) as revenue, o_orderdate, o_shippriority from customer,orders,lineitem where c_mktsegment = 'HOUSEHOLD' and c_custkey = o_custkey  and l_orderkey = o_orderkey and o_orderdate < date '1995-03-21' and l_shipdate > date '1955-03-21' group by l_orderkey, o_orderdate, o_shippriority order by revenue DESC, o_orderdate limit 10";
# q4 = "select o_orderpriority,count(*) as order_count from orders where o_orderdate >= date '1996-03-01' and o_orderdate < date '1996-03-01' + INTERVAL '3' MONTH and exists ( select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate ) group by o_orderpriority order by o_orderpriority limit 1;"
# q5 = "select l_returnflag, l_linestatus,sum(l_quantity) as sum_qty,sum(l_extendedprice) as sum_base_price,sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,avg(l_quantity) as avg_qty,avg(l_extendedprice) as avg_price,avg(l_discount) as avg_disc,count(*) as count_order from lineitem where l_extendedprice > 100 group by l_returnflag,l_linestatus order by l_returnflag,l_linestatus;"
# q6 = "select l_orderkey,sum(l_extendedprice * (1 - l_discount)) as revenue,o_orderdate,o_shippriority from customer,orders,lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_totalprice > 10 and l_extendedprice > 10 group by l_orderkey, o_orderdate, o_shippriority order by revenue desc,o_orderdate;"
# q7 = "select o_orderpriority,count(*) as order_count from orders where o_totalprice > 100 and exists (select * from lineitem where l_orderkey = o_orderkey and l_extendedprice > 100) group by o_orderpriority order by o_orderpriority;"
# q8 = "select sum(l_extendedprice * l_discount) as revenue from lineitem where l_extendedprice > 100;"
# q9 = "select n_name,sum(l_extendedprice * (1 - l_discount)) as revenue from customer,orders,lineitem,supplier,nation,region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey  and r_name = 'ASIA' and o_orderdate >= '1994-01-01' and o_orderdate < '1995-01-01'and c_acctball > 10 and s_acctbal > 20 group by n_name order by revenue desc;"
# q10 = "select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#31' and p_container = 'WRAP BOX' and l_quantity < ( select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey );"
# optimal_qep_tree = qm.get_query_tree(qm.get_query_plan(q_test))
# print(qm.get_query_plan(q_test))


# queue = [optimal_qep_tree.head]
# while(len(queue) >0 ):
#     node = queue.pop(0)
#     print(node.get_aqps(config, q_test))
#     if(node.left is not None):
#         queue.append(node.left)
#     if(node.right is not None):
#         queue.append(node.right)
    



