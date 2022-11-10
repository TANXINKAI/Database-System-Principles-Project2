import yaml
import preprocessing
import re
import yaml
from yaml.loader import SafeLoader

def state_qp_step(count,optimal_data):
    output_txt = ""
    filter_conditions = []
    for key in optimal_data.keys():
        if(key.endswith("Cond")):
            clauses = re.findall('\({1,}(.*?)\)', optimal_data[key])
            if clauses:
                filter_conditions += clauses
        elif(key == "Filter"):
            clauses = re.findall('\({1,}(.*?)\)', optimal_data[key])
            if clauses:
                filter_conditions += clauses

    if("Scan" in optimal_data["Node Type"]):
        if(len(filter_conditions) > 0):
            output_txt += "{}) A {} is performed on {} with the filter(s) {}.\n".format(count, optimal_data['Node Type'], optimal_data["Relation Name"], filter_conditions)
        else:
            output_txt += "{}) A {} is performed on {}.\n".format(count, optimal_data['Node Type'], optimal_data["Relation Name"])
    else:
        if(len(filter_conditions) > 0):
            output_txt += "{}) A {} is performed with the filter(s) {}.\n".format(count, optimal_data['Node Type'], filter_conditions)
        else:
            output_txt += "{}) A {} is performed.\n".format(count, optimal_data['Node Type'])
    
    return output_txt

def explain_scan(count, scan_dict):
    output_txt = ""
    output_txt = output_txt + state_qp_step(count, scan_dict["Optimal"])
    col_not_index = []
    col_is_index = []
    for column in scan_dict["columns_used"]:
        if(column in scan_dict["indexes"]):
            col_is_index.append(column)
        else:
            col_not_index.append(column)

    if(len(col_is_index) == 0 and len(col_not_index) == 0):
        output_txt += "There are no filters for this scan.\n"
    elif(len(col_is_index) > 0):
        output_txt += "The table ({}) is indexed over some/all of the columns that are used to filter the rows ({}).\n".format(scan_dict["Optimal"]["Relation Name"], col_is_index)
    elif((len(col_is_index) == 0) and len(col_not_index) > 0):
        output_txt += "The table ({}) is not indexed over any of the columns that are used to filter the rows ({}).\n".format(scan_dict["Optimal"]["Relation Name"], col_not_index)
    
    if(scan_dict["selectivity"] <= 0.25):
        output_txt += "The estimated selectivity for this scan is {}, which means that the filters used in the scan are highly selective.\n".format(scan_dict["selectivity"])
    elif(scan_dict["selectivity"] < 1):
        output_txt += "The estimated selectivity for this scan is {}, which means that the filters used in the scan are not as selective.\n".format(scan_dict["selectivity"])
    else:
        output_txt += "The estimated selectivity for this scan is {}, which means that all the rows have been selected.\n".format(scan_dict["selectivity"])
    
    output_txt += "Based on the selectivity and/or the filter columns, {} was used.\n".format(scan_dict["Optimal"]["Node Type"])

    output_txt += "The estimated cost for {} is {}.\n".format(scan_dict["Optimal"]["Node Type"], scan_dict["Optimal"]["Startup Cost"]+ scan_dict["Optimal"]["Total Cost"])

    if(len(scan_dict["aqp_data"]) != 0):
        output_txt += "Alternate Plans:\n"
        for aqp in scan_dict["aqp_data"].values():
            output_txt += "{}: {} ({:.1f} times slower)\n".format(aqp["Node Type"], aqp["Startup Cost"]+aqp["Total Cost"], (aqp["Startup Cost"]+ aqp["Total Cost"])/(scan_dict["Optimal"]["Startup Cost"]+ scan_dict["Optimal"]["Total Cost"]))

    return output_txt

def explain_join(count, join_dict):
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

    output_txt += "The estimated cost for {} is {}.\n".format(join_dict["Optimal"]["Node Type"], join_dict["Optimal"]["Startup Cost"]+ join_dict["Optimal"]["Total Cost"])

    if(len(join_dict["aqp_data"]) != 0):
        output_txt += "Alternate Plans:\n"
        for aqp in join_dict["aqp_data"].values():
            output_txt += "{}: {} ({:.1f} times slower)\n".format(aqp["Node Type"], aqp["Startup Cost"]+aqp["Total Cost"], (aqp["Startup Cost"]+ aqp["Total Cost"])/(join_dict["Optimal"]["Startup Cost"]+ join_dict["Optimal"]["Total Cost"]))
    
    return output_txt

def get_annotations(n, data):
    count = n
    output_txt = []
    for key in data.keys():
        if("Scan" in data[key]["Optimal"]["Node Type"]):
            output_txt.insert(0,explain_scan(count, data[key]))
        elif("Join" in data[key]["Optimal"]["Node Type"]):
            output_txt.insert(0,explain_join(count, data[key]))
        else:
            output_txt.insert(0,state_qp_step(count, data[key]["Optimal"]))
        count -= 1
        output_txt.insert(0,"\n")
    return "".join(output_txt)

# with open('config.yaml') as f:
#     config = yaml.load(f,Loader = SafeLoader)

# qm = preprocessing.Query_Manager(config["Database_Credentials"])
# q1 = "select * FROM region WHERE r_name LIKE 'A%'"
# q2 = "select * FROM orders O, customer C WHERE O.o_custkey = C.c_custkey"
# optimal_qep_tree = qm.get_query_tree(qm.get_query_plan(q2))
# data = preprocessing.post_order_wrap(optimal_qep_tree.head, config, q2)
# print(get_annotations(data))

