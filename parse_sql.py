#just for grabbing sql from queries folder


from glob import glob
from os import path

sql_dir = "Z:\\TPC-H V3.0.1\\dbgen\\queries"
files = glob(path.join(sql_dir,"*.sql"))

final_out = "["
for i in files:
  output = ""
  
  with open(i,mode="r") as file:
    lines = file.readlines()
    cnt = 0
    from_table = None
    multi = False
    where = False
    for line in lines:
      cnt += 1
      if cnt == 2:
        description = line[3::].strip()
      if line.startswith('-') or line.startswith(':'):
        continue
      output = output + ' '.join(line.strip().split(' ')) + " "
      if from_table and from_table == "TEMPLATE":
        
        from_table = line.strip()
      elif from_table and from_table != "TEMPLATE" and not where:
        if not line.startswith("where"):
          multi = True
        else:
          where = True
      
      
      if line.startswith("from"):
        from_table = "TEMPLATE"
    
    final_out += "\n{ \"table\":\""
    if multi:
      final_out += "MULTIPLE TABLES\","
    else:
      final_out += f"{from_table}\","
    final_out += f"\"description\":\"{description}\","
    final_out += f"\"sql\":\"{output.strip()}\"" + "},"
final_out = final_out[:-1:]
final_out += "]"

print(final_out)