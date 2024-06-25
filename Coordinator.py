# Databricks notebook source
dbutils.widgets.multiselect("metric", "CpuPercentage", ["CpuPercentage", "MemoryPercentage","DiskQueueLength"],"metric")
dbutils.widgets.multiselect("metric_name", "CPU Percentage", ["CPU Percentage", "Memory Percentage","Disk Queue Length"],"metric_name")
dbutils.widgets.multiselect("prefix", "CPU", ["CPU", "MEMORY","DISK"],"prefix")

dbutils.widgets.multiselect("bronze_table", "BRONZE_CPU_PERCENTAGE",
                         ["BRONZE_CPU_PERCENTAGE", "BRONZE_MEMORY_PERCENTAGE","BRONZE_DISK_PERCENTAGE"], "bronze_table")
bronze_table= dbutils.widgets.get("bronze_table")
print(bronze_table)

dbutils.widgets.multiselect("silver_table", "SILVER_CPU_PERCENTAGE",
                         ["SILVER_CPU_PERCENTAGE", "SILVER_MEMORY_PERCENTAGE","SILVER_DISK_PERCENTAGE"], "silver_table")
silver_table= dbutils.widgets.get("silver_table")
print(silver_table)


# COMMAND ----------

class NotebookData:
  def __init__(self, path, params):
    self.path = path
    self.params = params

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

metric= dbutils.widgets.get("metric")
metric_name= dbutils.widgets.get("metric_name")
prefix= dbutils.widgets.get("prefix")

metrics = metric.split(',')
metric_names = metric_name.split(',')
prefixes = prefix.split(',')
bronze_tables = bronze_table.split(',')
silver_tables = silver_table.split(',')
result = zip(metrics,metric_names,prefixes,bronze_tables,silver_tables)
my_list = []
my_list2=[]
for i in result:
  print(i[0],i[1],i[2],i[3],i[4])
  my_list.append(NotebookData("./KPI_extract",{"metric": i[0],"metric_name":i[1],"prefix":i[2]}))
  my_list2.append(NotebookData("./KPI_transform",{"prefix":i[2],"bronze_table":i[3],"silver_table":i[4]}))


# print(my_list)
def submitNotebook(notebookt):
    print("Running notebook %s" % notebookt.path)
    print("Running notebook %s" % notebookt.params)
    try:
      dbutils.notebook.run(notebookt.path,3600,notebookt.params)
    except Exception as e:
      print('exception is ',e)

numInParallel=len(metrics)


with ThreadPoolExecutor(max_workers=numInParallel) as ec:
    for notebook in my_list:
       ec.submit(submitNotebook, notebook) 
    for notebook2 in my_list2:
      ec.submit(submitNotebook, notebook2) 



# COMMAND ----------

# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebook-workflows
