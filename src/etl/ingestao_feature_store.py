# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import datetime
from tqdm import tqdm

def import_query(path): #importando a query
    with open (path,'r') as open_file: #usando o read para ler a query
        return open_file.read()
    
def table_exists(database, table):
    count = (spark.sql(f"SHOW TABLES FROM {database}")
        .filter (f"tableName = '{table}'")
        .count())
    return count > 0

def date_range(dt_start, dt_stop, period='daily'):
    datetime_start= datetime.datetime.strptime(dt_start, '%Y-%m-%d')
    datetime_stop= datetime.datetime.strptime(dt_stop, '%Y-%m-%d')
    dates = []
    
    while datetime_start <= datetime_stop:
        dates.append(datetime_start.strftime('%Y-%m-%d'))
        datetime_start += datetime.timedelta(days=1)
    if period == 'daily':
        return dates
    elif period == 'monthly':
        return[i for i in date if i.endswith("01")]  

table = dbutils.widgets.get("table")
table = "vendas"    
table_name = f"fs_vendedor_{table}"
database = "silver.analytics"
period = dbuilts.widgets.get("period")

query = import_query(f"{table}.sql")

date_start =dbutils.widgets.get ("date_start")
date_stop = dbutils.widgets.get ("date_stop")
date = date_range(date_start,date_stop)

print(table_name,table_exists(database,table_name))
print(date_start,date_stop)

# COMMAND ----------

df = spark.sql(query.format(date =dates.pop(0)))

# COMMAND ----------

#coalesce traz o dado para um único nó
if not table_exists(database,table_name):
    print("Criando a tabela")
    (spark.sql(query.format(date=dates.pop(0))
         .coalesce(1)
         .write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema","true")
         .partitionBy("dtReference")
         .saveAsTable(f"{database}.{table_name}")
    )
    print("OK")
     
else:
    print("Realizando update")
    for i in dates:
        spark.sql(f"DELETE FROM {database}.{table_name} WHERE dtReference = '{i}'")
        (spark.sql(query.format(date=i))
             .coalesce(1)
             .write
             .format("delta")
             .mode("append")
             .option("overwriteSchema","true")
             .saveAsTable(f"{database}.{table_name}"))
        print("OK")
        

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(distinct dtReference) FROM silver.analytics.fs_vendedor_vendas
