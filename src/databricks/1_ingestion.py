# Databricks notebook source
dbutils.fs.ls("dbfs:/tmp/datalake")

# COMMAND ----------

data_lake_files = [
    {'path':'dbfs:/tmp/datalake/payments.csv', 'table':'bronze_payments'},
    {'path':'dbfs:/tmp/datalake/riders.csv', 'table':'bronze_riders'},
    {'path':'dbfs:/tmp/datalake/stations.csv', 'table':'bronze_stations'},
    {'path':'dbfs:/tmp/datalake/trips.csv', 'table':'bronze_trips'}
]

data_lake_files

# COMMAND ----------

def create_bronze_table(table_name: str, file_location: str): 
    print(f"creating table {table_name} from file at {file_location}")
    (spark.read.format("csv")
     .load(file_location)
     .write.format("delta")
     .saveAsTable(table_name)
    )

# COMMAND ----------

for file in data_lake_files:
    create_bronze_table(file['table'], file['path'])