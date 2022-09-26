# Databricks notebook source
dbutils.fs.ls("dbfs:/tmp/datalake")

# COMMAND ----------

data_lake_files = [
    {'path':'dbfs:/tmp/datalake/payments.csv', 'table':'bronze_payments', 'columns': ['payment_id', 'date', 'amount', 'rider_id']},
    {'path':'dbfs:/tmp/datalake/riders.csv', 'table':'bronze_riders', 'columns': ['rider_id', 'first', 'last', 'address', 'birthday', 'account_start_date', 'account_end_date', 'is_member']},
    {'path':'dbfs:/tmp/datalake/stations.csv', 'table':'bronze_stations', 'columns': ['station_id', 'name', 'longitude', 'latitude']},
    {'path':'dbfs:/tmp/datalake/trips.csv', 'table':'bronze_trips', 'columns': ['trip_id', 'rideable_type', 'start_at', 'ended_at', 'start_station_id', 'end_station_id', 'rider_id']}
]

data_lake_files

# COMMAND ----------

for file in data_lake_files:
    print(f"""DROP TABLE IF EXISTS {file['table']}""")
    spark.sql(f"""DROP TABLE IF EXISTS {file['table']}""")

# COMMAND ----------

def create_bronze_table(table_name: str, file_location: str, columns: list): 
    print(f"creating table at location: /delta/{table_name} from file at {file_location}")
    (
     spark.read
        .option("inferSchema", "false")
        .option("header", "false")
        .option("sep", ",")
        .csv(file_location) 
        .toDF(*columns)
        .write 
        .format("delta") 
        .mode("overwrite") 
        .save(f"/delta/{table_name}")
    )

# COMMAND ----------

for file in data_lake_files:
    create_bronze_table(file['table'], file['path'], file['columns'])