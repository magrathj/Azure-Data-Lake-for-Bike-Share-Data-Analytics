# Databricks notebook source
data_lake_tables = [
    {'bronze_table':'bronze_payments', 'silver_table':'silver_payments'},
    {'bronze_table':'bronze_riders', 'silver_table':'silver_riders'},
    {'bronze_table':'bronze_stations', 'silver_table':'silver_stations'},
    {'bronze_table':'bronze_trips', 'silver_table':'silver_trips'}
]

data_lake_tables

# COMMAND ----------

for file in data_lake_tables:
    print(f"""DROP TABLE IF EXISTS {file['silver_table']}""")
    spark.sql(f"""DROP TABLE IF EXISTS {file['silver_table']}""")

# COMMAND ----------

# DBTITLE 1,Riders
# MAGIC %sql
# MAGIC CREATE TABLE silver_riders (
# MAGIC     rider_id VARCHAR(50), 
# MAGIC     first VARCHAR(50), 
# MAGIC     last VARCHAR(50), 
# MAGIC     address VARCHAR(100), 
# MAGIC     birthday VARCHAR(50), 
# MAGIC     account_start_date VARCHAR(50), 
# MAGIC     account_end_date VARCHAR(50), 
# MAGIC     is_member VARCHAR(50)
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Payments
# MAGIC %sql
# MAGIC CREATE TABLE silver_payments (
# MAGIC     payment_id VARCHAR(50), 
# MAGIC     date VARCHAR(50), 
# MAGIC     amount VARCHAR(50), 
# MAGIC     rider_id VARCHAR(50)
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Stations
# MAGIC %sql
# MAGIC CREATE TABLE silver_stations (
# MAGIC     station_id VARCHAR(50), 
# MAGIC     name VARCHAR(75), 
# MAGIC     latitude VARCHAR(50), 
# MAGIC     longitude VARCHAR(50)
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Trips
# MAGIC %sql
# MAGIC CREATE TABLE silver_trips (
# MAGIC     trip_id VARCHAR(50), 
# MAGIC     rideable_type VARCHAR(75), 
# MAGIC     start_at VARCHAR(50), 
# MAGIC     ended_at VARCHAR(50), 
# MAGIC     start_station_id VARCHAR(50), 
# MAGIC     end_station_id VARCHAR(50), 
# MAGIC     rider_id VARCHAR(50)
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

def create_bronze_table(source_table_name: str, target_table_name: str): 
    print(f"source table: {source_table_name}, target table: {target_table_name}")
    (
        spark.read.table(source_table_name)
        .toDF(*spark.read.table(target_table_name).columns)
        .write.format("delta")
        .mode("append")
        .saveAsTable(target_table_name)
    )

# COMMAND ----------

for file in data_lake_tables:
    create_bronze_table(file['bronze_table'], file['silver_table'])