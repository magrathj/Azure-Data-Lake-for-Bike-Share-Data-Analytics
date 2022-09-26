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
spark.sql(f"""
    CREATE TABLE {data_lake_tables[1]['silver_table']} (
        rider_id VARCHAR(50), 
        first VARCHAR(50), 
        last VARCHAR(50), 
        address VARCHAR(100), 
        birthday VARCHAR(50), 
        account_start_date VARCHAR(50), 
        account_end_date VARCHAR(50), 
        is_member VARCHAR(50)
    )
    USING DELTA
    LOCATION '/delta/{data_lake_tables[1]['silver_table']}'
""")

# COMMAND ----------

# DBTITLE 1,Payments
spark.sql(f"""
    CREATE TABLE {data_lake_tables[0]['silver_table']} (
        payment_id VARCHAR(50), 
        date VARCHAR(50), 
        amount VARCHAR(50), 
        rider_id VARCHAR(50)
    )
    USING DELTA
    LOCATION '/delta/{data_lake_tables[0]['silver_table']}'
""")

# COMMAND ----------

# DBTITLE 1,Stations
spark.sql(f"""
    CREATE TABLE {data_lake_tables[2]['silver_table']} (
        station_id VARCHAR(50), 
        name VARCHAR(75), 
        latitude VARCHAR(50), 
        longitude VARCHAR(50)
    )
    USING DELTA
    LOCATION '/delta/{data_lake_tables[2]['silver_table']}'
""")

# COMMAND ----------

# DBTITLE 1,Trips
spark.sql(f"""
    CREATE TABLE {data_lake_tables[3]['silver_table']} (
        trip_id VARCHAR(50), 
        rideable_type VARCHAR(75), 
        start_at VARCHAR(50), 
        ended_at VARCHAR(50), 
        start_station_id VARCHAR(50), 
        end_station_id VARCHAR(50), 
        rider_id VARCHAR(50)
    )
    USING DELTA
    LOCATION '/delta/{data_lake_tables[3]['silver_table']}'
""")

# COMMAND ----------

def create_bronze_table(source_table_location: str, target_table_name: str): 
    print(f"source table: /delta/{source_table_location}, target table: {target_table_name}")
    (
        spark.read.format("delta").load(f"/delta/{source_table_location}")
        .toDF(*spark.read.table(target_table_name).columns)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(target_table_name)
    )

# COMMAND ----------

for file in data_lake_tables:
    create_bronze_table(file['bronze_table'], file['silver_table'])