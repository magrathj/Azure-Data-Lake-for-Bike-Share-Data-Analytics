# Databricks notebook source
data_lake_tables = [
    {'silver_table':'silver_payments', 'gold_table':'gold_payments'},
    {'silver_table':'silver_riders', 'gold_table':'gold_users'},
    {'silver_table':'silver_stations', 'gold_table':'gold_stations'},
    {'silver_table':'silver_trips', 'gold_table':'gold_trips'},
    {'gold_table':'gold_date'}, 
    {'gold_table':'gold_time'},
]

data_lake_tables

# COMMAND ----------

for file in data_lake_tables:
    print(f"""DROP TABLE IF EXISTS {file['gold_table']}""")
    spark.sql(f"""DROP TABLE IF EXISTS {file['gold_table']}""")

# COMMAND ----------

# DBTITLE 1,Dates
spark.sql(f"""
CREATE TABLE Gold_Date (
    date_id INT,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    day_of_week VARCHAR(100) NOT NULL
)
USING DELTA
LOCATION '/delta/Gold_Date'
""")


# COMMAND ----------

spark.sql(f"""
CREATE TABLE Gold_Time (
    time_id INT,
    hour INT NOT NULL,
    min INT NOT NULL,
    second INT NOT NULL
)
USING DELTA
LOCATION '/delta/Gold_Time'
""")

# COMMAND ----------

# DBTITLE 1,Riders
spark.sql(f"""
CREATE TABLE gold_users (
    user_id INT,
    address VARCHAR(100) ,
    first_name VARCHAR(100), 
    second_name VARCHAR(100), 
    birth_date DATE, 
    is_member BOOLEAN,
    member_start_date_id INT,
    member_end_date_id INT
)
USING DELTA
LOCATION '/delta/gold_users'
""")

# COMMAND ----------

# DBTITLE 1,Payments
spark.sql(f"""
CREATE TABLE gold_payments (
    payment_id INT,
    user_id INT NOT NULL,
    payment_date_id INT NOT NULL,
    payment_amount FLOAT NOT NULL
)
USING DELTA
LOCATION '/delta/gold_payments'
""")

# COMMAND ----------

# DBTITLE 1,Stations
spark.sql(f"""
CREATE TABLE gold_stations (
    station_id VARCHAR(50), 
    station_name VARCHAR(75), 
    station_latitude FLOAT, 
    station_longitude FLOAT
)
USING DELTA
LOCATION '/delta/gold_stations'
""")

# COMMAND ----------

# DBTITLE 1,Trips
spark.sql(f"""
CREATE TABLE gold_trips (
    trip_id VARCHAR(100),
    trip_counter INT NOT NULL,
    start_at_time_id INT NOT NULL,
    start_at_date_id INT NOT NULL,
    end_at_time_id INT NOT NULL, 
    end_at_date_id INT NOT NULL,
    total_trip_time_seconds INT NOT NULL,
    user_id INT NOT NULL,
    rideable_type VARCHAR(100),
    start_station_id VARCHAR(50),
    end_station_id VARCHAR(50)
)
USING DELTA
LOCATION '/delta/gold_trips'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD

# COMMAND ----------

# DBTITLE 1,payments
payments_df = spark.read.table("silver_payments")
riders_df = spark.read.table("silver_riders") 

(
    payments_df.alias("p")
    .join(riders_df.alias("r"), ['rider_id'])
    .selectExpr("CAST(p.payment_id AS INT) AS payment_id", 
                "CAST(r.rider_id AS INT) AS user_id", 
                "CAST(UNIX_TIMESTAMP(CAST (p.date AS DATE)) AS INTEGER) AS payment_date_id", 
                "CAST(p.amount AS FLOAT) AS payment_amount")
    .distinct()
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("gold_payments")
)

# COMMAND ----------

# DBTITLE 1,Stations
stations_df = spark.read.table("silver_stations")

(
    stations_df.alias("s")
    .selectExpr("s.station_id AS station_id", 
                "s.name AS station_name", 
                "CAST(s.latitude AS FLOAT) AS station_latitude", 
                "CAST(s.longitude AS FLOAT) AS station_longitude")
    .distinct()
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("gold_stations")
)

# COMMAND ----------

# DBTITLE 1,Trips
trips_df = spark.read.table("silver_trips")
riders_df = spark.read.table("silver_riders") 

(
    trips_df.alias("t")
    .join(riders_df.alias("r"), ['rider_id'])
    .selectExpr(
                "t.trip_id AS trip_id",
                "1 AS trip_counter",
                "(hour(to_timestamp(CONCAT(start_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) * 3600 + minute(to_timestamp(CONCAT(start_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) * 60) +    second(to_timestamp(CONCAT(start_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) AS start_at_time_id",
                "CAST(UNIX_TIMESTAMP(to_date(CONCAT(start_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) AS INTEGER) AS start_at_date_id",
                "(hour(to_timestamp(CONCAT(ended_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) * 3600 + minute(to_timestamp(CONCAT(ended_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) * 60) +   second(to_timestamp(CONCAT(ended_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) AS end_at_time_id",
                "CAST(UNIX_TIMESTAMP(to_date(CONCAT(ended_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) AS INTEGER) AS end_at_date_id",
                "CAST(CAST(to_timestamp(CONCAT(ended_at, ':00'), 'dd/MM/yyyy HH:mm:ss') AS LONG) - CAST(to_timestamp(CONCAT(start_at, ':00'), 'dd/MM/yyyy HH:mm:ss') AS LONG) AS INTEGER) AS total_trip_time_seconds",
                "CAST(r.rider_id AS INTEGER) AS user_id",
                "t.rideable_type AS rideable_type",
                "t.start_station_id",
                "t.end_station_id"
               )
    .distinct()
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("gold_trips")
)

# COMMAND ----------

# DBTITLE 1,Create Date Dim
from pyspark.sql.functions import explode, sequence, to_date

beginDate = '2000-01-01'
endDate = '2050-12-31'

df = (
  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
)
df = df.selectExpr("CAST(UNIX_TIMESTAMP(calendarDate) AS INTEGER) AS date_id", 
                   "DAY(calendarDate) AS DAY", 
                   "MONTH(calendarDate) AS MONTH", 
                   "YEAR(calendarDate) AS YEAR", 
                   "CAST(DAYOFWEEK(calendarDate) AS STRING) AS day_of_week")
df.write.format("delta").mode("overwrite").saveAsTable("gold_date")

# COMMAND ----------

# DBTITLE 1,Time Dim
from pyspark.sql.functions import explode, sequence, to_date
from datetime import datetime

beginDate = datetime(2022, 9, 25, 20, 6, 58)
endDate = datetime(2022, 9, 26, 20, 6, 58)

df = (
  spark.sql(f"select explode(sequence(to_timestamp('{beginDate}'), to_timestamp('{endDate}'), interval 1 second)) as calendarDate")
)

df = df.selectExpr("""hour(calendarDate) * 3600 + minute(calendarDate) * 60 + second(calendarDate) AS time_id""",
                   "hour(calendarDate) AS hour",
                   "minute(calendarDate) AS min",
                   "second(calendarDate) AS second"
                  )
df.write.format("delta").mode("overwrite").saveAsTable("gold_time")

# COMMAND ----------

