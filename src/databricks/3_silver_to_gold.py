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
# MAGIC %sql
# MAGIC CREATE TABLE Gold_Date (
# MAGIC     date_id INT,
# MAGIC     day INT NOT NULL,
# MAGIC     month INT NOT NULL,
# MAGIC     year INT NOT NULL,
# MAGIC     day_of_week VARCHAR(100) NOT NULL
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Gold_Time (
# MAGIC     time_id INT,
# MAGIC     hour INT NOT NULL,
# MAGIC     min INT NOT NULL,
# MAGIC     second INT NOT NULL
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Riders
# MAGIC %sql
# MAGIC CREATE TABLE gold_users (
# MAGIC     user_id INT,
# MAGIC     address VARCHAR(100) ,
# MAGIC     first_name VARCHAR(100), 
# MAGIC     second_name VARCHAR(100), 
# MAGIC     birth_date DATE, 
# MAGIC     is_member BOOLEAN,
# MAGIC     member_start_date_id INT,
# MAGIC     member_end_date_id INT
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Payments
# MAGIC %sql
# MAGIC CREATE TABLE gold_payments (
# MAGIC     payment_id INT,
# MAGIC     user_id INT NOT NULL,
# MAGIC     payment_date_id INT NOT NULL,
# MAGIC     payment_amount FLOAT NOT NULL
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Stations
# MAGIC %sql
# MAGIC CREATE TABLE gold_stations (
# MAGIC     station_id VARCHAR(50), 
# MAGIC     station_name VARCHAR(75), 
# MAGIC     station_latitude FLOAT, 
# MAGIC     station_longitude FLOAT
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Trips
# MAGIC %sql
# MAGIC CREATE TABLE gold_trips (
# MAGIC     trip_id VARCHAR(100),
# MAGIC     trip_counter INT NOT NULL,
# MAGIC     start_at_time_id INT NOT NULL,
# MAGIC     start_at_date_id INT NOT NULL,
# MAGIC     end_at_time_id INT NOT NULL, 
# MAGIC     end_at_date_id INT NOT NULL,
# MAGIC     total_trip_time_seconds INT NOT NULL,
# MAGIC     user_id INT NOT NULL,
# MAGIC     rideable_type VARCHAR(100),
# MAGIC     start_station_id VARCHAR(50),
# MAGIC     end_station_id VARCHAR(50)
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD

# COMMAND ----------

# DBTITLE 1,Create Payments Fact
# MAGIC %sql
# MAGIC INSERT INTO gold_payments
# MAGIC SELECT DISTINCT
# MAGIC         CAST(p.payment_id AS INT),
# MAGIC         CAST(r.rider_id AS INT), 
# MAGIC        UNIX_TIMESTAMP(CAST (p.date AS DATE)),
# MAGIC        CAST(p.amount AS FLOAT)
# MAGIC FROM silver_payments p
# MAGIC JOIN silver_riders r 
# MAGIC ON r.rider_id = p.rider_id

# COMMAND ----------

# DBTITLE 1,Create Station Dim
# MAGIC %sql
# MAGIC INSERT INTO gold_stations
# MAGIC SELECT DISTINCT
# MAGIC     s.station_id,
# MAGIC     s.name,
# MAGIC     s.latitude,
# MAGIC     s.longitude
# MAGIC FROM silver_stations s

# COMMAND ----------

# DBTITLE 1,Create Trip Fact
# MAGIC %sql
# MAGIC INSERT INTO gold_trips
# MAGIC SELECT DISTINCT
# MAGIC     t.trip_id,
# MAGIC     1,
# MAGIC     (
# MAGIC       hour(to_timestamp(CONCAT(start_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) * 3600 + 
# MAGIC       minute(to_timestamp(CONCAT(start_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) * 60) + 
# MAGIC       second(to_timestamp(CONCAT(start_at, ':00'), 'dd/MM/yyyy HH:mm:ss')
# MAGIC     ),
# MAGIC     UNIX_TIMESTAMP(to_date(CONCAT(start_at, ':00'), 'dd/MM/yyyy HH:mm:ss')),
# MAGIC     (
# MAGIC       hour(to_timestamp(CONCAT(ended_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) * 3600 + 
# MAGIC       minute(to_timestamp(CONCAT(ended_at, ':00'), 'dd/MM/yyyy HH:mm:ss')) * 60) + 
# MAGIC       second(to_timestamp(CONCAT(ended_at, ':00'), 'dd/MM/yyyy HH:mm:ss')
# MAGIC     ),
# MAGIC     UNIX_TIMESTAMP(to_date(CONCAT(ended_at, ':00'), 'dd/MM/yyyy HH:mm:ss')),
# MAGIC     CAST(to_timestamp(CONCAT(ended_at, ':00'), 'dd/MM/yyyy HH:mm:ss') AS LONG) - CAST(to_timestamp(CONCAT(start_at, ':00'), 'dd/MM/yyyy HH:mm:ss') AS LONG),
# MAGIC     r.rider_id,
# MAGIC     t.rideable_type,
# MAGIC     t.start_station_id,
# MAGIC     t.end_station_id
# MAGIC FROM silver_trips t
# MAGIC JOIN silver_riders r 
# MAGIC ON t.rider_id = r.rider_id

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
df.write.format("delta").mode("append").saveAsTable("gold_date")

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
df.write.format("delta").mode("append").saveAsTable("gold_time")

# COMMAND ----------

