# Databricks notebook source
dbutils.fs.ls("dbfs:/tmp/datalake")

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /dbfs/tmp/datalake/payments.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC cp payments.csv /dbfs/tmp/datalake/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /dbfs/tmp/datalake/riders.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC cp riders.csv /dbfs/tmp/datalake/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /dbfs/tmp/datalake/stations.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC cp stations.csv /dbfs/tmp/datalake/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip /dbfs/tmp/datalake/trips.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC cp trips.csv /dbfs/tmp/datalake/

# COMMAND ----------

dbutils.fs.ls(path)

# COMMAND ----------

