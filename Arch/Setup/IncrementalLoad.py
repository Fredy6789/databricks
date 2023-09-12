# Databricks notebook source
# Import functions
from pyspark.sql.functions import col, current_timestamp


file_path = "/databricks-datasets/atlas_higgs/"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_atlas_higgs"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"


spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/atlas_higgs")

# COMMAND ----------

dbutils.fs.rm("/user/hive/warehouse",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.alfredroy99_gmail_com_atlas_higgs

# COMMAND ----------

dbutils.fs.put('/databricks-datasets/atlas_higgs/atlas_higgs2.csv','/databricks-datasets/atlas_higgs/atlas_higgs.csv')
