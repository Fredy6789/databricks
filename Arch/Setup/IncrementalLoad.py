# Databricks notebook source
# Import functions
from pyspark.sql.functions import col, current_timestamp


file_path = "/FileStore/tables/Incr/"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = "circuits"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"


spark.sql(f"DROP TABLE IF EXISTS {table_name}")
#dbutils.fs.rm(checkpoint_path, True)


df=spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", checkpoint_path).load(file_path)

df.writeStream.option("checkpointLocation", checkpoint_path).trigger(once=True).toTable(table_name)

# COMMAND ----------

dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from circuits;

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/Incr')

# COMMAND ----------

dbutils.fs.rm('/user/hive/warehouse/alfredroy99_gmail_com_atlas_higgs',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended alfredroy99_gmail_com_atlas_higgs

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended formulaone_bronze_db.constructors

# COMMAND ----------

#df=spark.read.format("csv").option(header,True).load('/databricks-datasets/atlas_higgs/atlas_higgs.csv').display()

raw_df = spark.read.csv('/databricks-datasets/atlas_higgs/atlas_higgs.csv',header=True,inferSchema=True).display()

# COMMAND ----------

dbutils.fs.rm("/user/hive/warehouse",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.alfredroy99_gmail_com_atlas_higgs

# COMMAND ----------

dbutils.fs.put('/databricks-datasets/atlas_higgs/atlas_higgs2.csv','/databricks-datasets/atlas_higgs/atlas_higgs.csv')
