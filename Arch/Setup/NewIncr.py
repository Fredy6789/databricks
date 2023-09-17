# Databricks notebook source
dbutils.widgets.text('env','','Environment')
dbutils.widgets.text('Incremental_source_path','','Incremental_source_path')
dbutils.widgets.text('Table_Name','',"Table Name")
env=dbutils.widgets.get('env')
Incremental_source_path=dbutils.widgets.get('Incremental_source_path')
Tablename=dbutils.widgets.get('Table_Name')
checkpointPath = "/tmp/incr/"
deltapathdummy=f"/FileStore/{env}_bronze_db/dummydelta"
print(env)
print(Tablename)
dynamic_location=f"/FileStore/{env}_bronze_db/{Tablename}"
print(dynamic_location)


# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %run /Arch/Setup/SetupDB

# COMMAND ----------

setupdb(env)

# COMMAND ----------

# MAGIC %run /Arch/Setup/IncrementalSetup $Tablename=$Tablename $dynamic_location=$dynamic_location $env=$env

# COMMAND ----------

df_schema=spark.read.format("csv").option('inferSchema',True).option('header',True).load(Incremental_source_path)
#df.display()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import lit
streamnew2=spark.readStream.schema(df_schema.schema).option("header", True).csv(Incremental_source_path)

transformed_df = (streamnew2.withColumn("TimeStamp",current_timestamp()).withColumn("Update_id",lit("ETL")))

#transformed_df.writeStream.trigger(once=True).format("delta").option("checkpointLocation", checkpointPath).start(deltaPath_dummy)

df=transformed_df.writeStream.trigger(once=True).format("delta").option("checkpointLocation", checkpointPath).option("maxFilesPerTrigger", 1).foreachBatch(process_batch).start()
print(df)


#transformed_df.writeStream.trigger(once=True).format("delta").option("checkpointLocation", checkpointPath).foreachBatch(process_batch).start(deltaPath_dummy)






# COMMAND ----------

# MAGIC %sql
# MAGIC select * from circuits

# COMMAND ----------

  #from delta.tables import *
from delta.tables import DeltaTable
from pyspark.sql.functions import expr


condition = expr('Circuits.circuitId = updates.circuitId')


deltaTableCircuits = DeltaTable.forPath(spark, '/user/hive/warehouse/circuits')
deltaTableCircuitsUpdates = DeltaTable.forPath(spark, deltapath_org)

history_df = deltaTableCircuitsUpdates.history()
print(history_df)
num_versions = history_df.count()
print(num_versions)

dfCircuitsUpdates = deltaTableCircuitsUpdates.toDF()

deltaTableCircuits.alias('Circuits') \
.merge(
dfCircuitsUpdates.alias('updates'),
condition
)\
.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
  
  

# COMMAND ----------

dbutils.fs.rm(checkpointPath,True)

# COMMAND ----------

dbutils.fs.rm(deltapathdummy,True)

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/Incr/',True)

# COMMAND ----------

dbutils.fs.rm('/FileStore/Incremnetal_bronze_db/circuits',True)
