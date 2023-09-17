# Databricks notebook source
dbutils.widgets.text("deltapathdummy","","Dummy Path")
dbutils.widgets.text("Tablename","","Table Name")
dbutils.widgets.text("env","","Environment")
dynamic_location=dbutils.widgets.get("deltapathdummy")
Tablename=dbutils.widgets.get("Tablename")
env=dbutils.widgets.get("env")
#dynamic_location=f"/FileStore/{env}_bronze_db/{Tablename}"
print(Tablename)
print(env)
print(dynamic_location)

# COMMAND ----------

def process_batch(df,batch_id):
    deltapathdummy=f"/FileStore/{env}_bronze_db/dummydelta"
    dynamic_location=f"/FileStore/{env}_bronze_db/{Tablename}"
    deduplicated_df = df.dropDuplicates(["circuitId"])  # Replace with your column names
    deduplicated_df.write.format("delta").mode("overwrite").save(deltapathdummy)
    if not spark.catalog.tableExists("circuits"):
        df=spark.read.format("delta").load(deltapathdummy)
        spark.sql(f"""USE DATABASE {env}_bronze_db""")
        df.write.format("delta").mode("overwrite").option("inferSchema",True).option("mergeSchema", "true").saveAsTable("circuits",path=dynamic_location)
    MergeLoader(deltapathdummy,Tablename)

# COMMAND ----------

def MergeLoader(deltapathdummy:str,Tablename:str):
    #from delta.tables import *
    from delta.tables import DeltaTable
    from pyspark.sql.functions import expr
    condition = expr('Circuits.circuitId = updates.circuitId')
    deltaTableCircuits = DeltaTable.forPath(spark, f"/FileStore/{env}_bronze_db/{Tablename}")
    deltaTableCircuitsUpdates = DeltaTable.forPath(spark, deltapathdummy)
    dfCircuitsUpdates = deltaTableCircuitsUpdates.toDF()

    deltaTableCircuits.alias('Circuits') \
     .merge(
    dfCircuitsUpdates.alias('updates'),
    condition
    )\
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
  
  


  
