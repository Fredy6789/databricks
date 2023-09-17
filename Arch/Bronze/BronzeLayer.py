# Databricks notebook source
# MAGIC %md
# MAGIC Set in above variables as required. NB: File Format is not mandatory for non-parquet files, for parquet files include file format along with absolute file path.
# MAGIC
# MAGIC Please choose Environment as required, functionality will create database accordingly.

# COMMAND ----------

dbutils.widgets.text('env','','Environment')
dbutils.widgets.text('Filename','','Filename')
dbutils.widgets.text('Filepath','','Filepath')
dbutils.widgets.text('FileFormat','',"File Format")
env=dbutils.widgets.get('env')
file_name=dbutils.widgets.get('Filename')
file_path=dbutils.widgets.get('Filepath')
file_format=dbutils.widgets.get('FileFormat')
print(env)
print(file_name)


# COMMAND ----------

# MAGIC %run /Arch/Bronze/CommonUtilityBronze $env=env 

# COMMAND ----------

# MAGIC %run /Arch/Setup/SetupDB $env=env
# MAGIC

# COMMAND ----------

setupdb(env)

# COMMAND ----------

loaddata(file_name,env,file_path,file_format)

# COMMAND ----------



# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

#dbutils.fs.put('/FileStore/tables/Incr/Circuits1.csv','/FileStore/tables/Circuits1.csv')
dbutils.fs.rm('/FileStore/tables/Incr/Circuits1.csv',True)

# COMMAND ----------

dbutils.fs.ls('/FileStore/Cardamom_bronze_db/Indian_Small_Cardamom_Price_History')

# COMMAND ----------


