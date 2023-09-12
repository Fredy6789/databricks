# Databricks notebook source
dbutils.widgets.text('env','','Environment')
env=dbutils.widgets.get('env')
dbutils.widgets.text('table','','TableName')
table_name=dbutils.widgets.get('table')

# COMMAND ----------


spark.sql(f"""USE DATABASE {env}""")
tables_df=spark.sql("show tables")


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE ${table}

# COMMAND ----------

#To Drop all tables in a database in a single only use when required.
tables_df=spark.sql("show tables")

table_names = [row.tableName for row in tables_df.collect()]

for table_name in table_names:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    

# COMMAND ----------

# MAGIC %md
# MAGIC To Drop the whole database, make sure the the tables are dropped individully before running the below command.

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC DROP database ${env}
