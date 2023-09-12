# Databricks notebook source
# MAGIC %run /Arch/Silver/SilverCommonUtility 

# COMMAND ----------

showdatabases()

# COMMAND ----------

dbutils.widgets.text('env','','Environment')
env=dbutils.widgets.get('env')
dbutils.widgets.text('table','','TableName')
table_name=dbutils.widgets.get('table')

# COMMAND ----------

showtables(env)

# COMMAND ----------

silvercommon(table_name,env)

# COMMAND ----------

df=spark.sql("select * from indian_small_cardamom_price_history").display()
df2=df.groupBy('Date_of_Auction')#.sum('Total_Qty_Arrived__Kgs_').sum('Qty_Sold__Kgs_').avg('Avg.Price__Rs._Kg_')

# COMMAND ----------

# MAGIC %sql
# MAGIC select Date_of_Auction,sum(Total_Qty_Arrived__Kgs_) as total_wieght,sum(Qty_Sold__Kgs_) as total_sale from indian_small_cardamom_price_history group by Date_of_Auction order by Date_of_Auction asc;

# COMMAND ----------

df2=spark.sql("select * from circuits").display()

# COMMAND ----------



# COMMAND ----------

df=spark.read.format("delta").table("circuits")
env="formulaone_bronze_db"
name="partition"
dynamic_location='/FileStore/'+env+'_bronze_db/'+name
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("database",env).partitionBy("country").saveAsTable(name,path=dynamic_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions  partition;

# COMMAND ----------

# MAGIC %sql
# MAGIC select r.*,c.circuitRef from races r left outer join circuits c on r.circuitId=c.circuitId

# COMMAND ----------

dbutils.fs.ls('/FileStore/formulaone_bronze_db_bronze_db/partition')

# COMMAND ----------


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("sparkbyexamples.com").getOrCreate()

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)


# COMMAND ----------

empDF.join(deptDF,empDF("emp_dept_id") ==  deptDF("dept_id"),"inner").show(false)
