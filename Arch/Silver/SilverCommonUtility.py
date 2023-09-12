# Databricks notebook source
def silvercommon(table_name:str,env:str):
    spark.sql(f"""USE DATABASE {env}""")
    df=spark.sql(f"""select * from {table_name}""")
    import pyspark.sql.functions as F
    cols = df.schema.names
    dq=[]
    for i in range(len(cols)):
        if cols.count(cols[i])>1:
            df2.schema[i].name = cols[i]+'_tobeDropped'
            cols[i] = cols[i]+'_tobeDropped'
            droped=cols[i]+'_tobeDropped'
            dq.append(droped)
    df2 = spark.createDataFrame([], df.schema).union(df) 
    for i in range(len(dq)):
        df2=df2.drop(dq[i])
        print(f"Duplicate Column{dq[i]}has been dropped")
    df2.show()     

#df=df.dropna()
#df.count()
#df.fillna(0)

# COMMAND ----------

def showdatabases():
    return(spark.sql("show databases").display())


# COMMAND ----------

def showtables(env:str):
    spark.sql(f"""USE DATABASE {env}""").display()
    return(spark.sql("show tables").display())
