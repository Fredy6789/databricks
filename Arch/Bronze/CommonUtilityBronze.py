# Databricks notebook source
def loaddata (file_name:str,env:str,file_path:str,file_format:str) ->None:
    files=dbutils.fs.ls(file_path)
    dit={}
    a=0
    for f in files:
        dit[f.name]=a
        a=a+1
    if file_format!="parquet":
        try:
            raw_file_path = file_path+"/"+file_name
            table_name=file_name.split(".")
            name=table_name[0]
            file_format=table_name[1]
            if dit[file_name]:    
                raw_file_path = file_path+"/"+file_name
                table_name=file_name.split(".")
                name=table_name[0]
                file_format=table_name[1]
            if file_format=="csv":
                raw_df=csvload(name,env,raw_file_path)
                raw_df.display()
                dynamic_location='/FileStore/'+env+'_bronze_db/'+name
            if file_format=="json":
                raw_df=jsonload(name,env,raw_file_path)
                dynamic_location='/FileStore/'+env+'_bronze_db/'+name
            if file_format=="parquet":
                raw_df=parquetload(name,env,raw_file_path)
                dynamic_location='/FileStore/'+env+'_bronze_db/'+name
            new_cols=[]
            d={}
            for i in raw_df.columns:
                k=[" ","/","\t",",",";","{","}","(",")","\n","="]
                for f in range(len(k)):
                    if k[f] in i:
                        s=i
                        i=i.replace(k[f],"_")
                        new_cols.append(i)
                        d[s]=i
            for key, value in d.items():
                raw_df=raw_df.withColumnRenamed(key,value)
                raw_df_c=raw_df
            if len(new_cols)>=1:
                spark.sql(f"""USE DATABASE {env}_bronze_db""")
                raw_df_c.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("database",env+"_bronze_db").saveAsTable(name,path=dynamic_location)
                print(f"Bronze Table for {file_name} created")
            else:
                spark.sql(f"""USE DATABASE {env}_bronze_db""")
                raw_df_c=raw_df
                raw_df_c.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("database",env+"_bronze_db").saveAsTable(name,path=dynamic_location)
                print(f"Bronze Table for {file_name} created")
            df=spark.sql(f"""select * from {name} limit 10""" )
        except KeyError:
            print("File Not Found")
    if file_format=="parquet":
        name=file_name
        env=env
        raw_file_path = file_path
        raw_df=parquetload(name,env,raw_file_path)


# COMMAND ----------

def csvload(name:str,env:str,raw_file_path:str):
    dynamic_location='/FileStore/'+env+'_bronze_db/'+name
    raw_df = spark.read.csv(raw_file_path,header=True,inferSchema=True)
    return(raw_df)

# COMMAND ----------

def textload():
    dynamic_location='/FileStore/'+env+'_bronze_db/'+name
    raw_df = spark.read.format("text").load(raw_file_path)
    return(raw_df)

# COMMAND ----------

def jsonload(name:str,env:str,raw_file_path:str):
    import pyspark.sql.utils
    try:
        dynamic_location='/FileStore/'+env+'_bronze_db/'+name
        raw_df = spark.read.format("json").load(raw_file_path)
        raw_df.display(1)
        return(raw_df)
    except pyspark.sql.utils.AnalysisException:
        raw_df = spark.read.json(raw_file_path, multiLine=True)
        return(raw_df)



# COMMAND ----------

def textload():
    dynamic_location='/FileStore/'+env+'_bronze_db/'+name
    raw_df = spark.read.format("avro").load(raw_file_path)

# COMMAND ----------

def parquetload(name:str,env:str,raw_file_path:str):
    dynamic_location='/FileStore/'+env+'_bronze_db/'+name
    raw_file_path='/FileStore/tables/airportclean.parquet/'
    file_name=name
    raw_df=spark.read.parquet(raw_file_path)
    raw_df.display()
    new_cols=[]
    d={}
    for i in raw_df.columns:
                k=[" ","/","\t",",",";","{","}","(",")","\n","="]
                for f in range(len(k)):
                    if k[f] in i:
                        s=i.replace(k[f],"_")
                        new_cols.append(s)
                        d[i]=s
    for key, value in d.items():
        raw_df=raw_df.withColumnRenamed(key,value)
        raw_df_c=raw_df
    if len(new_cols)>=1:
        spark.sql(f"""USE DATABASE {env}_bronze_db""")
        raw_df_c.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("database",env+"_bronze_db").saveAsTable(name,path=dynamic_location)
        print(f"Bronze Table for {file_name} created")
    else:
        spark.sql(f"""USE DATABASE {env}_bronze_db""")
        raw_df_c=raw_df
        raw_df_c.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("database",env+"_bronze_db").saveAsTable(name,path=dynamic_location)
        print(f"Bronze Table for {file_name} created")
