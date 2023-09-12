# Databricks notebook source
def setupdb (env:str) -> None:
    try:
        spark.sql(
                f"""CREATE database if not exists {env}_bronze_db
                LOCATION '/FileStore/{env}_bronze_db'
            """
            )
        print("Database "+env+"_bronze_db has been setup")
    except:
        print('Database Creation Failed')

    
