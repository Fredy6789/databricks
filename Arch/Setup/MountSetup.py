# Databricks notebook source
dbutils.widgets.text("Project","","Project Name")
dbutils.widgets.text("Container","","Container Name")
dbutils.widgets.text("Storage_Account","","Storage Account Name")
Project=dbutils.widgets.get("Project")
Container=dbutils.widgets.get("Container")
Storage_Account=dbutils.widgets.get("Storage_Account")



# COMMAND ----------

# MAGIC %run /Arch/Setup/MountADLSGen2Setup $Project=$Project $Container=$Container $Storage_Account=$Storage_Account
# MAGIC
