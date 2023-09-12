# Databricks notebook source
dbutils.widgets.text("Project","","Project")
dbutils.widgets.text("Container","","Container Name")
dbutils.widgets.text("Storage_Account","","Storage Account Name")
Container=dbutils.widgets.get("Container")
Project=dbutils.widgets.get("Project")
Storage_Account=dbutils.widgets.get("Storage_Account")
scope=Storage_Account+'_scope'
key='accesskey_'+Storage_Account



# COMMAND ----------

container_name = Container
storage_account_name = Storage_Account
#storage_access_key=dbutils.secrets.get(scope=scope,key=key)
storage_access_key ="aFDlcJrb9inmSx6UbPBIaxwcTbq/B2n4C9qHoDI1QDZ5ggLxqMzVfOCcKQziCCZFKIUbTxGeOpsh+AStsz3XUQ=="
mount_point_path='/mnt/'+Project+'/'


# COMMAND ----------

def mount_blob(container_name, mount_point):
    try:
        dbutils.fs.mount(
        source = "wasbs://"+container_name+"@"+storage_account_name+".blob.core.windows.net",
        mount_point = mount_point,
        extra_configs ={"fs.azure.account.key."+storage_account_name+".blob.core.windows.net":storage_access_key})
        print("Successfully Mounted")
    except Exception as e:
        print("Already mounted.")

# COMMAND ----------

mount_blob(container_name, mount_point_path)
