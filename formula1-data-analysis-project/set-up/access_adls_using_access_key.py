# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC 1. add access key to azure key vault
# MAGIC 2. connect azure key vault to databricks key vault(add #/secrets/createScope)
# MAGIC 3. configure spark conf parameter to access adls using access key
# MAGIC 4. read data from raw layer and print

# COMMAND ----------

#Accessing ADLS Using Access Key
'''%fs %sh %md dbutils package'''
storage_account = 'dataanalysisonazuredl'
container = 'formula1dataanalysisproject'
access_key = dbutils.secrets.get(scope="formula1-scope",key="formula1dl-access-key")
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    access_key)

# COMMAND ----------

circuit_df = spark.read.csv(
    path=f"abfss://{container}@{storage_account}.dfs.core.windows.net/raw/circuits.csv",
    header=True,
    inferSchema=True
)
display(circuit_df.limit(10))

# COMMAND ----------


