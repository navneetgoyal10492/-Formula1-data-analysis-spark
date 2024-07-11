# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. generate sas key for the specific container and copy sas token
# MAGIC 2. add sas token to azure key vault
# MAGIC 3. connect azure key vault to databricks key vault(add #/secrets/createScope)
# MAGIC 4. configure spark conf parameter to access adls using sas key
# MAGIC 5. read data from raw layer and print

# COMMAND ----------

#Accessing Data Using SAS Token
storage_account = 'dataanalysisonazuredl'
container = 'formula1dataanalysisproject'

sas_token = dbutils.secrets.get(scope="formula1-scope",key='formula1dl-sas-token')
#"sp=rwl&st=2024-06-13T08:53:22Z&se=2024-07-14T16:53:22Z&spr=https&sv=2022-11-02&sr=c&#sig=1VDKzWX5LtAVlKVttuk5srhEVstG8KDsKlzaofnuK2g%3D"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net",sas_token)

# COMMAND ----------

#dbutils.fs.ls(f'abfss://{container}@{storage_account}.dfs.core.windows.net')
circuit_df = spark.read.csv(
    path=f'abfss://{container}@{storage_account}.dfs.core.windows.net/raw/circuits.csv',
    header=True,
    inferSchema=True)
display(circuit_df.limit(5))

# COMMAND ----------


