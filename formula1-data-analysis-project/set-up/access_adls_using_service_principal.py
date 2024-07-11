# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Create a resource(Search Microsoft Intra ID - Azure Active Directory)
# MAGIC 2. Under App Registration - Create an App - Copy "Client Id" & "Tenant Id"
# MAGIC 3. Create Client & Credentials - Copy Secret Id and Secret Value
# MAGIC 4. add "Client Id"(Application ID), "Tenant Id"(Directory ID) and Secret Value(Viewed Once only) to azure key vault
# MAGIC 5. connect azure key vault to databricks key vault(add #/secrets/createScope)
# MAGIC 6. DNS name = Vault URI & Resource ID = resource id
# MAGIC 7. configure spark conf parameter to access adls using sas key
# MAGIC 8. read data from raw layer and print

# COMMAND ----------

#Accessing Data Using Servie Principal
storage_account = 'dataanalysisonazuredl'
container = 'formula1dataanalysisproject'

client_id = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1-secret-value')

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

#dbutils.fs.ls(f'abfss://{container}@{storage_account}.dfs.core.windows.net')
circuit_df = spark.read.csv(
    path=f'abfss://{container}@{storage_account}.dfs.core.windows.net/raw/circuits.csv',
    header=True,
    inferSchema=True)
display(circuit_df.limit(5))

# COMMAND ----------


