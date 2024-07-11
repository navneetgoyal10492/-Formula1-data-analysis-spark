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
# MAGIC 7. configure spark conf parameter to access adls using Service Principal
# MAGIC 8. Mountint ADLS gen2 on azure Databricks
# MAGIC 9. read data from raw layer and print

# COMMAND ----------

def mount_adls(container,storage_account):
    #Accessing Data Using Servie Principal
    #Mountint ADLS gen2 on azure Databricks

    client_id = dbutils.secrets.get(scope='formula1-scope',key='formula1dl-client-id')
    tenant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-tenant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1-secret-value')

    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    if any(mount.mountPoint == f"/mnt/{storage_account}/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account}/{container}")

    # Optionally, you can add <directory-name> to the source URI of your mount point.
    dbutils.fs.mount(
    source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account}/{container}",
    extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

storage_account = 'dataanalysisonazuredl'
container = 'formula1dataanalysisproject'
mount_adls(container,storage_account)

# COMMAND ----------

#dbutils.fs.ls(f'abfss://{container}@{storage_account}.dfs.core.windows.net')
circuit_df = spark.read.csv(
    path=f'/mnt/dataanalysisonazuredl/formula1dataanalysisproject/raw/circuits.csv',
    header=True,
    inferSchema=True)
display(circuit_df.limit(5))

# COMMAND ----------


