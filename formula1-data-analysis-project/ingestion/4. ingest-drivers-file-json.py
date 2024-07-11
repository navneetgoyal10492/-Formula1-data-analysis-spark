# Databricks notebook source
# MAGIC %md
# MAGIC 1. Specify Schema which needs to be applied
# MAGIC 2. Read data using spark json reader API passing schema
# MAGIC 3. Concat Date and Time Column to get race datetimestamp
# MAGIC 4. Renaming all the required columns
# MAGIC 5. Add audit Columns
# MAGIC 6. Write data in parquet format using Spark writer API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType
from pyspark.sql.functions import col, current_timestamp,concat,lit

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField('code', StringType(), False),
    StructField('dob', DateType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('driverRef', StringType(), True),
    StructField('name',StructType(fields=[
        StructField('forename',StringType(),True),
        StructField('surname',StringType(),True)
    ])),
    StructField('nationality',StringType(),True),
    StructField('number',StringType(),True),
    StructField('url',StringType(),True)
])
driver_df = spark.read.schema(drivers_schema).json(path=f'{raw_folder_path}/drivers.json')

# COMMAND ----------

driver_df = driver_df.\
    withColumn('full_name', concat(col('name.forename'),lit(' '),col('name.surname'))).\
    withColumn('ingestion_date',current_timestamp())
driver_selected_df = driver_df.\
    select(col('driverId').alias('driver_id'),
           col('driverRef').alias('driver_ref'),
           col('number'),
           col('code'),
           col('full_name'),
           col('dob'),
           col('nationality'),
           col('ingestion_date')
           )

# COMMAND ----------

#driver_selected_df.write.mode('overwrite').parquet(path=f'{processed_folder_path}/drivers')
driver_selected_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/drivers'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers
