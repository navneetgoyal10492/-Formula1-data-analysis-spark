# Databricks notebook source
# MAGIC %md
# MAGIC 1. Multiple Multi Line JSON files
# MAGIC 2. Specify Schema which needs to be applied
# MAGIC 2. Read data using spark JSON reader API passing schema
# MAGIC 4. Renaming all the required columns
# MAGIC 5. Add audit Columns
# MAGIC 6. Write data in parquet format using Spark writer API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType,DoubleType
from pyspark.sql.functions import col, current_timestamp,concat,lit

# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField('qualifyId', IntegerType()),
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('constructorId', IntegerType()),
    StructField('number', IntegerType()),
    StructField('position', IntegerType()),
    StructField('q1', StringType()),
    StructField('q2', StringType()),
    StructField('q3', StringType())
])

qualifying_df = spark.read.\
    option('multiLine', True).\
    schema(qualifying_schema).\
    json(path=f'{raw_folder_path}/qualifying/qualifying_split*.json')

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed('qualifyId','qualify_id').\
    withColumnRenamed('raceId','race_id').\
    withColumnRenamed('driverId','driver_id').\
    withColumnRenamed('constructorId','constructor_id').\
    withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

#qualifying_renamed_df.write.mode('overwrite').parquet(path=f'{processed_folder_path}/qualifying')
qualifying_renamed_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/qualifying'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying
