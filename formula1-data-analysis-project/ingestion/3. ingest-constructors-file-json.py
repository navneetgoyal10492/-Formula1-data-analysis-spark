# Databricks notebook source
# MAGIC %md
# MAGIC 1. Specify Schema using DDL command you want to apply
# MAGIC 2. Read data using spark json reader API passing schema
# MAGIC 3. Concat Date and Time Column to get race datetimestamp
# MAGIC 4. Renaming all the required columns
# MAGIC 5. Add audit Columns
# MAGIC 6. Write data in parquet format using Spark writer API

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
constructor_df = spark.read.\
    schema(constructors_schema).\
    json(path=f'{raw_folder_path}/constructors.json')

# COMMAND ----------

constructor_df = constructor_df.drop('url')
constructor_df = constructor_df.withColumn("ingestion_date",current_timestamp())
constructor_renamed_df = constructor_df.\
    withColumnRenamed("constructorId", "constructor_id").\
    withColumnRenamed("constructorRef", "constructor_ref")
constructor_renamed_df.printSchema()
constructor_renamed_df.show()

# COMMAND ----------

print(constructor_renamed_df.count())

# COMMAND ----------

#constructor_renamed_df.write.mode('overwrite').partitionBy('nationality').parquet(f'{processed_folder_path}/constructors')
constructor_renamed_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/constructors/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors
