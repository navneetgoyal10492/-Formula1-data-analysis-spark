# Databricks notebook source
# MAGIC %md
# MAGIC 1. Specify Schema which you want to apply
# MAGIC 2. Read data using spark csv reader API passing schema
# MAGIC 3. Renaming all the required columns
# MAGIC 4. Add audit Columns
# MAGIC 5. Write data in parquet format using Spark writer API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,DoubleType,StringType

# COMMAND ----------

circuit_table_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

circuit_df = spark.read.\
    option("header", True).\
    schema(circuit_table_schema).\
    csv(path=f'{raw_folder_path}/circuits.csv')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#circuit_df = circuit_df.select(cols("circuitId"))
circuit_df = circuit_df.drop("url")

circuits_renamed_df = circuit_df.withColumnRenamed("circuitId","circuit_id")\
                                .withColumnRenamed("circuitRef","circuit_ref")\
                                .withColumnRenamed("lat","latitude")\
                                .withColumnRenamed("long","longitude")\
                                .withColumnRenamed("alt","altitude")

circuits_renamed_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

#circuits_renamed_df.write.mode("overwrite").parquet(path=f'{processed_folder_path}/#circuits')
circuits_renamed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits

# COMMAND ----------


