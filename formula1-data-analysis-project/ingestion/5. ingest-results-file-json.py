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

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType,DoubleType
from pyspark.sql.functions import col, current_timestamp,concat,lit

# COMMAND ----------

results_schema = StructType(fields=[
    StructField('constructorId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('fastestLap', IntegerType()),
    StructField('fastestLapSpeed', StringType()),
    StructField('fastestLapTime', StringType()),
    StructField('grid', IntegerType()),
    StructField('laps', IntegerType()),
    StructField('milliseconds', IntegerType()),
    StructField('number', IntegerType()),
    StructField('points', DoubleType()),
    StructField('position', IntegerType()),
    StructField('positionOrder', IntegerType()),
    StructField('positionText', StringType()),
    StructField('raceId', IntegerType()),
    StructField('rank', IntegerType()),
    StructField('resultId', IntegerType()),
    StructField('statusId', IntegerType()),
    StructField('time', StringType()),
                ])

#dbutils.fs.ls('/mnt/dataanalysisonazuredl/formula1dataanalysisproject/raw/results.json')
results_df = spark.read.\
    schema(results_schema).\
    json(path=f'{raw_folder_path}/results.json')

# COMMAND ----------

results_selected_df = results_df.select(
    col('resultId').alias('result_id'),
    col('raceId').alias('race_id'),
    col('driverId').alias('driver_id'),
    col('constructorId').alias('constructor_id'),
    col('number'),
    col('grid'),
    col('position'),
    col('positionText').alias('position_text'),
    col('positionOrder').alias('position_order'),
    col('points'),
    col('laps'),
    col('time'),
    col('milliseconds'),
    col('fastestLap').alias('fastest_lap'),
    col('rank'),
    col('fastestLapTime').alias('fastest_lap_time'),
    col('fastestLapSpeed').alias('fastest_lap_speed')
).withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

#results_selected_df.write.mode('overwrite').partitionBy('race_id').parquet(path=f'{processed_folder_path}/results')
results_selected_df.write.mode('overwrite').partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/results'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results
