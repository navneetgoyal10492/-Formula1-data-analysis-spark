# Databricks notebook source
# MAGIC %md
# MAGIC 1. Multi Line JSON
# MAGIC 2. Specify Schema which needs to be applied
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

#dbutils.fs.ls('/mnt/dataanalysisonazuredl/formula1dataanalysisproject/raw/results.json')
pitstops_schema = StructType(fields=[
    StructField('driverId', IntegerType(), False),
    StructField('duration', StringType(),True),
    StructField('lap', IntegerType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('raceId', IntegerType(), True),
    StructField('stop', IntegerType(), True),
    StructField('time', StringType(), True)
])
                             
pitstops_df = spark.read.\
    option("multiLine", True).\
    schema(pitstops_schema).\
    json(path=f'{raw_folder_path}/pit_stops.json')

# COMMAND ----------

pitstops_renamed_df = pitstops_df.withColumnRenamed('driverId','driver_id').\
    withColumnRenamed('raceId','race_id').\
    withColumn('ingestion_date',current_timestamp())
pitstops_renamed_df.show()

# COMMAND ----------

#pitstops_renamed_df.write.mode('overwrite').parquet(path=f'{processed_folder_path}/pitstops')
pitstops_renamed_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/pitstops'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pitstops
