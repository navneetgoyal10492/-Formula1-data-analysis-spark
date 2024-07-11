# Databricks notebook source
# MAGIC %md
# MAGIC 1. Multiple CSV files(no header)
# MAGIC 2. Specify Schema which needs to be applied
# MAGIC 2. Read data using spark csv reader API passing schema
# MAGIC 4. Renaming all the required columns
# MAGIC 5. Add audit Columns
# MAGIC 6. Write data in parquet format using Spark writer API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType,DoubleType
from pyspark.sql.functions import col, current_timestamp,concat,lit

# COMMAND ----------

laptime_schema = StructType(fields=[
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('lap', IntegerType()),
    StructField('position', IntegerType()),
    StructField('time', StringType()),
    StructField('milliseconds', IntegerType())
])

laptime_df = spark.read.\
    schema(laptime_schema).\
    csv(path=f'{raw_folder_path}/lap_times/lap_times_split*.csv')

# COMMAND ----------

laptime_final_df = laptime_df.withColumnRenamed('raceId','race_id').\
    withColumnRenamed('driverId','driver_id').\
    withColumn('ingestion_date',current_timestamp())

print(laptime_final_df.count())
laptime_final_df.printSchema()
laptime_final_df.show()

# COMMAND ----------

#laptime_final_df.write.mode('overwrite').parquet(path=f'{processed_folder_path}/lap_times')
laptime_final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/lap_times'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times
