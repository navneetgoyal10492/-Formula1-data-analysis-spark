# Databricks notebook source
# MAGIC %md
# MAGIC 1. Specify Schema which you want to apply
# MAGIC 2. Read data using spark csv reader API passing schema
# MAGIC 3. Concat Date and Time Column to get race datetimestamp
# MAGIC 4. Renaming all the required columns
# MAGIC 5. Add audit Columns
# MAGIC 6. Write data in parquet format using Spark writer API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DateType
from pyspark.sql.functions import current_timestamp,concat,col,lit,to_timestamp

# COMMAND ----------

races_table_schema = StructType(fields=[
    StructField("raceId",IntegerType(),False),
    StructField("year",IntegerType(),True),
    StructField("round",IntegerType(),True),
    StructField("circuitId",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("date",DateType(),True),
    StructField("time",StringType(),True),
    StructField("url",StringType(),True)
])
races_df = spark.read.\
    option("header", True).\
    schema(races_table_schema).\
    csv(path=f'{raw_folder_path}/races.csv')

# COMMAND ----------

races_df = races_df.withColumn("ingestion_date",current_timestamp()).\
                    withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time'))))

races_renamed_df = races_df.\
  withColumnRenamed('raceId','race_id').\
  withColumnRenamed("year", "race_year").\
  withColumnRenamed("circuitId", "circuit_id")

races_selected_df = races_renamed_df.select(col('race_id'),col('race_year'),col('round'),col('circuit_id'),
                                            col('name'),col('race_timestamp'),col('ingestion_date'))


# COMMAND ----------

#races_selected_df.write.mode('overwrite').parquet(f'{processed_folder_path}/races')
races_selected_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races
