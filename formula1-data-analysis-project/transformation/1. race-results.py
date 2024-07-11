# Databricks notebook source
# MAGIC %md
# MAGIC 1.
# MAGIC 2.
# MAGIC 3.
# MAGIC 4.
# MAGIC 5.

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

#dbutils.fs.ls('mnt/dataanalysisonazuredl/formula1dataanalysisproject/processed/results')
results_df = spark.read.parquet(f'{processed_folder_path}/results')
results_df = results_df.withColumnRenamed('time','race_time')
results_df.limit(10).show()

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')
races_df = races_df.withColumnRenamed('name','race_name').\
    withColumnRenamed('round','race_round').\
    withColumnRenamed('circuit_id','race_circuit_id').\
    select(col('race_id'),col('race_year'),col('race_name'),col('race_timestamp'),col('race_circuit_id'))
races_df.limit(10).show()

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')
circuits_df = circuits_df.withColumnRenamed('name','circuit_name').\
    withColumnRenamed('location','circuit_location').\
    withColumnRenamed('country','circuit_country').\
    withColumnRenamed('latitude','circuit_latitude').\
    withColumnRenamed('lng','circuit_lng').\
    withColumnRenamed('altitude','circuit_altitude').\
    select(col('circuit_id'),col('circuit_location'))
circuits_df.limit(10).show()

# COMMAND ----------

drivers_df = spark.read.parquet(f'{processed_folder_path}/drivers')
drivers_df = drivers_df.withColumnRenamed('number','driver_number').\
    withColumnRenamed('code','driver_code').\
    withColumnRenamed('full_name','driver_name').\
    withColumnRenamed('dob','driver_dob').\
    withColumnRenamed('nationality','driver_nationality').\
    select(col('driver_id'),col('driver_name'),col('driver_number'),col('driver_nationality'))
drivers_df.limit(10).show()

# COMMAND ----------

constructors_df = spark.read.parquet(f'{processed_folder_path}/constructors')
constructors_df = constructors_df.withColumnRenamed('name','team').\
    withColumnRenamed('nationality','constructor_nationality').\
    select(col('constructor_id'),col('team'))
constructors_df.limit(10).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Joining Race dataframe with Circuit Dataframe to Circuit details for each race

# COMMAND ----------

print(races_df.count())
print(circuits_df.count())
race_circuit_df = races_df.\
    join(circuits_df,circuits_df.circuit_id == races_df.race_circuit_id, "inner").\
    select(col('race_id'),col('race_year'),col('race_name'),col('race_timestamp'),col('circuit_location'))
print(race_circuit_df.count())
race_circuit_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Join Result Dataframe to Race_Circuit, Drivers,constructor Dataframe to get all the details

# COMMAND ----------

results_final_df = results_df.\
  join(race_circuit_df,results_df.race_id == race_circuit_df.race_id, "inner").\
  join(drivers_df,results_df.driver_id == drivers_df.driver_id, "inner").\
  join(constructors_df,results_df.constructor_id == constructors_df.constructor_id, "inner").\
  select(col('race_year'),col('race_name'),col('race_timestamp'),col('circuit_location'),
         col('driver_name'),col('driver_number'),col('driver_nationality'),
         col('team'),col('grid'),col('fastest_lap'),col('race_time'),col('points'),
         col('position'))

results_final_df = results_final_df.withColumn('created_date', current_timestamp())
display(results_final_df.filter((col('race_year') == 2020) & (col('race_name') == 'Abu Dhabi Grand Prix')).orderBy(results_final_df.points.desc()))

# COMMAND ----------

#results_final_df.write.mode('overwrite').parquet(path=f'{reporting_folder_path}/race_results')
results_final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

display(spark.read.parquet(f'{reporting_folder_path}/race_results'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results
