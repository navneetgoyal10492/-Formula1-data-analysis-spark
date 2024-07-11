# Databricks notebook source
# MAGIC %md
# MAGIC 1. Get Driver's Standing by calculating number of wins and total points

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,count,sum,when
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc

# COMMAND ----------

race_results_df = spark.read.parquet(f'{reporting_folder_path}/race_results')
#display(race_results_df.limit(5))
#race_results_df.printSchema()
#race_results_df = race_results_df.withColumn("points", race_results_df.#points.cast(IntegerType()))
drivers_standing_df = race_results_df.\
    groupBy("race_year","driver_name","driver_nationality","team").\
    agg(sum("points").alias("total_points"),
        count(when(col("position") == 1,True)).alias("wins")).\
        sort("total_points",ascending=False)

# COMMAND ----------

driver_window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = drivers_standing_df.withColumn("rank",rank().over(driver_window_spec))
#final_df.filter(col('race_year') == 2020).show()

# COMMAND ----------

#final_df.write.mode('overwrite').parquet(path=f"{reporting_folder_path}/drivers_standing")
final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_presentation.drivers_standing")

# COMMAND ----------

display(spark.read.parquet(f'{reporting_folder_path}/drivers_standing'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.drivers_standing
