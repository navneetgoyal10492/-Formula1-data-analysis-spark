# Databricks notebook source
# MAGIC %md
# MAGIC 1. Get Constructors Standing by calculating number of wins and total points

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,count,sum,when
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc

# COMMAND ----------

race_results_df = spark.read.parquet(f'{reporting_folder_path}/race_results')

constructors_standing_df = race_results_df.\
    groupBy("race_year","team").\
    agg(sum("points").alias("total_points"),
        count(when(col("position") == 1,True)).alias("wins")).\
        sort("total_points",ascending=False)

# COMMAND ----------

cons_window_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructors_standing_df.withColumn("rank",rank().over(cons_window_spec))
final_df.filter(col('race_year') == 2020).show()

# COMMAND ----------

#final_df.write.mode('overwrite').parquet(path=f"{reporting_folder_path}/constructors_standing")
final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_presentation.constructors_standing")

# COMMAND ----------

display(spark.read.parquet(f'{reporting_folder_path}/constructors_standing'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructors_standing
