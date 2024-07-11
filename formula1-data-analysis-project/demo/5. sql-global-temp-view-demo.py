# Databricks notebook source
# MAGIC %md
# MAGIC ##Accessing Dataframe Using SQL
# MAGIC ###Objective
# MAGIC 1. Create global temporary view on dataframe
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{reporting_folder_path}/race_results")
display(race_results_df.limit(5))

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC --SHOW TABLES IN global_temp
# MAGIC SELECT COUNT(*) FROM global_temp.gv_race_results

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM global_temp.gv_race_results WHERE race_year == 2019")
display(race_results_2019_df)
