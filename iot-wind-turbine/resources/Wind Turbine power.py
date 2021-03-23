# Databricks notebook source
display(spark.read.format("text").load("/mnt/quentin-demo-resources/turbine/power/raw"))

# COMMAND ----------

#Load stream from our files
spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "json") \
                .option("cloudFiles.maxFilesPerTrigger", 1) \
                .schema("turbine_id bigint, date timestamp, power float, wind_speed float, theoretical_power_curve float, wind_direction float") \
                .load("/Users/quentin.ambard@databricks.com/turbine/power/raw") \
     .writeStream.format("delta") \
        .option("checkpointLocation", "/Users/joseph@databricks.com/turbine/power/bronze/checkpoint") \
        .option("path", "/Users/joseph@databricks.com/turbine/power/bronze/data") \
        .trigger(processingTime = "10 seconds") \
        .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists joseph.turbine_power_bronze;
# MAGIC 
# MAGIC create table if not exists joseph.turbine_power_bronze
# MAGIC   using delta
# MAGIC   location '/Users/joseph@databricks.com/turbine/power/bronze/data'
# MAGIC   TBLPROPERTIES ('delta.autoOptimize.autoCompact' = true, 'delta.autoOptimize.optimizeWrite' = true);
# MAGIC 
# MAGIC select to_date(date) date, sum(power) as power from joseph.turbine_power_bronze group by date;
