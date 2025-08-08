# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS evlytics_bronze
# MAGIC LOCATION "/mnt/datalake/lake/databricks/bronze/transportation/evlytics-integration/web/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS evlytics_silver
# MAGIC LOCATION "/mnt/datalake/lake/databricks/silver/transportation/evlytics-integration/web/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS evlytics_gold
# MAGIC LOCATION "/mnt/datalake/lake/databricks/gold/transportation/evlytics-integration/"

# COMMAND ----------

# spark.sql("""
# CREATE TABLE IF NOT EXISTS nbanalytics_silver.players(
#     PlayerID INT,
#     FirstName STRING,
#     LastName STRING,
#     DOB DATE,
#     Age INT,
#     Height STRING,
#     Weight INT,
#     Position STRING,
#     TeamID INT,
#     EffectiveStartDate DATE,
#     EffectiveEndDate DATE,
#     IsCurrent INT,
#     FileName STRING,
#     source_system STRING,
#     ingestion_date TIMESTAMP,
#     HASH_ID STRING
# )
# USING DELTA
# LOCATION "/mnt/datalake/lake/databricks/silver/sports/nbanalytics/github/players"
# """)
