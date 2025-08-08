# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "/Repos/waleokare@gmail.com/databricks-project/Projects/Setup/04_Configurations"

# COMMAND ----------

p_Domain = 'transportation'
p_ProjectName = 'evlytics-integration'
p_DataSourceName = 'web'
p_PlatformName = 'databricks'
p_EntityName = 'vehicle-specification/'
p_FileName = 'vehicle_specs_lookup.csv'
p_SourceQuery = ''
landing_folder_path = '/mnt/datamladls26/landing/'
databricks_bronze_path = '/mnt/datamladls26/lake/databricks/bronze/'
databricks_silver_path = '/mnt/datamladls26/lake/databricks/silver/'
databricks_gold_path = '/mnt/datamladls26/lake/databricks/gold/'

# COMMAND ----------

VehicleSpecs_path = f"{landing_folder_path}/{p_PlatformName}/{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_FileName}"

VehicleSpecs_schema = StructType([
    StructField("Make", StringType(), True),
    StructField("Model", StringType(), True),
    StructField("ModelYear", IntegerType(), True),
    StructField("ElectricRange", IntegerType(), True),
    StructField("BaseMSRP", IntegerType(), True)
])

VehicleSpecs_bronze_lookup_df = spark.read.format("csv").option('header', 'true').schema(VehicleSpecs_schema).load(VehicleSpecs_path)

# COMMAND ----------

# VehicleSpecs_bronze_lookup_df.write.format("delta").mode("append").option('path', f"{databricks_bronze_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}").saveAsTable("evlytics_bronze.vehicleSpecificationsLookup")

# COMMAND ----------

display(spark.read.table("evlytics_bronze.vehicleSpecificationsLookup"))

# COMMAND ----------

p_FileName = 'LegislativeDistrict_Lookup.csv'
p_EntityName= 'legislative-district'

LegislativeDistrict_path = f"{landing_folder_path}/{p_PlatformName}/{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_FileName}"

LegislativeDistrict_schema = StructType([
    StructField("LegislativeDistrictNumber", IntegerType(), True),
    StructField("LegislativeDistrictName", StringType(), True),
    StructField("County", StringType(), True)  
])

LegislativeDistrict_bronze_lookup_df = spark.read.format("csv").schema(LegislativeDistrict_schema).option('header', 'true').load(LegislativeDistrict_path)

# COMMAND ----------

# LegislativeDistrict_bronze_lookup_df .write.format("delta").mode("append").option('path', f"{databricks_bronze_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}").saveAsTable("evlytics_bronze.LegislativeDistrictLookup")

# COMMAND ----------

display(spark.read.table("evlytics_bronze.LegislativeDistrictLookup"))