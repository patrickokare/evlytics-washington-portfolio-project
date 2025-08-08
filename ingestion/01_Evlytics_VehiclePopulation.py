# Databricks notebook source
#Author: PATRICK OKARE
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "/Repos/test@gmail.com/databricks-project/Projects/Setup/04_Configurations"

# COMMAND ----------

# Retrieve parameters passed from the master notebook
p_Domain = dbutils.widgets.get("Domain")
p_ProjectName = dbutils.widgets.get("ProjectName")
p_DataSourceName = dbutils.widgets.get("DataSourceName")
p_PlatformName = dbutils.widgets.get("PlatformName")
p_EntityName = dbutils.widgets.get("EntityName")
p_FileName = dbutils.widgets.get("FileName")
p_SourceQuery = dbutils.widgets.get("SourceQuery")
landing_folder_path = dbutils.widgets.get("landing_folder_path")
databricks_bronze_path = dbutils.widgets.get("databricks_bronze_path")
databricks_silver_path = dbutils.widgets.get("databricks_silver_path")
databricks_gold_path = dbutils.widgets.get("databricks_gold_path")

# COMMAND ----------

# p_Domain = 'transportation'
# p_ProjectName = 'evlytics-integration'
# p_DataSourceName = 'web'
# p_PlatformName = 'databricks'
# p_EntityName = 'vehicles-population/'
# p_FileName = 'Electric_Vehicle_Population_Data_20250723.csv'
# p_SourceQuery = ''
# landing_folder_path = '/mnt/datalake/landing/'
# databricks_bronze_path = '/mnt/datalake/lake/databricks/bronze/'
# databricks_silver_path = '/mnt/datalake/lake/databricks/silver/'
# databricks_gold_path = '/mnt/datalake/lake/databricks/gold/'

# COMMAND ----------

# MAGIC %md
# MAGIC Step I - Schema Structure & DataTypes AS-IS from Source System

# COMMAND ----------

vehicle_path = f"{landing_folder_path}/{p_PlatformName}/{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}"

vehicle_schema = StructType([
    StructField("VIN (1-10)", StringType(), True),
    StructField("County", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),
    StructField("Model Year", StringType(), True),
    StructField("Make", StringType(), True),
    StructField("Model", StringType(), True),
    StructField("Electric Vehicle Type", StringType(), True),
    StructField("Clean Alternative Fuel Vehicle (CAFV) Eligibility", StringType(), True),
    StructField("Electric Range", StringType(), True),
    StructField("Base MSRP", StringType(), True),
    StructField("Legislative District", StringType(), True),
    StructField("DOL Vehicle ID", StringType(), True),
    StructField("Vehicle Location", StringType(), True),
    StructField("Electric Utility", StringType(), True),
    StructField("2020 Census Tract", StringType(), True)
])

vehicle_bronze_df = spark.read.format("csv").option('header', 'true').schema(vehicle_schema).load(vehicle_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Step II -  Data Cleaning of Invalid Characters ON hEADERS

# COMMAND ----------

vehicle_bronze_df = (
    vehicle_bronze_df
    .withColumnRenamed("VIN (1-10)", "VehicleIdentificationNumber")
    .withColumnRenamed("Postal Code", "PostalCode")
    .withColumnRenamed("Model Year", "ModelYear")
    .withColumnRenamed("Electric Vehicle Type", "ElectricVehicleType")
    .withColumnRenamed("Clean Alternative Fuel Vehicle (CAFV) Eligibility", "CAFVEligibility")
    .withColumnRenamed("Electric Range", "ElectricRange")
    .withColumnRenamed("Base MSRP", "BaseMSRP")
    .withColumnRenamed("Legislative District", "LegislativeDistrict")
    .withColumnRenamed("DOL Vehicle ID"  , "DOLVehicleID")
    .withColumnRenamed("Vehicle Location", "VehicleLocation")
    .withColumnRenamed("Electric Utility", "ElectricUtility")
    .withColumnRenamed("2020 Census Tract", "2020CensusTract")
    .withColumn("FileName", regexp_extract(input_file_name(), r"([^/]+$)", 1))
    .withColumn("FileProcessingTime", lit(ingestion_date).cast(TimestampType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step III - Check for new Files; Filter already processed file, Append to Bronze Tables Only If the File is New!

# COMMAND ----------

# Step 1: Get already loaded filenames from the Bronze table
existing_files = (
    spark.sql("SELECT DISTINCT FileName FROM evlytics_bronze.raw_ext_vehiclepopulation")
    .rdd.flatMap(lambda x: x)
    .collect()
)

# Step 2: Filter out files that already exist
new_vehicle_bronze_df = vehicle_bronze_df.filter(~col("FileName").isin(existing_files))

# Step 3: Write only if new data exists
if not new_vehicle_bronze_df.rdd.isEmpty():
    new_vehicle_bronze_df.write.format("delta").option("mergeSchema", "true").mode("append").option('path', f"{databricks_bronze_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}").saveAsTable("evlytics_bronze.raw_ext_vehiclepopulation")
else:
    print("No new files to ingest. Skipping write.")

# COMMAND ----------

# MAGIC %md
# MAGIC Step IV - Data transformations, Enrichments, Standardization & Data Quality Checks

# COMMAND ----------

#Testing Purposes Only for DQ Checks
# new_vehicle_bronze_df  = spark.read.table("evlytics_bronze.raw_ext_vehiclepopulation") 

#Fitering out non-valid usa states
non_valid_states = ["NS", "BC", "AP"]
vehicle_filtered_df = (
    new_vehicle_bronze_df.filter(~col("State").isin(non_valid_states))
)

# COMMAND ----------

vehicle_specs_lookup_df = spark.read.table("evlytics_bronze.vehiclespecificationslookup")
enriched_vehicle_df = (
    vehicle_filtered_df.alias("bronze")
    .join(
        vehicle_specs_lookup_df.alias("lookup"),
        (col("bronze.Make") == col("lookup.Make")) &
        (col("bronze.Model") == col("lookup.Model")) &
        (col("bronze.ModelYear") == col("lookup.ModelYear")),
        "left"
    )
    .select(
        # All columns from Bronze except ElectricRange and BaseMSRP
        *[col(f"bronze.{c}") for c in vehicle_filtered_df.columns if c not in ["ElectricRange", "BaseMSRP"]],
        
        # Enrich ElectricRange only if it's NULL
        when(
            col("bronze.ElectricRange").isNull(), col("lookup.ElectricRange")).otherwise(col("bronze.ElectricRange")).alias("ElectricRange"),

        # Enrich BaseMSRP only if it's NULL
        when(col("bronze.BaseMSRP").isNull(), col("lookup.BaseMSRP")).otherwise(col("bronze.BaseMSRP")).alias("BaseMSRP")
    )
    .withColumn(
        "IsWashingtonState",
        when(col("bronze.State") != "WA",  "NON-WASHINGTON STATE")
        .when(col("bronze.legislativeDistrict").isNull() & (col("bronze.State") == "WA"),  "NO LEGISLATIVE DISTRICT")
        .otherwise("WASHINGTON STATE")   
    )
    .withColumn(
        "VehicleLocation",
        when(col("VehicleLocation").isNull(),lit("UNKNOWN")).otherwise(col("VehicleLocation")
                
                            )  
     
)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Step V - Implementing Data Types, Re-Arrange Fileds & Standardization

# COMMAND ----------

selected_vehicle_df = enriched_vehicle_df.select(
    col("DOLVehicleID").cast(IntegerType()),
    col("VehicleIdentificationNumber").cast(StringType()),
    col("County").cast(StringType()),
    col("City").cast(StringType()),
    col("State").cast(StringType()),
    col("PostalCode").cast(IntegerType()),
    col("ModelYear").cast(IntegerType()),
    col("Make").cast(StringType()),
    col("Model").cast(StringType()),
    col("ElectricVehicleType").cast(StringType()),
    col("CAFVEligibility").cast(StringType()),
    col("LegislativeDistrict").cast(IntegerType()),
    col("VehicleLocation").cast(StringType()),
    col("ElectricUtility").cast(StringType()),
    col("2020CensusTract").cast(StringType()),
    col("ElectricRange").cast(IntegerType()),
    col("BaseMSRP").cast(IntegerType()),
    col("IsWashingtonState").cast(StringType()),
    col("FileName").cast(StringType()),
    col("FileProcessingTime").cast(TimestampType()),
    lit(None).alias("UpdatedDate").cast(TimestampType())
)

# COMMAND ----------

# MAGIC %md
# MAGIC Step VI Add Hash-based identifier for change detection and SCD tracking → Deduplication Change detection

# COMMAND ----------

# Define the column list to concatenate for the hash key (horizontal format)
hash_columns = [ 'DOLVehicleID','VehicleIdentificationNumber', 'County', 'City', 'State', 'PostalCode', 'ModelYear', 'Make', 'Model', 'ElectricVehicleType', 'CAFVEligibility', 'LegislativeDistrict', 'VehicleLocation', 'ElectricUtility', '2020CensusTract', 'ElectricRange', 'BaseMSRP', 'IsWashingtonState', 'FileName']

selected_vehicle_df = add_metadata_columns(selected_vehicle_df, p_DataSourceName, ingestion_date, hash_columns)

# COMMAND ----------

# selected_vehicle_df = selected_vehicle_df.withColumn("ingestion_date", col("ingestion_date").cast(TimestampType()))

# COMMAND ----------

# MAGIC %md
# MAGIC Step VII - Create Temp Table & Stage Data

# COMMAND ----------

# Create a temporary view from your DataFrame
selected_vehicle_df.createOrReplaceTempView("vw_Vehicle")

# COMMAND ----------

# MAGIC %md
# MAGIC Final Step - Load Silver Table for Vehicle Population

# COMMAND ----------

#  selected_vehicle_df.write.format("delta").mode("append").option('path', f"{databricks_silver_path}{p_Domain}/{p_ProjectName}/{p_DataSourceName}/{p_EntityName}").saveAsTable("evlytics_silver.vehiclepopulation")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO evlytics_silver.vehiclepopulation AS target
# MAGIC USING vw_Vehicle AS source
# MAGIC ON target.DOLVehicleID = source.DOLVehicleID
# MAGIC --Correctly check HASH_ID to avoid unnecessary updates — change detection.
# MAGIC WHEN MATCHED AND source.HASH_ID <> target.HASH_ID THEN
# MAGIC   UPDATE SET
# MAGIC     target.VehicleIdentificationNumber = source.VehicleIdentificationNumber,
# MAGIC     target.County = source.County,
# MAGIC     target.City = source.City,
# MAGIC     target.State = source.State,
# MAGIC     target.PostalCode = source.PostalCode,
# MAGIC     target.ModelYear = source.ModelYear,
# MAGIC     target.Make = source.Make,
# MAGIC     target.Model = source.Model,
# MAGIC     target.ElectricVehicleType = source.ElectricVehicleType,
# MAGIC     target.CAFVEligibility = source.CAFVEligibility,
# MAGIC     target.LegislativeDistrict = source.LegislativeDistrict,
# MAGIC     target.VehicleLocation = source.VehicleLocation,
# MAGIC     target.ElectricUtility = source.ElectricUtility,
# MAGIC     target.2020CensusTract = source.2020CensusTract,
# MAGIC     target.ElectricRange = source.ElectricRange,
# MAGIC     target.BaseMSRP = source.BaseMSRP,
# MAGIC     target.IsWashingtonState = source.IsWashingtonState,
# MAGIC     target.FileName = source.FileName,
# MAGIC     target.FileProcessingTime = source.FileProcessingTime,
# MAGIC     target.UpdatedDate = current_timestamp(),
# MAGIC     target.source_system = source.source_system,
# MAGIC     target.HASH_ID = source.HASH_ID
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     DOLVehicleID, VehicleIdentificationNumber, County, City, State, PostalCode, ModelYear, Make, Model, ElectricVehicleType, CAFVEligibility, LegislativeDistrict, VehicleLocation, ElectricUtility, 2020CensusTract, ElectricRange, BaseMSRP, IsWashingtonState, FileName, FileProcessingTime, UpdatedDate, source_system, ingestion_date, HASH_ID
# MAGIC ) VALUES (
# MAGIC     source.DOLVehicleID, source.VehicleIdentificationNumber, source.County, source.City, source.State, source.PostalCode, source.ModelYear, source.Make, source.Model, source.ElectricVehicleType, source.CAFVEligibility, source.LegislativeDistrict, source.VehicleLocation, source.ElectricUtility, source.2020CensusTract, source.ElectricRange, source.BaseMSRP, source.IsWashingtonState, source.FileName, source.FileProcessingTime, NULL, source.source_system,
# MAGIC     source.ingestion_date, source.HASH_ID
# MAGIC )
