# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

# --------------------------------------------
# Utility Function: Safe Logging
# --------------------------------------------
def log_message(message: str):
    print(f"[EVLytics Bronze] {message}")

# --------------------------------------------
# Step 1: Load Raw CSV into Bronze
# --------------------------------------------
def load_raw_csv(vehicle_path: str, schema: StructType):
    try:
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(schema)
            .load(vehicle_path)
        )
        log_message("Raw CSV successfully loaded into DataFrame.")
        return df
    except Exception as e:
        log_message(f"Error loading CSV: {e}")
        sys.exit(1)

# --------------------------------------------
# Step 2: Clean Headers & Add Metadata
# --------------------------------------------
def clean_and_enrich(df, ingestion_date):
    try:
        df = (
            df.withColumnRenamed("VIN (1–10)", "VehicleIdentificationNumber")
              .withColumnRenamed("Postal Code", "PostalCode")
              .withColumnRenamed("Model Year", "ModelYear")
              .withColumnRenamed("Electric Vehicle Type", "ElectricVehicleType")
              .withColumnRenamed("Clean Alternative Fuel Vehicle (CAFV) Eligibility", "CAFVEligibility")
              .withColumnRenamed("Electric Range", "ElectricRange")
              .withColumnRenamed("Base MSRP", "BaseMSRP")
              .withColumnRenamed("Legislative District", "LegislativeDistrict")
              .withColumnRenamed("DOL Vehicle ID", "DOLVehicleID")
              .withColumnRenamed("Vehicle Location", "VehicleLocation")
              .withColumnRenamed("Electric Utility", "ElectricUtility")
              .withColumnRenamed("2020 Census Tract", "CensusTract2020")
              .withColumn("FileName", regexp_extract(input_file_name(), r"([^/]+$)", 1))
              .withColumn("FileProcessingTime", lit(ingestion_date).cast(TimestampType()))
        )
        log_message("Columns renamed & metadata added (FileName, ProcessingTime).")
        return df
    except Exception as e:
        log_message(f"Error during cleaning: {e}")
        sys.exit(1)

# --------------------------------------------
# Step 3: Deduplicate New Files
# --------------------------------------------
def filter_new_files(df, bronze_table: str):
    try:
        existing_files = (
            spark.sql(f"SELECT DISTINCT FileName FROM {bronze_table}")
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        filtered_df = df.filter(~col("FileName").isin(existing_files))
        log_message(f"Found {filtered_df.count()} new rows to ingest.")
        return filtered_df
    except Exception:
        log_message("Bronze table not found yet — treating all files as new.")
        return df

# --------------------------------------------
# Step 4: Write Data to Bronze
# --------------------------------------------
def write_to_bronze(df, target_path: str, table_name: str):
    if not df.rdd.isEmpty():
        try:
            df.write.format("delta") \
                .option("mergeSchema", "true") \
                .mode("append") \
                .option("path", target_path) \
                .saveAsTable(table_name)
            log_message(f"Successfully written to Bronze table: {table_name}")
        except Exception as e:
            log_message(f"Error writing to Bronze: {e}")
            sys.exit(1)
    else:
        log_message("No new files to ingest. Skipping write.")

# --------------------------------------------
# Step 5: DQ Checks & Enrichment
# --------------------------------------------
def apply_dq_and_enrichment(df, specs_lookup):
    # Example rules: Remove invalid states, enrich with lookup values
    non_valid_states = ["NS", "BC", "AP"]

    enriched_df = (
        df.filter(~col("State").isin(non_valid_states))
          .join(
              specs_lookup,
              (df["Make"] == specs_lookup["Make"]) &
              (df["Model"] == specs_lookup["Model"]) &
              (df["ModelYear"] == specs_lookup["ModelYear"]),
              "left"
          )
          .withColumn(
              "ElectricRange",
              when(df["ElectricRange"].isNull(), specs_lookup["ElectricRange"]).otherwise(df["ElectricRange"])
          )
          .withColumn(
              "BaseMSRP",
              when(df["BaseMSRP"].isNull(), specs_lookup["BaseMSRP"]).otherwise(df["BaseMSRP"])
          )
          .withColumn(
              "IsWashingtonState",
              when(df["State"] != "WA", "NON-WASHINGTON STATE")
              .when(df["LegislativeDistrict"].isNull() & (df["State"] == "WA"), "NO LEGISLATIVE DISTRICT")
              .otherwise("WASHINGTON STATE")
          )
    )
    log_message("Data Quality checks applied and enrichment complete.")
    return enriched_df

# --------------------------------------------
# Main Bronze Pipeline
# --------------------------------------------
def bronze_pipeline(vehicle_path, vehicle_schema, ingestion_date, bronze_table, bronze_path, lookup_table):
    raw_df = load_raw_csv(vehicle_path, vehicle_schema)
    cleaned_df = clean_and_enrich(raw_df, ingestion_date)
    new_df = filter_new_files(cleaned_df, bronze_table)

    if new_df.rdd.isEmpty():
        log_message("No new data found. Pipeline ended gracefully.")
        return

    specs_lookup_df = spark.read.table(lookup_table)
    enriched_df = apply_dq_and_enrichment(new_df, specs_lookup_df)

    write_to_bronze(
        enriched_df,
        bronze_path,
        bronze_table
    )

    # Create temp view for next layers
    enriched_df.createOrReplaceTempView("vw_Vehicle")

# --------------------------------------------
# Example Run
# --------------------------------------------
bronze_pipeline(
    vehicle_path="/mnt/somerandomdatalake11/landing/databricks/transportation/evlytics-integration/web/vehicles-population/",
    vehicle_schema=vehicle_schema,
    ingestion_date=current_timestamp(),
    bronze_table="evlytics_bronze.raw_ext_vehiclepopulation",
    bronze_path="/mnt/somerandomdatalake11/lake/databricks/bronze/transportation/evlytics-integration/web/vehicles-population/",
    lookup_table="evlytics_bronze.vehiclespecificationslookup"
)



# Step 1: Create a temporary view from the cleaned DataFrame
try:
    selected_vehicle_df.createOrReplaceTempView("vw_Vehicle")
    print("Temporary view vw_Vehicle created successfully.")
except Exception as e:
    print(f"Error creating temp view: {str(e)}")
    raise



#Step 2: Merge Bronze → Silver with SCD Type 1 logic
MERGE INTO evlytics_silver.vehiclepopulation AS target
USING vw_Vehicle AS source
ON target.DOLVehicleID = source.DOLVehicleID

#Update only when data has changed (via HASH_ID)
WHEN MATCHED AND source.HASH_ID <> target.HASH_ID THEN
  UPDATE SET
    target.VehicleIdentificationNumber = source.VehicleIdentificationNumber,
    target.County = source.County,
    target.City = source.City,
    target.State = source.State,
    target.PostalCode = source.PostalCode,
    target.ModelYear = source.ModelYear,
    target.Make = source.Make,
    target.Model = source.Model,
    target.ElectricVehicleType = source.ElectricVehicleType,
    target.CAFVEligibility = source.CAFVEligibility,
    target.LegislativeDistrict = source.LegislativeDistrict,
    target.VehicleLocation = source.VehicleLocation,
    target.ElectricUtility = source.ElectricUtility,
    target.2020CensusTract = source.2020CensusTract,
    target.ElectricRange = source.ElectricRange,
    target.BaseMSRP = source.BaseMSRP,
    target.IsWashingtonState = source.IsWashingtonState,
    target.FileName = source.FileName,
    target.FileProcessingTime = source.FileProcessingTime,
    target.UpdatedDate = current_timestamp(),
    target.source_system = source.source_system,
    target.HASH_ID = source.HASH_ID

#Insert new rows if no match exists
WHEN NOT MATCHED THEN
  INSERT (
    DOLVehicleID, VehicleIdentificationNumber, County, City, State, PostalCode,
    ModelYear, Make, Model, ElectricVehicleType, CAFVEligibility, LegislativeDistrict,
    VehicleLocation, ElectricUtility, 2020CensusTract, ElectricRange, BaseMSRP,
    IsWashingtonState, FileName, FileProcessingTime, UpdatedDate, source_system,
    ingestion_date, HASH_ID
  )
  VALUES (
    source.DOLVehicleID, source.VehicleIdentificationNumber, source.County, source.City,
    source.State, source.PostalCode, source.ModelYear, source.Make, source.Model,
    source.ElectricVehicleType, source.CAFVEligibility, source.LegislativeDistrict,
    source.VehicleLocation, source.ElectricUtility, source.2020CensusTract,
    source.ElectricRange, source.BaseMSRP, source.IsWashingtonState, source.FileName,
    source.FileProcessingTime, NULL, source.source_system, source.ingestion_date,
    source.HASH_ID
  );


