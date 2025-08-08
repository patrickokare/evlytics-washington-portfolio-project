# Databricks notebook source
# Install pygeohash if not already installed
# %pip install pygeohash

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, sha2, concat_ws, lit, current_timestamp, row_number
from pyspark.sql.window import Window
import pygeohash as geohash
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC Step I - Load latest record from Silver Zone

# COMMAND ----------

# Step 1: Start from source table and filter out already processed FileNames
# dimGeoPoint_filenames_df = spark.table("evlytics_gold.DimGeoPoint").select("FileName").distinct()

vehicle_population_df = spark.table("evlytics_silver.vehiclepopulation") \
    .filter(col("VehicleLocation").isNotNull()) \
    .filter(col("VehicleLocation").rlike("POINT ?\\([-0-9.]+ [-0-9.]+\\)"))
 #       .join(dimGeoPoint_filenames_df, on ="FileName", how="left_anti") -- NB: Watermark tracker -> etl_log_metadata table is a better alternative for incremental inserts.

# COMMAND ----------

# MAGIC %md
# MAGIC Step II - Extract Longitude and Latitude using regexp

# COMMAND ----------

# Step 2: Extract Longitude and Latitude using regexp
geopoint_df = vehicle_population_df.withColumn("Longitude", regexp_extract("VehicleLocation", r"POINT \((-?[0-9.]+) -?[0-9.]+\)", 1).cast("double")) \
  .withColumn("Latitude", regexp_extract("VehicleLocation", r"POINT \(-?[0-9.]+ (-?[0-9.]+)\)", 1).cast("double"))
  

# COMMAND ----------

# MAGIC %md
# MAGIC Step III - Add Hash & Natural kEYS TO the Dimension Table

# COMMAND ----------

# Step 3: Compute GeoPointNaturalKey and HASH_ID for our dimension table
geopoint_df = geopoint_df.withColumn("GeoPointNaturalkey",  sha2(concat_ws("|", col("Longitude").cast("string"),col("Latitude").cast("string")), 256)) \
  .withColumn("HASH_ID", sha2(concat_ws("|", col("Longitude").cast("string"),col("Latitude").cast("string")), 256))

# COMMAND ----------

# MAGIC %md
# MAGIC Step IV - Adding Derived / Auditing columns

# COMMAND ----------

geopoint_df = geopoint_df.withColumn("GeoHash", lit(None).cast("string")) \
  .withColumn("ZipCentroidFlag", lit(False)) \
  .withColumn("SourceSystem", col("source_system")) \
  .withColumn("UpdatedDate", lit(None).cast("timestamp")) \
  .withColumn("InsertedDate", current_timestamp())


# COMMAND ----------

# Final: Select and reorder columns if needed
selectGeoPoint_df = geopoint_df.select("VehicleLocation", "Longitude", "Latitude", "GeoHash", "ZipCentroidFlag","FileName", "SourceSystem", "UpdatedDate", "InsertedDate","GeoPointNaturalKey", "HASH_ID")

# COMMAND ----------

# MAGIC %md
# MAGIC Step V - dEDUPLICATION via ROW_NUMBER() on Natural Key

# COMMAND ----------

windowSpec = Window.partitionBy("GeoPointNaturalKey").orderBy(current_timestamp())

# COMMAND ----------

deduped_geopoint_df = selectGeoPoint_df.withColumn("row_num", row_number().over(windowSpec))
deduped_geopoint_df = deduped_geopoint_df.filter(col("row_num") == 1)

# COMMAND ----------

# MAGIC %md
# MAGIC Enrichment for GeoHash
# MAGIC **GeoHash** is a short string of letters and digits that encodes a **geographic coordinate (latitude, longitude)** into a compact form.
# MAGIC
# MAGIC Think of it as a **spatial ZIP code** that:
# MAGIC
# MAGIC - Groups nearby locations into the same prefix
# MAGIC - Allows **fast lookups**, **indexing**, **clustering**, and **partitioning** in big data systems

# COMMAND ----------

# Define the actual geohash computation function
def compute_geohash(lat, lon, precision=9):
    if lat is None or lon is None:
        return None
    try:
        return geohash.encode(lat, lon, precision)
    except:
        return None

# Register UDF with Spark
geohash_udf = udf(compute_geohash, StringType())

# Apply to your DataFrame after Longitude and Latitude are extracted
dimGeoPoint_df = deduped_geopoint_df.withColumn("GeoHash", geohash_udf("Latitude", "Longitude"))

# COMMAND ----------

# MAGIC %md
# MAGIC Step VIII - Create TEMP View to Stage the Data

# COMMAND ----------

dimGeoPoint_df = dimGeoPoint_df.drop("row_num")
dimGeoPoint_df.createOrReplaceTempView("vw_DimGeoPoint")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO evlytics_gold.DimGeoPoint AS T
# MAGIC USING vw_DimGeoPoint AS S
# MAGIC ON T.GeoPointNaturalKey = S.GeoPointNaturalKey
# MAGIC
# MAGIC -- Type 1 logic with HASH_ID for overwrite
# MAGIC WHEN MATCHED AND (T.HASH_ID <> S.HASH_ID) THEN
# MAGIC   UPDATE SET
# MAGIC     T.VehicleLocation = S.VehicleLocation,
# MAGIC     T.Latitude = S.Latitude,
# MAGIC     T.Longitude = S.Longitude,
# MAGIC     T.GeoHash = S.GeoHash,
# MAGIC     T.ZipCentroidFlag = S.ZipCentroidFlag,
# MAGIC     T.FileName = S.FileName,
# MAGIC     T.SourceSystem = S.SourceSystem,
# MAGIC     T.UpdatedDate = current_timestamp(),
# MAGIC     T.HASH_ID = S.HASH_ID
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     VehicleLocation, Latitude, Longitude, GeoHash, ZipCentroidFlag,FileName, SourceSystem, UpdatedDate, InsertedDate,GeoPointNaturalKey, HASH_ID
# MAGIC   )
# MAGIC   VALUES (
# MAGIC    S.VehicleLocation,  S.Latitude, S.Longitude, S.GeoHash, FALSE,S.FileName, S.SourceSystem, NULL, current_timestamp(),S.GeoPointNaturalKey, S.HASH_ID
# MAGIC   )