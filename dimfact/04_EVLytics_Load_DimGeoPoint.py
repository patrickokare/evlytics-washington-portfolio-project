from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, sha2, concat_ws, lit, current_timestamp, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pygeohash as geohash

# Initialize Spark session
spark = SparkSession.builder.appName("DimGeoPoint_ETL").getOrCreate()

# Step 1 - Load source Silver table
vehicle_population_df = spark.table("evlytics_silver.vehiclepopulation") \
    .filter(col("VehicleLocation").isNotNull()) \
    .filter(col("VehicleLocation").rlike("POINT ?\\([-0-9.]+ [-0-9.]+\\)"))

# Step 2 - Extract Longitude and Latitude
geopoint_df = vehicle_population_df.withColumn(
    "Longitude", regexp_extract("VehicleLocation", r"POINT \((-?[0-9.]+) -?[0-9.]+\)", 1).cast("double")
).withColumn(
    "Latitude", regexp_extract("VehicleLocation", r"POINT \(-?[0-9.]+ (-?[0-9.]+)\)", 1).cast("double")
)

# Step 3 - Compute Natural Key and Hash ID
geopoint_df = geopoint_df.withColumn(
    "GeoPointNaturalKey",
    sha2(concat_ws("|", col("Longitude").cast("string"), col("Latitude").cast("string")), 256)
).withColumn(
    "HASH_ID",
    sha2(concat_ws("|", col("Longitude").cast("string"), col("Latitude").cast("string")), 256)
)

# Step 4 - Add Audit & Static Columns
geopoint_df = geopoint_df.withColumn("GeoHash", lit(None).cast("string")) \
    .withColumn("ZipCentroidFlag", lit(False)) \
    .withColumn("SourceSystem", col("source_system")) \
    .withColumn("UpdatedDate", lit(None).cast("timestamp")) \
    .withColumn("InsertedDate", current_timestamp())

# Step 5 - Select Final Columns
selectGeoPoint_df = geopoint_df.select(
    "VehicleLocation", "Longitude", "Latitude", "GeoHash", "ZipCentroidFlag",
    "FileName", "SourceSystem", "UpdatedDate", "InsertedDate",
    "GeoPointNaturalKey", "HASH_ID"
)

# Step 6 - Deduplicate by Natural Key
windowSpec = Window.partitionBy("GeoPointNaturalKey").orderBy(current_timestamp())
deduped_geopoint_df = selectGeoPoint_df.withColumn("row_num", row_number().over(windowSpec)) \
    .filter(col("row_num") == 1)

# Step 7 - Compute GeoHash
def compute_geohash(lat, lon, precision=9):
    if lat is None or lon is None:
        return None
    try:
        return geohash.encode(lat, lon, precision)
    except:
        return None

geohash_udf = udf(compute_geohash, StringType())

dimGeoPoint_df = deduped_geopoint_df.withColumn("GeoHash", geohash_udf("Latitude", "Longitude")) \
    .drop("row_num")

# Step 8 - Create Temporary View for Merge
dimGeoPoint_df.createOrReplaceTempView("vw_DimGeoPoint")

# Step 9 - Merge Logic (run as Spark SQL)
spark.sql("""
MERGE INTO evlytics_gold.DimGeoPoint AS T
USING vw_DimGeoPoint AS S
ON T.GeoPointNaturalKey = S.GeoPointNaturalKey

WHEN MATCHED AND (T.HASH_ID <> S.HASH_ID) THEN
  UPDATE SET
    T.VehicleLocation = S.VehicleLocation,
    T.Latitude = S.Latitude,
    T.Longitude = S.Longitude,
    T.GeoHash = S.GeoHash,
    T.ZipCentroidFlag = S.ZipCentroidFlag,
    T.FileName = S.FileName,
    T.SourceSystem = S.SourceSystem,
    T.UpdatedDate = current_timestamp(),
    T.HASH_ID = S.HASH_ID

WHEN NOT MATCHED THEN
  INSERT (
    VehicleLocation, Latitude, Longitude, GeoHash, ZipCentroidFlag,
    FileName, SourceSystem, UpdatedDate, InsertedDate, GeoPointNaturalKey, HASH_ID
  )
  VALUES (
    S.VehicleLocation, S.Latitude, S.Longitude, S.GeoHash, FALSE,
    S.FileName, S.SourceSystem, NULL, current_timestamp(), S.GeoPointNaturalKey, S.HASH_ID
  )
""")
