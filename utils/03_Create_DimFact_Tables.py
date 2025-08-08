# Databricks notebook source
# %sql
# CREATE TABLE IF NOT EXISTS evlytics_gold.DimLocation (
#     LocationKey BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
#     County STRING,
#     City STRING,
#     State STRING,
#     PostalCode INT,
#     CensusTract2020 STRING,
#     LegislativeDistrict INT,
#     FileName STRING,
#     EffectiveStartDate TIMESTAMP,
#     EffectiveEndDate TIMESTAMP,
#     IsCurrent BOOLEAN,
#     SourceSystem STRING,
#     UpdatedDate TIMESTAMP,
#     InsertedDate TIMESTAMP,
#     LocationNaturalKey STRING,
#     HASH_ID STRING
# )
# USING DELTA
# LOCATION "/mnt/datamladls26/lake/databricks/gold/transportation/evlytics-integration/dimlocation";

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS evlytics_gold.DimElectricUtility (
#     UtilityKey BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
#     ElectricUtility STRING,
#     FileName STRING,
#     SourceSystem STRING,
#     UpdatedDate TIMESTAMP,
#     InsertedDate TIMESTAMP,
#     ElectricUtilityNaturalKey STRING,
#     HASH_ID STRING
# )
# USING DELTA
# LOCATION "/mnt/datamladls26/lake/databricks/gold/transportation/evlytics-integration/dimelectricutility";

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS evlytics_gold.DimVehicle (
#     VehicleKey BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
#     Make STRING,
#     Model STRING,
#     ModelYear INT,
#     FileName STRING,
#     SourceSystem STRING,
#     UpdatedDate TIMESTAMP,
#     InsertedDate TIMESTAMP,
#     VehicleNaturalKey STRING,
#     HASH_ID STRING
# )
# USING DELTA
# LOCATION '/mnt/datamladls26/lake/databricks/gold/transportation/evlytics-integration/dimvehicle'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS evlytics_gold.DimGeoPoint (
# MAGIC     GeoPointKey BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     VehicleLocation STRING,
# MAGIC     Latitude DOUBLE,
# MAGIC     Longitude DOUBLE,
# MAGIC     GeoHash STRING,
# MAGIC     ZipCentroidFlag BOOLEAN,
# MAGIC     FileName STRING,
# MAGIC     SourceSystem STRING,
# MAGIC     UpdatedDate TIMESTAMP,
# MAGIC     InsertedDate TIMESTAMP,
# MAGIC     GeoPointNaturalKey STRING,
# MAGIC     HASH_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/datamladls26/lake/databricks/gold/transportation/evlytics-integration/dimgeopoint'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS evlytics_gold.DimLegislativeDistrict (
# MAGIC     LegislativeDistrictKey BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     LegislativeDistrictNumber INT,
# MAGIC     LegislativeDistrictName STRING,
# MAGIC     County STRING,
# MAGIC     State STRING,
# MAGIC     FileName STRING,
# MAGIC     SourceSystem STRING,
# MAGIC     EffectiveStartDate TIMESTAMP,
# MAGIC     EffectiveEndDate TIMESTAMP,
# MAGIC     IsCurrent BOOLEAN,
# MAGIC     UpdatedDate TIMESTAMP,
# MAGIC     InsertedDate TIMESTAMP,
# MAGIC     LegislativeDistrictNaturalKey STRING,
# MAGIC     HASH_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/datamladls26/lake/databricks/gold/transportation/evlytics-integration/dimlegislativedistrict';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS evlytics_gold.Fact_ElectricVehicleRegistrations (
# MAGIC     EVRegKey BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC     SnapshotDate TIMESTAMP,
# MAGIC     DateKey INT,
# MAGIC     LocationKey BIGINT,
# MAGIC     VehicleKey BIGINT,
# MAGIC     UtilityKey BIGINT,
# MAGIC     GeoPointKey BIGINT,
# MAGIC     LegislativeDistrictKey BIGINT,
# MAGIC     VehicleIdentificationNumber STRING,
# MAGIC     ElectricVehicleType STRING,
# MAGIC     CAFVEligibility STRING,
# MAGIC     ElectricRange INT,
# MAGIC     BaseMSRP INT,
# MAGIC     DOLVehicleID INT,
# MAGIC     IsWashingtonState STRING,
# MAGIC     FileName STRING,
# MAGIC     SourceSystem STRING,
# MAGIC     UpdatedDate TIMESTAMP,
# MAGIC     InsertedDate TIMESTAMP,
# MAGIC     HASH_ID STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/datamladls26/lake/databricks/gold/transportation/evlytics-integration/fact_electricvehicleregistrations'