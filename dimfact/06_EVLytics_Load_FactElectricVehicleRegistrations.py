# Databricks notebook source
# %sql
# -- Enable column mapping on the Delta table
# ALTER TABLE skyflights_gold.factflights 
# SET TBLPROPERTIES (
#   'delta.minReaderVersion' = '2', 
#   'delta.minWriterVersion' = '5', 
#   'delta.columnMapping.mode' = 'name'
# );

# -- Rename the column after enabling column mapping
# ALTER TABLE skyflights_gold.factflights 
# RENAME COLUMN Carrier_Key TO Carrier_Airline_Key;

# COMMAND ----------

fact_electricVehicleRegistrations = spark.sql("""
SELECT 
    VP.ingestion_date AS SnapshotDate, -- snapshot date from system
    COALESCE(DD.Date_ID, 19000101) AS DateKey,
    COALESCE(DL.LocationKey, -1) AS LocationKey,
    COALESCE(DV.VehicleKey, -1) AS VehicleKey,
    COALESCE(DEU.UtilityKey, -1) AS UtilityKey,
    COALESCE(DG.GeoPointKey, -1) AS GeoPointKey,
    COALESCE(DLD.LegislativeDistrictKey, -1) AS LegislativeDistrictKey,

    VP.VehicleIdentificationNumber,
    VP.ElectricVehicleType,
    VP.CAFVEligibility,
    VP.ElectricRange,
    VP.BaseMSRP,
    VP.DOLVehicleID,
    VP.IsWashingtonState,
    VP.FileName,
    VP.source_system AS SourceSystem,

    NULL AS UpdatedDate,
    current_timestamp() AS InsertedDate,

    sha2(concat_ws('||',
        COALESCE(CAST(DD.Date_ID AS STRING), ''),
        COALESCE(CAST(DL.LocationKey AS STRING), ''),
        COALESCE(CAST(DV.VehicleKey AS STRING), ''),
        COALESCE(CAST(DEU.UtilityKey AS STRING), ''),
        COALESCE(CAST(DG.GeoPointKey AS STRING), ''),
        COALESCE(CAST(DLD.LegislativeDistrictKey AS STRING), ''),
        COALESCE(VP.VehicleIdentificationNumber, ''),
        COALESCE(VP.ElectricVehicleType, ''),
        COALESCE(VP.CAFVEligibility, ''),
        COALESCE(CAST(VP.ElectricRange AS STRING), ''),
        COALESCE(CAST(VP.BaseMSRP AS STRING), ''),
        COALESCE(VP.DOLVehicleID, ''),
        COALESCE(VP.IsWashingtonState, '')
        -- COALESCE(VP.FileName, ''),
        -- COALESCE(VP.source_system, ''),
        -- CAST(VP.ingestion_date AS STRING)
    ), 256) AS HASH_ID

FROM EVLytics_silver.vehiclepopulation VP
LEFT JOIN EVLytics_gold.DimVehicle DV ON DV.Make = VP.Make AND DV.Model = VP.Model AND DV.ModelYear = VP.ModelYear
LEFT JOIN EVLytics_gold.DimGeoPoint DG ON DG.VehicleLocation = VP.VehicleLocation
LEFT JOIN evlytics_gold.dimlegislativedistrict DLD ON DLD.LegislativeDistrictNumber = VP.LegislativeDistrict AND DLD.IsCurrent = TRUE
LEFT JOIN dbo.DimDate DD ON CAST(DD.Date as date) = CAST(VP.ingestion_date as date)
LEFT JOIN EVLytics_gold.DimElectricUtility DEU ON DEU.ElectricUtility = VP.ElectricUtility
LEFT JOIN EVLytics_gold.DimLocation DL ON DL.County = VP.County AND DL.City = VP.City AND DL.State = VP.State 
    AND DL.PostalCode = VP.PostalCode AND DL.CensusTract2020 = VP.2020CensusTract AND DL.IsCurrent = TRUE
WHERE VP.IsWashingtonState = 'WASHINGTON STATE'
""")


# COMMAND ----------

fact_electricVehicleRegistrations.createOrReplaceTempView("vw_fact_ElectricVehicleRegistrations")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO evlytics_gold.Fact_ElectricVehicleRegistrations AS tgt
# MAGIC USING vw_fact_ElectricVehicleRegistrations AS src
# MAGIC ON tgt.DateKey = src.DateKey 
# MAGIC AND tgt.LocationKey = src.LocationKey 
# MAGIC AND tgt.VehicleKey = src.VehicleKey 
# MAGIC AND tgt.UtilityKey = src.UtilityKey 
# MAGIC AND tgt.GeoPointKey = src.GeoPointKey 
# MAGIC AND tgt.LegislativeDistrictKey = src.LegislativeDistrictKey
# MAGIC AND tgt.SnapshotDate = src.SnapshotDate
# MAGIC AND tgt.DOLVehicleID = src.DOLVehicleID
# MAGIC
# MAGIC WHEN MATCHED AND (tgt.HASH_ID <> src.HASH_ID) THEN UPDATE SET
# MAGIC     tgt.UpdatedDate = current_timestamp(),
# MAGIC     tgt.SnapshotDate = src.SnapshotDate,
# MAGIC     tgt.DateKey = src.DateKey,
# MAGIC     tgt.LocationKey = src.LocationKey,
# MAGIC     tgt.VehicleKey = src.VehicleKey,
# MAGIC     tgt.UtilityKey = src.UtilityKey,
# MAGIC     tgt.GeoPointKey = src.GeoPointKey,
# MAGIC     tgt.LegislativeDistrictKey = src.LegislativeDistrictKey,
# MAGIC     tgt.VehicleIdentificationNumber = src.VehicleIdentificationNumber,
# MAGIC     tgt.ElectricVehicleType = src.ElectricVehicleType,
# MAGIC     tgt.CAFVEligibility = src.CAFVEligibility,
# MAGIC     tgt.ElectricRange = src.ElectricRange,
# MAGIC     tgt.BaseMSRP = src.BaseMSRP,
# MAGIC     tgt.DOLVehicleID = src.DOLVehicleID,
# MAGIC     tgt.IsWashingtonState = src.IsWashingtonState,
# MAGIC     tgt.FileName = src.FileName,
# MAGIC     tgt.SourceSystem = src.SourceSystem
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     SnapshotDate, DateKey, LocationKey, VehicleKey, UtilityKey, GeoPointKey,LegislativeDistrictKey, VehicleIdentificationNumber, ElectricVehicleType,CAFVEligibility, ElectricRange, BaseMSRP, DOLVehicleID, IsWashingtonState,FileName, SourceSystem, UpdatedDate, InsertedDate, HASH_ID
# MAGIC )
# MAGIC VALUES (
# MAGIC     src.SnapshotDate, src.DateKey, src.LocationKey, src.VehicleKey, src.UtilityKey, src.GeoPointKey,src.LegislativeDistrictKey, src.VehicleIdentificationNumber, src.ElectricVehicleType,src.CAFVEligibility, src.ElectricRange, src.BaseMSRP, src.DOLVehicleID, src.IsWashingtonState,
# MAGIC     src.FileName, src.SourceSystem, src.UpdatedDate, src.InsertedDate, src.HASH_ID
# MAGIC );