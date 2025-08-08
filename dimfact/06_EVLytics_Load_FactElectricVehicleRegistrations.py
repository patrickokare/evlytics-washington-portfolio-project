# Fact Table Creation - Fact_ElectricVehicleRegistrations
# Author: Patrick Okare
# Description: Snapshot fact table creation using SCD Type 1 logic
# Source: EVLytics_silver.vehiclepopulation
# Target: EVLytics_gold.Fact_ElectricVehicleRegistrations

# Step 1 - Transform: Join dimension tables and enrich the source data
fact_electricVehicleRegistrations = spark.sql("""
SELECT 
    VP.ingestion_date AS SnapshotDate, -- snapshot date from system
    COALESCE(DD.Date_ID, 19000101) AS DateKey,
    COALESCE(DL.LocationKey, -1) AS LocationKey,
    COALESCE(DV.VehicleKey, -1) AS VehicleKey,
    COALESCE(DEU.UtilityKey, -1) AS UtilityKey,
    COALESCE(DG.GeoPointKey, -1) AS GeoPointKey,
    COALESCE(DLD.LegislativeDistrictKey, -1) AS LegislativeDistrictKey,

    -- Degenerate dimensions and transaction details
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

    -- HASH_ID for change detection
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
    ), 256) AS HASH_ID

FROM EVLytics_silver.vehiclepopulation VP

-- Join to Dimension Tables
LEFT JOIN EVLytics_gold.DimVehicle DV 
    ON DV.Make = VP.Make AND DV.Model = VP.Model AND DV.ModelYear = VP.ModelYear

LEFT JOIN EVLytics_gold.DimGeoPoint DG 
    ON DG.VehicleLocation = VP.VehicleLocation

LEFT JOIN evlytics_gold.DimLegislativeDistrict DLD 
    ON DLD.LegislativeDistrictNumber = VP.LegislativeDistrict AND DLD.IsCurrent = TRUE

LEFT JOIN dbo.DimDate DD 
    ON CAST(DD.Date as date) = CAST(VP.ingestion_date as date)

LEFT JOIN EVLytics_gold.DimElectricUtility DEU 
    ON DEU.ElectricUtility = VP.ElectricUtility

LEFT JOIN EVLytics_gold.DimLocation DL 
    ON DL.County = VP.County 
    AND DL.City = VP.City 
    AND DL.State = VP.State 
    AND DL.PostalCode = VP.PostalCode 
    AND DL.CensusTract2020 = VP.2020CensusTract 
    AND DL.IsCurrent = TRUE

-- Optional data scope filter
WHERE VP.IsWashingtonState = 'WASHINGTON STATE'
""")

# Step 2 - Register the DataFrame as a Temp View for MERGE operation
fact_electricVehicleRegistrations.createOrReplaceTempView("vw_fact_ElectricVehicleRegistrations")



-- Step 3 - MERGE into Gold Fact Table with HASH-based change detection
MERGE INTO evlytics_gold.Fact_ElectricVehicleRegistrations AS tgt
USING vw_fact_ElectricVehicleRegistrations AS src
ON tgt.DateKey = src.DateKey 
   AND tgt.LocationKey = src.LocationKey 
   AND tgt.VehicleKey = src.VehicleKey 
   AND tgt.UtilityKey = src.UtilityKey 
   AND tgt.GeoPointKey = src.GeoPointKey 
   AND tgt.LegislativeDistrictKey = src.LegislativeDistrictKey
   AND tgt.SnapshotDate = src.SnapshotDate
   AND tgt.DOLVehicleID = src.DOLVehicleID

-- When matched and data has changed, update only
WHEN MATCHED AND tgt.HASH_ID <> src.HASH_ID THEN UPDATE SET
    tgt.UpdatedDate = current_timestamp(),
    tgt.SnapshotDate = src.SnapshotDate,
    tgt.DateKey = src.DateKey,
    tgt.LocationKey = src.LocationKey,
    tgt.VehicleKey = src.VehicleKey,
    tgt.UtilityKey = src.UtilityKey,
    tgt.GeoPointKey = src.GeoPointKey,
    tgt.LegislativeDistrictKey = src.LegislativeDistrictKey,
    tgt.VehicleIdentificationNumber = src.VehicleIdentificationNumber,
    tgt.ElectricVehicleType = src.ElectricVehicleType,
    tgt.CAFVEligibility = src.CAFVEligibility,
    tgt.ElectricRange = src.ElectricRange,
    tgt.BaseMSRP = src.BaseMSRP,
    tgt.DOLVehicleID = src.DOLVehicleID,
    tgt.IsWashingtonState = src.IsWashingtonState,
    tgt.FileName = src.FileName,
    tgt.SourceSystem = src.SourceSystem

-- When new, insert into fact table
WHEN NOT MATCHED THEN INSERT (
    SnapshotDate, DateKey, LocationKey, VehicleKey, UtilityKey, GeoPointKey,
    LegislativeDistrictKey, VehicleIdentificationNumber, ElectricVehicleType,
    CAFVEligibility, ElectricRange, BaseMSRP, DOLVehicleID, IsWashingtonState,
    FileName, SourceSystem, UpdatedDate, InsertedDate, HASH_ID
)
VALUES (
    src.SnapshotDate, src.DateKey, src.LocationKey, src.VehicleKey, src.UtilityKey, src.GeoPointKey,
    src.LegislativeDistrictKey, src.VehicleIdentificationNumber, src.ElectricVehicleType,
    src.CAFVEligibility, src.ElectricRange, src.BaseMSRP, src.DOLVehicleID, src.IsWashingtonState,
    src.FileName, src.SourceSystem, src.UpdatedDate, src.InsertedDate, src.HASH_ID
);
