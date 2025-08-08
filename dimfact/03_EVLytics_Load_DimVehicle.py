# Author: Patrick Okare
# Date: 2025-08-08
# Script: DimVehicle SCD Merge Logic
# Description: Deduplication and SCD Type 1 logic for the DimVehicle dimension table.

# Step I - Deduplication Logic
dimVehicle_df = spark.sql("""
    SELECT *
    FROM (
        SELECT
            Make,
            Model,
            ModelYear,
            FileName,
            source_system AS SourceSystem,
            NULL AS UpdatedDate,
            current_timestamp() AS InsertedDate,

            -- Natural Key: stable identifier for vehicle make-model-year
            sha2(concat_ws('|', 
                COALESCE(Make, 'UNKNOWN'), 
                COALESCE(Model, 'UNKNOWN'), 
                COALESCE(ModelYear, '-1')
            ), 256) AS VehicleNaturalKey,

            -- Change detection hash
            sha2(concat_ws('|',
                COALESCE(Make, 'UNKNOWN'), 
                COALESCE(Model, 'UNKNOWN'), 
                COALESCE(ModelYear, '-1')
            ), 256) AS HASH_ID,

            -- Deduplication: keep one row per unique key
            ROW_NUMBER() OVER (
                PARTITION BY 
                    sha2(concat_ws('|', COALESCE(Make, 'UNKNOWN'), COALESCE(Model, 'UNKNOWN'), COALESCE(ModelYear, '-1')), 256)
                ORDER BY current_timestamp()
            ) AS row_num
        FROM evlytics_silver.vehiclepopulation
        WHERE Make IS NOT NULL 
          AND Model IS NOT NULL 
          AND ModelYear IS NOT NULL
          AND FileName NOT IN (
              SELECT DISTINCT FileName 
              FROM evlytics_gold.DimVehicle
          ) -- TODO: Replace with watermark strategy
    ) AS deduped
    WHERE row_num = 1
""")

# Step II - Register as Temp View
dimVehicle_df = dimVehicle_df.drop("row_num")
dimVehicle_df.createOrReplaceTempView("vw_DimVehicle")

# Step III - SCD Merge Into Gold Layer
spark.sql("""
    MERGE INTO evlytics_gold.DimVehicle AS T
    USING vw_DimVehicle AS S
    ON T.VehicleNaturalKey = S.VehicleNaturalKey

    WHEN MATCHED AND S.HASH_ID <> T.HASH_ID THEN
        UPDATE SET
            T.Make = S.Make,
            T.Model = S.Model,
            T.ModelYear = S.ModelYear,
            T.FileName = S.FileName,
            T.SourceSystem = S.SourceSystem,
            T.UpdatedDate = current_timestamp(),
            T.HASH_ID = S.HASH_ID

    WHEN NOT MATCHED THEN
        INSERT (
            Make, Model, ModelYear, FileName, SourceSystem, UpdatedDate, InsertedDate, VehicleNaturalKey, HASH_ID
        )
        VALUES (
            S.Make, S.Model, S.ModelYear, S.FileName, S.SourceSystem, NULL, current_timestamp(), S.VehicleNaturalKey, S.HASH_ID
        );
""")
