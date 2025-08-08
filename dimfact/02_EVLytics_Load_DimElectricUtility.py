# Author: Patrick Okra
# Date: 2025-08-08
# Script: DimElectricUtility SCD Merge Logic
# Description: Implements deduplication and SCD Type 1 logic for the DimElectricUtility table using data from the Silver layer.

# Step I - Deduplication Logic
dimUtility_df = spark.sql("""
    SELECT *
    FROM (
        SELECT
            NULL AS UtilityKey,
            ElectricUtility,
            FileName,
            source_system AS SourceSystem,
            NULL AS UpdatedDate,
            current_timestamp() AS InsertedDate,

            -- Natural Key for matching
            sha2(COALESCE(ElectricUtility, 'UNKNOWN'), 256) AS ElectricUtilityNaturalKey,

            -- Hash for change detection (used for update detection)
            sha2(concat_ws('|', COALESCE(ElectricUtility, 'UNKNOWN')), 256) AS HASH_ID,

            -- Deduplication logic
            ROW_NUMBER() OVER (
                PARTITION BY sha2(COALESCE(ElectricUtility, 'UNKNOWN'), 256)
                ORDER BY current_timestamp()
            ) AS row_num

        FROM evlytics_silver.vehiclepopulation
        WHERE ElectricUtility IS NOT NULL
        AND FileName NOT IN (
            SELECT DISTINCT FileName FROM evlytics_gold.DimElectricUtility
        ) -- Replace with a watermark strategy
    ) AS deduped
    WHERE row_num = 1;
""")

# Step II - Register as Temp View
dimUtility_df = dimUtility_df.drop("row_num")
dimUtility_df.createOrReplaceTempView("vw_DimUtility")

# Step III - Merge into Gold Layer (SCD Type 1)
spark.sql("""
    MERGE INTO evlytics_gold.DimElectricUtility AS T
    USING vw_DimUtility AS S
    ON T.ElectricUtility = S.ElectricUtility

    WHEN MATCHED AND S.HASH_ID <> T.HASH_ID THEN
        UPDATE SET
            T.ElectricUtility = S.ElectricUtility,
            T.FileName = S.FileName,
            T.SourceSystem = S.SourceSystem,
            T.UpdatedDate = current_timestamp(),
            T.HASH_ID = S.HASH_ID

    WHEN NOT MATCHED THEN
        INSERT (
            ElectricUtility,
            FileName,
            SourceSystem,
            UpdatedDate,
            InsertedDate,
            ElectricUtilityNaturalKey,
            HASH_ID
        )
        VALUES (
            S.ElectricUtility,
            S.FileName,
            S.SourceSystem,
            NULL,
            current_timestamp(),
            S.ElectricUtilityNaturalKey,
            S.HASH_ID
        );
""")
