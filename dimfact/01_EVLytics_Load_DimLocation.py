# Databricks Notebook
# Title: DimLocation - SCD Type 2 with Deduplication
# Author: Patrick Okare
# Date: 2025-08-08
# Description: This notebook applies SCD Type 2 logic on location data 
# using a hashed natural key and deduplication framework.

# Step I - Deduplication Logic
dimLocation_df = spark.sql(""" 
    SELECT * 
    FROM (
        SELECT
            County,
            City,
            State,
            PostalCode,
            source.`2020CensusTract` AS CensusTract2020,
            COALESCE(LegislativeDistrict, -1) AS LegislativeDistrict,
            FileName,
            current_date() AS EffectiveStartDate,
            NULL AS EffectiveEndDate,
            1 AS IsCurrent,
            source_system AS SourceSystem,
            NULL AS UpdatedDate,
            current_timestamp() AS InsertedDate,

            -- Business grain key
            sha2(concat_ws('|',
                COALESCE(County, 'UNKNOWN'),
                COALESCE(City, 'UNKNOWN'),
                COALESCE(State, 'UNKNOWN'),
                COALESCE(PostalCode, '-1'),
                COALESCE(`2020CensusTract`, 'UNKNOWN')
            ), 256) AS LocationNaturalKey,

            -- Change detection
            sha2(concat_ws('|',
                COALESCE(County, 'UNKNOWN'),
                COALESCE(City, 'UNKNOWN'),
                COALESCE(State, 'UNKNOWN'),
                COALESCE(PostalCode, '-1'),
                COALESCE(LegislativeDistrict, '-1'),
                COALESCE(`2020CensusTract`, 'UNKNOWN')
            ), 256) AS HASH_ID,

            -- Row number for deduplication
            ROW_NUMBER() OVER (
                PARTITION BY sha2(concat_ws('|',
                    COALESCE(County, 'UNKNOWN'),
                    COALESCE(City, 'UNKNOWN'),
                    COALESCE(State, 'UNKNOWN'),
                    COALESCE(PostalCode, '-1'),
                    COALESCE(`2020CensusTract`, 'UNKNOWN')
                ), 256)
                ORDER BY current_timestamp()
            ) AS row_num

        FROM evlytics_silver.vehiclepopulation AS source 
        WHERE FileName NOT IN (
            SELECT DISTINCT FileName 
            FROM evlytics_gold.DimLocation
        )
    ) AS deduped
    WHERE row_num = 1
""")

# Step II - Register as Temp View
dimLocation_df = dimLocation_df.drop("row_num")
dimLocation_df.createOrReplaceTempView("vw_DimLocation")

# Step III - Merge View into Gold Layer (SCD Type 2)
spark.sql("""
    MERGE INTO evlytics_gold.DimLocation AS target
    USING vw_DimLocation AS source
    ON target.LocationNaturalKey = source.LocationNaturalKey AND target.IsCurrent = 1

    WHEN MATCHED AND (source.HASH_ID <> target.HASH_ID) THEN
        UPDATE SET
            target.IsCurrent = 0,
            target.EffectiveEndDate = current_date() - 1,
            target.UpdatedDate = current_timestamp()

    WHEN NOT MATCHED THEN
        INSERT (
            County,
            City,
            State,
            PostalCode,
            CensusTract2020,
            LegislativeDistrict,
            FileName,
            EffectiveStartDate,
            EffectiveEndDate,
            IsCurrent,
            SourceSystem,
            UpdatedDate,
            InsertedDate,
            LocationNaturalKey,
            HASH_ID
        )
        VALUES (
            source.County,
            source.City,
            source.State,
            source.PostalCode,
            source.CensusTract2020,
            source.LegislativeDistrict,
            source.FileName,
            source.EffectiveStartDate,
            source.EffectiveEndDate,
            source.IsCurrent,
            source.SourceSystem,
            source.UpdatedDate,
            source.InsertedDate,
            source.LocationNaturalKey,
            source.HASH_ID
        )
""")
