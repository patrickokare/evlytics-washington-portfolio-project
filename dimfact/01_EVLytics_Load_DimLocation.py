# Databricks notebook source
# MAGIC %md
# MAGIC Step I - Deduplication Logic

# COMMAND ----------

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
        WHERE FileName NOT IN (SELECT DISTINCT FileName FROM evlytics_gold.DimLocation) -- NB:REPLACE this with IngestionDate watermark framework table  is in place.
    ) AS deduped
    WHERE row_num = 1;
    """)


# COMMAND ----------

# MAGIC %md
# MAGIC Step II -  Register as Temp View

# COMMAND ----------

dimLocation_df = dimLocation_df.drop("row_num")
dimLocation_df.createOrReplaceTempView("vw_DimLocation")

# COMMAND ----------

# MAGIC %md
# MAGIC Step III - Use MERGE from the View into Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Merge for SCD2 with Hashed Natural Key
# MAGIC
# MAGIC MERGE INTO evlytics_gold.DimLocation AS target
# MAGIC USING vw_DimLocation AS source
# MAGIC ON target.LocationNaturalKey = source.LocationNaturalKey AND target.IsCurrent = 1
# MAGIC
# MAGIC -- Type 2 Change: Only when HASH_ID is different
# MAGIC WHEN MATCHED AND 
# MAGIC     (source.HASH_ID <> target.HASH_ID 
# MAGIC     
# MAGIC     )THEN
# MAGIC     UPDATE SET
# MAGIC         target.IsCurrent = 0,
# MAGIC         target.EffectiveEndDate = current_date() - 1,
# MAGIC         target.UpdatedDate = current_timestamp()
# MAGIC
# MAGIC -- Insert new records (new or updated)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         County,
# MAGIC         City,
# MAGIC         State,
# MAGIC         PostalCode,
# MAGIC         CensusTract2020,
# MAGIC         LegislativeDistrict,
# MAGIC         FileName,
# MAGIC         EffectiveStartDate,
# MAGIC         EffectiveEndDate,
# MAGIC         IsCurrent,
# MAGIC         SourceSystem,
# MAGIC         UpdatedDate,
# MAGIC         InsertedDate,
# MAGIC         LocationNaturalKey,
# MAGIC         HASH_ID
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.County,
# MAGIC         source.City,
# MAGIC         source.State,
# MAGIC         source.PostalCode,
# MAGIC         source.CensusTract2020,
# MAGIC         source.LegislativeDistrict,
# MAGIC         source.FileName,
# MAGIC         source.EffectiveStartDate,
# MAGIC         source.EffectiveEndDate,
# MAGIC         source.IsCurrent,
# MAGIC         source.SourceSystem,
# MAGIC         source.UpdatedDate,
# MAGIC         source.InsertedDate,
# MAGIC         source.LocationNaturalKey,
# MAGIC         source.HASH_ID
# MAGIC     );