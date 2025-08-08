# Databricks notebook source
# MAGIC %md
# MAGIC Step I - Deduplication Logic

# COMMAND ----------

dimLegislativeDistrict_df = spark.sql("""
   -- Reused in %sql
SELECT *
FROM (
    SELECT
        LD.LegislativeDistrictNumber,
        LD.LegislativeDistrictName,
        LD.County,
        VP.State,
        VP.FileName,
        VP.source_system AS SourceSystem,
        NULL AS UpdatedDate,
        current_timestamp() AS InsertedDate,

        current_date() AS EffectiveStartDate,
        NULL AS EffectiveEndDate,
        TRUE AS IsCurrent,

        -- Natural Key: stable across versions
        sha2(COALESCE(CAST(LD.LegislativeDistrictNumber AS STRING), 'UNKNOWN'), 256) AS LegislativeDistrictNaturalKey,

        -- Hash Key: Detects field-level change
        sha2(
            COALESCE(CAST(LD.LegislativeDistrictNumber AS STRING), 'UNKNOWN') || 
            COALESCE(LD.LegislativeDistrictName, 'UNKNOWN') || 
            COALESCE(LD.County, 'UNKNOWN') || 
            COALESCE(VP.State, 'UNKNOWN'),
        256) AS HASH_ID,

        ROW_NUMBER() OVER (
            PARTITION BY LD.LegislativeDistrictNumber
            ORDER BY current_timestamp()
        ) AS row_num

    FROM evlytics_silver.vehiclepopulation VP
    LEFT JOIN evlytics_bronze.legislativedistrictlookup LD 
        ON VP.LegislativeDistrict = LD.LegislativeDistrictNumber
    WHERE State = 'WA' 
      AND LegislativeDistrict IS NOT NULL
    AND VP.FileName NOT IN (SELECT DISTINCT FileName FROM evlytics_gold.DimLegislativeDistrict)
) deduped
WHERE row_num = 1;
""")


# COMMAND ----------

# MAGIC %md
# MAGIC Step II -  Register as Temp View

# COMMAND ----------

dimLegislativeDistrict_df = dimLegislativeDistrict_df.drop("row_num")
dimLegislativeDistrict_df.createOrReplaceTempView("vw_DimLegislativeDistrict")

# COMMAND ----------

# MAGIC %md
# MAGIC Step III - Use MERGE from the View into Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO evlytics_gold.DimLegislativeDistrict AS target
# MAGIC USING vw_DimLegislativeDistrict AS source
# MAGIC ON target.LegislativeDistrictNaturalKey = source.LegislativeDistrictNaturalKey
# MAGIC AND target.IsCurrent = TRUE
# MAGIC
# MAGIC --  Type 2 change: Invalidate current record
# MAGIC WHEN MATCHED AND source.HASH_ID <> target.HASH_ID THEN
# MAGIC   UPDATE SET
# MAGIC     target.IsCurrent = FALSE,
# MAGIC     target.EffectiveEndDate = current_date() - 1,
# MAGIC     target.UpdatedDate = current_timestamp()
# MAGIC
# MAGIC --  New record (either new district or changed version)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     LegislativeDistrictNumber,
# MAGIC     LegislativeDistrictName,
# MAGIC     County,
# MAGIC     State,
# MAGIC     FileName,
# MAGIC     SourceSystem,
# MAGIC     UpdatedDate,
# MAGIC     InsertedDate,
# MAGIC     EffectiveStartDate,
# MAGIC     EffectiveEndDate,
# MAGIC     IsCurrent,
# MAGIC     LegislativeDistrictNaturalKey,
# MAGIC     HASH_ID
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.LegislativeDistrictNumber,
# MAGIC     source.LegislativeDistrictName,
# MAGIC     source.County,
# MAGIC     source.State,
# MAGIC     source.FileName,
# MAGIC     source.SourceSystem,
# MAGIC     NULL,
# MAGIC     current_timestamp(),
# MAGIC     current_date(),
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     source.LegislativeDistrictNaturalKey,
# MAGIC     source.HASH_ID
# MAGIC   );