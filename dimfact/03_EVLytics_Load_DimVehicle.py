# Databricks notebook source
# MAGIC %md
# MAGIC Step I - Deduplication Logic

# COMMAND ----------

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

            -- Natural Key: stable ID for the vehicle combo
            sha2(concat_ws('|', 
                COALESCE(Make, 'UNKNOWN'), 
                COALESCE(Model, 'UNKNOWN'), 
                COALESCE(ModelYear, '-1')
            ), 256) AS VehicleNaturalKey,

            -- For change detection
            sha2(concat_ws('|',
                COALESCE(Make, 'UNKNOWN'), 
                COALESCE(Model, 'UNKNOWN'), 
                COALESCE(ModelYear, '-1')
            ), 256) AS HASH_ID,

            -- Deduplication
            ROW_NUMBER() OVER (
                PARTITION BY 
                    sha2(concat_ws('|', COALESCE(Make, 'UNKNOWN'), COALESCE(Model, 'UNKNOWN'), COALESCE(ModelYear, '-1')), 256)
                ORDER BY current_timestamp()
            ) AS row_num
        FROM evlytics_silver.vehiclepopulation
        WHERE Make IS NOT NULL AND Model IS NOT NULL AND ModelYear IS NOT NULL
        AND FileName NOT IN (SELECT DISTINCT FileName FROM evlytics_gold.DimVehicle) -- NB:REPLACE this with IngestionDate watermark framework
    ) AS deduped
    WHERE row_num = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Step II -  Register as Temp View

# COMMAND ----------

dimVehicle_df = dimVehicle_df.drop("row_num")
dimVehicle_df.createOrReplaceTempView("vw_DimVehicle")

# COMMAND ----------

# MAGIC %md
# MAGIC Step III - Use MERGE from the View into Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO evlytics_gold.DimVehicle AS T
# MAGIC USING vw_DimVehicle AS S
# MAGIC ON T.VehicleNaturalKey = S.VehicleNaturalKey
# MAGIC
# MAGIC WHEN MATCHED AND S.HASH_ID <> T.HASH_ID THEN
# MAGIC   UPDATE SET
# MAGIC     T.Make = S.Make,
# MAGIC     T.Model = S.Model,
# MAGIC     T.ModelYear = S.ModelYear,
# MAGIC     T.FileName = S.FileName,
# MAGIC     T.SourceSystem = S.SourceSystem,
# MAGIC     T.UpdatedDate = current_timestamp(),
# MAGIC     T.HASH_ID = S.HASH_ID
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     Make, Model, ModelYear, FileName, SourceSystem, UpdatedDate, InsertedDate, VehicleNaturalKey, HASH_ID
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     S.Make, S.Model, S.ModelYear, S.FileName, S.SourceSystem, NULL, current_timestamp(), S.VehicleNaturalKey, S.HASH_ID
# MAGIC   );