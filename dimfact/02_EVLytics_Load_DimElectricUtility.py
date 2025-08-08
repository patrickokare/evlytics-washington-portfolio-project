# Databricks notebook source
# MAGIC %md
# MAGIC Step I - Deduplication Logic

# COMMAND ----------

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

                                    -- Hash for change detection (can help decide when to update)
                                    sha2(concat_ws('|',COALESCE(ElectricUtility, 'UNKNOWN')), 256) AS HASH_ID,

                                    -- Deduplication logic
                                    ROW_NUMBER() OVER (PARTITION BY sha2(COALESCE(ElectricUtility, 'UNKNOWN'), 256) ORDER BY current_timestamp() ) AS row_num

                                  FROM evlytics_silver.vehiclepopulation
                                  WHERE ElectricUtility IS NOT NULL
                                  AND FileName NOT IN (SELECT DISTINCT FileName FROM evlytics_gold.DimElectricUtility) -- NB:REPLACE this with IngestionDate watermark framework
                                ) AS deduped
                              WHERE row_num = 1;
  """

)

# COMMAND ----------

# MAGIC %md
# MAGIC Step II -  Register as Temp View

# COMMAND ----------

dimUtility_df = dimUtility_df.drop("row_num")
dimUtility_df.createOrReplaceTempView("vw_DimUtility")

# COMMAND ----------

# MAGIC %md
# MAGIC Step III - Use MERGE from the View into Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO evlytics_gold.DimElectricUtility AS T
# MAGIC USING vw_DimUtility AS S
# MAGIC ON T.ElectricUtility = S.ElectricUtility
# MAGIC WHEN MATCHED AND ( S.HASH_ID <> T.HASH_ID) THEN
# MAGIC   UPDATE SET
# MAGIC     T.ElectricUtility = S.ElectricUtility,
# MAGIC     T.FileName = S.FileName,
# MAGIC     T.SourceSystem = S.SourceSystem,
# MAGIC     T.UpdatedDate = current_timestamp(),
# MAGIC     T.HASH_ID = S.HASH_ID
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (ElectricUtility, FileName, SourceSystem, UpdatedDate, InsertedDate, ElectricUtilityNaturalKey, HASH_ID)
# MAGIC   VALUES (S.ElectricUtility, S.FileName, S.SourceSystem, NULL, current_timestamp(), S.ElectricUtilityNaturalKey, S.HASH_ID)