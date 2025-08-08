dimLegislativeDistrict_df = spark.sql("""
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

        sha2(COALESCE(CAST(LD.LegislativeDistrictNumber AS STRING), 'UNKNOWN'), 256) AS LegislativeDistrictNaturalKey,

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

dimLegislativeDistrict_df = dimLegislativeDistrict_df.drop("row_num")
dimLegislativeDistrict_df.createOrReplaceTempView("vw_DimLegislativeDistrict")

MERGE INTO evlytics_gold.DimLegislativeDistrict AS target
USING vw_DimLegislativeDistrict AS source
ON target.LegislativeDistrictNaturalKey = source.LegislativeDistrictNaturalKey
AND target.IsCurrent = TRUE

WHEN MATCHED AND source.HASH_ID <> target.HASH_ID THEN
  UPDATE SET
    target.IsCurrent = FALSE,
    target.EffectiveEndDate = current_date() - 1,
    target.UpdatedDate = current_timestamp()

WHEN NOT MATCHED THEN
  INSERT (
    LegislativeDistrictNumber,
    LegislativeDistrictName,
    County,
    State,
    FileName,
    SourceSystem,
    UpdatedDate,
    InsertedDate,
    EffectiveStartDate,
    EffectiveEndDate,
    IsCurrent,
    LegislativeDistrictNaturalKey,
    HASH_ID
  )
  VALUES (
    source.LegislativeDistrictNumber,
    source.LegislativeDistrictName,
    source.County,
    source.State,
    source.FileName,
    source.SourceSystem,
    NULL,
    current_timestamp(),
    current_date(),
    NULL,
    TRUE,
    source.LegislativeDistrictNaturalKey,
    source.HASH_ID
  );


