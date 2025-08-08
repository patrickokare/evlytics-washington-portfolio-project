# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ####  5. EV Density by Legislative District (w/ Redistricting)
# MAGIC 💡Business Question
# MAGIC “Which legislative districts had highest EV concentration — before and after redistricting?”
# MAGIC 🗺️ Use with slicers for “Pre-2020 Redistricting” vs “Post-2020” to compare policy impacts.
# MAGIC “Which legislative districts had the highest EV concentration — and how has that changed across different district versions (using EffectiveStartDate/EndDate as proxies for redistricting)?”
# MAGIC This view still supports time-based analysis even without DistrictVersion, because you can slice by date of registration vs. district’s effective dates.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW EVLytics_gold.vw_EVDensityByDistrictAgg AS
# MAGIC SELECT 
# MAGIC     LD.LegislativeDistrictNumber,
# MAGIC     LD.LegislativeDistrictName,
# MAGIC     LD.EffectiveStartDate,
# MAGIC     LD.EffectiveEndDate,
# MAGIC     COUNT(*) AS TotalRegistrations
# MAGIC FROM EVLytics_gold.Fact_ElectricVehicleRegistrations F
# MAGIC JOIN EVLytics_gold.DimLegislativeDistrict LD 
# MAGIC   ON F.LegislativeDistrictKey = LD.LegislativeDistrictKey
# MAGIC GROUP BY 
# MAGIC     LD.LegislativeDistrictNumber,
# MAGIC     LD.LegislativeDistrictName,
# MAGIC     LD.EffectiveStartDate, 
# MAGIC     LD.EffectiveEndDate
# MAGIC     ORDER BY  LD.LegislativeDistrictNumber ASC ;

# COMMAND ----------

#### 