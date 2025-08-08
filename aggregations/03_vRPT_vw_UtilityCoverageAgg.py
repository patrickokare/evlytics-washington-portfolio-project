# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC 3. Utility Service Area vs EV Registrations
# MAGIC 💡Business Question
# MAGIC “Are utilities keeping up with EV demand in their service areas?”
# MAGIC ⚡ Powerful for comparing utility readiness by EV volume in each county.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW EVLytics_gold.vw_UtilityCoverageAgg AS
# MAGIC SELECT 
# MAGIC     EU.ElectricUtility AS ElectricUtilityName,
# MAGIC     -- EU.UtilityType,
# MAGIC     f.electricVehicleType AS ElectricVehicleType,
# MAGIC     L.County,
# MAGIC     COUNT(*) AS TotalEVsInServiceArea
# MAGIC FROM EVLytics_gold.Fact_ElectricVehicleRegistrations F
# MAGIC JOIN EVLytics_gold.DimElectricUtility EU ON F.UtilityKey = EU.UtilityKey
# MAGIC JOIN EVLytics_gold.DimLocation L ON F.LocationKey = L.LocationKey
# MAGIC GROUP BY EU.ElectricUtility, L.County, f.electricVehicleType
# MAGIC ORDER BY TotalEVsInServiceArea DESC ;

# COMMAND ----------

#### 