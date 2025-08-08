# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC 2. Market Share by Make, Model, EV Type
# MAGIC 💡Business Question
# MAGIC “Which manufacturers and models dominate the EV landscape over time?”
# MAGIC 📊 Use this for pie charts, stacked bars, or trend lines grouped by model, make, and type.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW EVLytics_gold.vw_EVMarketShareAgg AS
# MAGIC SELECT 
# MAGIC     D.Year,
# MAGIC     V.Make,
# MAGIC     V.Model,
# MAGIC     f.ElectricVehicleType,
# MAGIC     COUNT(*) AS TotalRegistrations
# MAGIC FROM EVLytics_gold.Fact_ElectricVehicleRegistrations F
# MAGIC JOIN dbo.DimDate D ON F.DateKey = D.Date_ID
# MAGIC JOIN EVLytics_gold.DimVehicle V ON F.VehicleKey = V.VehicleKey
# MAGIC GROUP BY D.Year, V.Make, V.Model, f.ElectricVehicleType
# MAGIC ORDER BY TotalRegistrations DESC ;

# COMMAND ----------

#### 