# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ####  4. Average Range and MSRP by Vehicle Type
# MAGIC 💡Business Question
# MAGIC “Are higher-range vehicles becoming more affordable?”
# MAGIC 💰 Show trends of price vs. range. Good for scatter plots or KPI cards.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW EVLytics_gold.vw_RangeMSRPByTypeAgg AS
# MAGIC SELECT 
# MAGIC     f.electricVehicleType AS ElectricVehicleType,
# MAGIC     D.Year,
# MAGIC     ROUND(AVG(f.ElectricRange), 1) AS AvgElectricRange,
# MAGIC     ROUND(AVG(f.BaseMSRP), 2) AS AvgMSRP
# MAGIC FROM EVLytics_gold.Fact_ElectricVehicleRegistrations F
# MAGIC JOIN dbo.DimDate D ON F.DateKey = D.Date_ID
# MAGIC JOIN EVLytics_gold.DimVehicle V ON F.VehicleKey = V.VehicleKey
# MAGIC GROUP BY f.electricVehicleType, D.Year;

# COMMAND ----------

#### 