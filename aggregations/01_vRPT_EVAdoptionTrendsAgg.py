# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ####  EV Adoption Over Time by County, City, Zip
# MAGIC #### 💡Business Question
# MAGIC “How is EV adoption trending over time across counties, cities, and zip codes?”
# MAGIC 📈 Use this to build line charts and area maps over time by location.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW EVLytics_gold.vRPT_EVAdoptionTrendsAgg AS
# MAGIC SELECT 
# MAGIC     D.Date,
# MAGIC     L.County,
# MAGIC     L.City,
# MAGIC     L.PostalCode,
# MAGIC     COUNT(*) AS TotalRegistrations
# MAGIC FROM EVLytics_gold.Fact_ElectricVehicleRegistrations F
# MAGIC JOIN dbo.DimDate D ON F.DateKey = D.Date_ID
# MAGIC JOIN EVLytics_gold.DimLocation L ON F.LocationKey = L.LocationKey
# MAGIC GROUP BY D.Date, L.County, L.City, L.PostalCode
# MAGIC ORDER BY TotalRegistrations DESC ;

# COMMAND ----------

#### 