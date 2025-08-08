
⚡ EVLytics: WA EV Population Lakehouse Project

This repository contains the full implementation of a modern Electric Vehicle Registration Lakehouse, designed and built using Databricks, Delta Lake, and Kimball Dimensional Modeling principles.

📍 Project Title: EVLytics
🧱 Architecture: Medallion (Bronze → Silver → Gold)
📊 Modelling: Dimensional (Star Schema)
☁️ Platform: Databricks (Lakehouse, Delta Live Tables, Notebooks)

⸻

📁 Project Structure
├── ingestion/           # Raw data ingestion logic (Bronze Layer),  Cleaned & conformed transformations (Silver Layer)
├── dimfact/                # Analytical modeling & aggregations (Gold Layer)
├── docs/                  # Solution Designs
├── aggregations/           # Semantic views for Power BI or dashboards
├── utils/               # Reusable helper functions & constants, Data Quality rules and tests
└── README.md            # Project overview and documentation


⸻

📐 Features
	•	✅ Incremental ingestion with source tracking and schema evolution
	•	✅ Metadata enrichment (SnapshotDate, IngestionDate, SourceSystem)
	•	✅ Comprehensive QA Checks (Nulls, Referentials, Duplicates, Valid Ranges)
	•	✅ Slowly Changing Dimensions (SCD) implementation where applicable
	•	✅ Star Schema modelling optimized for BI tools
	•	✅ Semantic Views for insights like EV adoption trends, market share, and utility coverage

⸻

🔎 Data Overview
	•	Source: Washington State Department of Licensing (WA DOL)
	•	Granularity: Snapshot-level EV registrations
	•	Snapshot Frequency: Monthly
	•	Primary Entities:
	•	Vehicle Details (Make, Model, Electric Range, MSRP, etc.)
	•	Geographic Info (City, County, Zip, Census Tract)
	•	Electric Utilities
	•	Legislative Districts (for policy impact analysis)

⸻

🌟 Semantic Views for Reporting

The following Gold views were created for Power BI / Semantic Layer:
	1.	EV Adoption Trends

CREATE VIEW View_EV_Registrations_OverTime

Track the monthly adoption of EVs, sliced by fuel type, state, and region.

	2.	EV Market Share by Make/Model

CREATE VIEW View_MarketShare_ByMakeModel

Understand top-performing manufacturers and vehicle types.

	3.	EV Coverage by Utility

CREATE VIEW View_EV_Coverage_ByUtility

Evaluate EV distribution by utility service areas.

	4.	EV Range and MSRP Analysis

CREATE VIEW View_EV_Range_MSRP_ByType

Compare electric range and price by EV type (BEV, PHEV, etc.)

	5.	EV Density by District

CREATE VIEW View_EV_Density_ByDistrict

Map EV concentration across legislative districts with historical boundary versions.

⸻

🧠 Insights

This project helps answer questions like:
	•	“Which electric utility areas have the most EVs?”
	•	“How is EV adoption evolving?”
	•	“What are the most common EV models and types registered?”
	•	“How do redistricting changes affect EV representation?”
	•	“What is the average electric range by EV type or county?”

⸻

🧰 Tools & Tech
	•	Platform: Databricks (SQL, Delta Lake, Notebooks)
	•	Language: PySpark, SQL
	•	Architecture: Medallion + Dimensional Modelling (Kimball)
	•	Storage Format: Delta
	•	Reporting: Power BI (Semantic Views)

⸻

📈 Future Enhancements
	•	🔄 Real-time streaming ingestion with Auto Loader
	•	🌎 Geospatial clustering using census data
	•	🧠 Predictive modelling for EV adoption forecasting
	•	🔌 Integration with charging station datasets

⸻

👨‍💻 Author
	•	Patrick Okare.
Data Engineer | Data Storyteller | Building Lakehouses with Purpose

For more portfolio projects, visit (https://github.com/PatrickOkare)


 
