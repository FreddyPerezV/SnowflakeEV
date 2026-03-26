🚗 Snowflake EV Analytics Demo
📌 Overview
This project demonstrates an end-to-end data pipeline and analytics solution built on Snowflake using an Electric Vehicles (EV) dataset.

The solution follows a modern data architecture pattern (Medallion Architecture) and showcases how data can be ingested, transformed, modeled, and exposed for analytics and sharing.

🎯 Objectives
Build a scalable data pipeline in Snowflake
Apply Medallion Architecture (Bronze, Silver, Gold)
Implement data quality, governance, and observability
Design a semantic layer for analytics and AI readiness
Enable secure data sharing across consumers
🏗️ Architecture
The solution is structured in the following layers:

🔹 Bronze Layer
Raw data ingestion (JSON)
Stores original data for traceability
Minimal transformations
🔹 Silver Layer
Data cleansing and normalization
Deduplication and standardization
Data quality validations
🔹 Gold Layer
Dimensional modeling (Fact & Dimensions)
Business-ready datasets
Aggregations and KPIs
📊 Data Model
The Gold layer includes:

FACT_EV_REGISTRATIONS
DIM_LOCATION
DIM_MAKE_MODEL
Derived views:

EV adoption trends
State-level performance
Vehicle type insights (BEV vs PHEV)
🔗 Data Sharing
Secure data sharing is implemented using Snowflake Shares:

EV_DEMO_SHARE
Consumer-ready views exposed securely
No data duplication required
🧠 Semantic Layer
A semantic layer is prepared to enable:

Business-friendly querying
Integration with AI tools (e.g., Cortex Analyst)
Natural language exploration
⚙️ Orchestration & Monitoring
Task-based orchestration
Audit and monitoring scripts
Data freshness and validation checks
📁 Project Structure
/01_setup.sql /02_file_format_and_stage_validation.sql /03_bronze_raw_ingestion.sql /04_bronze_profiling.sql /05_silver_clean_layer.sql /06_silver_data_quality.sql /07_gold_dimensional_model.sql /08_gold_business_views.sql /09_sharing_secure_views.sql /10_semantic_model_prep.sql /11_monitoring_and_audit.sql /12_tasks_and_orchestration.sql /13_bonus_extensions.sql

🚀 How to Run
Create database and schemas
Load raw data into Bronze layer
Execute Silver transformations
Build Gold data model
Create views and semantic layer
Enable sharing and orchestration
💡 Key Features
End-to-end pipeline in Snowflake
Medallion architecture implementation
Secure data sharing
Data quality checks
AI-ready semantic layer
Scalable and modular design
🧪 Future Enhancements
Real-time ingestion (Streaming)
Integration with external APIs
Advanced analytics and ML models
CI/CD pipeline integration
Streamlit application for visualization
👨‍💻 Author
Freddy Perez

🧾 Notes
This project is intended for demonstration purposes and showcases best practices in modern data engineering using Snowflake.
