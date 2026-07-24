# Project Car Rentals Batch Ingestion

**Tech Stack:** AWS Managed Airflow Alternative, AWS EMR Serverless (Spark), Snowflake, Python, SQL

* **Dimensional Model:** Star schema with date_dim, car_dim, location_dim, customer_dim (SCD2), and rentals_fact (rental_id grain)

* **Batch Ingestion (AWS Managed Airflow Alternative):** Daily DAG with parameterized execution_date, idempotent stages, and dependency-managed tasks

* **Customer SCD2:** Close-and-insert pattern (effective_date, end_date, is_current) via MERGE and staged CSV loads

* **Spark Transformations (EMR Serverless):** Duration, total/avg daily amount, long_rental_flag, surrogate key enrichment from dims

* **Snowflake Loads:** INFER_SCHEMA/TEMPLATE table creation, COPY INTO from AWS S3, FK integrity and schema-managed writes

* **KPIs & Analytics:** Revenue by car/location, long-rental ratio, avg duration, tickets by segment; sample SQL for insights

* **Ops & Reliability:** S3, csv_format, retries, logging, and job parameterization for reproducibility
