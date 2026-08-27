# Project Car Rentals Batch Ingestion

**Tech Stack:** AWS Managed Airflow Alternative, AWS EMR Serverless (Spark), Snowflake, Python (Pyspark), SQL

* **Dimensional Model:** Star schema with date_dim, car_dim, location_dim, customer_dim (SCD2), and rentals_fact (rental_id grain)

* **Batch Ingestion (AWS Managed Airflow Alternative):** Daily DAG with parameterized execution_date, idempotent stages, and dependency-managed tasks

* **Customer SCD2:** Close-and-insert pattern (effective_date, end_date, is_current) via MERGE and staged CSV loads

* **Spark Transformations (EMR Serverless):** Duration, total/avg daily amount, long_rental_flag, surrogate key enrichment from dims

* **Snowflake Loads:** INFER_SCHEMA/TEMPLATE table creation, COPY INTO from AWS S3, FK integrity and schema-managed writes

* **KPIs & Analytics:** Revenue by car/location, long-rental ratio, avg duration, tickets by segment; sample SQL for insights

## Workflow Diagram
<img width="619" height="300" alt="image" src="https://github.com/user-attachments/assets/2e8d5ea6-bf58-4502-9e09-9db3f0c5f890" />
  

