from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, round, datediff, coalesce, broadcast
import argparse

def process_car_rental_data(args):

    spark = SparkSession.builder \
        .appName("CarRentalDataProcessing") \
        .getOrCreate()

    # Define S3 file path based on execution date
    s3_file_path = f"s3://rootshailesh1/car_rental_data/car_rental_daily_data/car_rental_{args.date}.json"

    # Read raw JSON data
    raw_df = spark.read.option("multiline", "true").json(s3_file_path)

    # Data validation
    validated_df = raw_df.filter(
        col("rental_id").isNotNull() & 
        col("customer_id").isNotNull() & 
        col("car.make").isNotNull() & 
        col("car.model").isNotNull() & 
        col("car.year").isNotNull() & 
        col("rental_period.start_date").isNotNull() & 
        col("rental_period.end_date").isNotNull() & 
        col("rental_location.pickup_location").isNotNull() & 
        col("rental_location.dropoff_location").isNotNull() & 
        col("amount").isNotNull() & 
        col("quantity").isNotNull()
    )

    # Transformations
    transformed_df = validated_df.withColumn(
        "rental_duration_days", 
        datediff(col("rental_period.end_date"), col("rental_period.start_date"))
    ).withColumn(
        "total_rental_amount", 
        col("amount") * col("quantity")
    ).withColumn(
        "average_daily_rental_amount", 
        round(col("total_rental_amount") / col("rental_duration_days"), 2)
    ).withColumn(
        "is_long_rental", 
        when(col("rental_duration_days") > 7, lit(1)).otherwise(lit(0))
    )

    # Build Snowflake options directly from passed arguments
    snowflake_options = {
        "sfURL": args.sf_url,
        "sfUser": args.sf_user,
        "sfPassword": args.sf_password,
        "sfDatabase": args.sf_database,
        "sfSchema": args.sf_schema,
        "sfWarehouse": args.sf_warehouse,
        "sfRole": args.sf_role
    }

    SNOWFLAKE_SOURCE_NAME = "snowflake"

    # Load Dimension Tables from Snowflake
    car_dim_df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_options).option("dbtable", "car_dim").load()
    location_dim_df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_options).option("dbtable", "location_dim").load()
    date_dim_df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_options).option("dbtable", "date_dim").load()
    customer_dim_df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_options).option("dbtable", "customer_dim").load()

    # Join logic - Join all dimensions using Broadcast Left Joins
fact_df = (
    transformed_df.alias("raw")
    .join(
        broadcast(car_dim_df).alias("car"),
        (col("raw.car.make") == col("car.make"))
        & (col("raw.car.model") == col("car.model"))
        & (col("raw.car.year") == col("car.year")),
        "left",
    )
    .join(
        broadcast(location_dim_df).alias("pickup_loc"),
        col("raw.rental_location.pickup_location")
        == col("pickup_loc.location_name"),
        "left",
    )
    .join(
        broadcast(location_dim_df).alias("dropoff_loc"),
        col("raw.rental_location.dropoff_location")
        == col("dropoff_loc.location_name"),
        "left",
    )
    .join(
        broadcast(date_dim_df).alias("start_date_dim"),
        col("raw.rental_period.start_date") == col("start_date_dim.date"),
        "left",
    )
    .join(
        broadcast(date_dim_df).alias("end_date_dim"),
        col("raw.rental_period.end_date") == col("end_date_dim.date"),
        "left",
    )
    .join(
        broadcast(customer_dim_df).alias("cust"),
        col("raw.customer_id") == col("cust.customer_id"),
        "left",
    )
)

# Extract final Star Schema columns and coalesce missing keys to -1 (UNKNOWN)
final_fact_df = fact_df.select(
    col("raw.rental_id").alias("rental_id"),
    coalesce(col("cust.customer_key"), lit(-1)).alias("customer_key"),
    coalesce(col("car.car_key"), lit(-1)).alias("car_key"),
    coalesce(col("pickup_loc.location_key"), lit(-1)).alias(
        "pickup_location_key"
    ),
    coalesce(col("dropoff_loc.location_key"), lit(-1)).alias(
        "dropoff_location_key"
    ),
    coalesce(col("start_date_dim.date_key"), lit(-1)).alias("start_date_key"),
    coalesce(col("end_date_dim.date_key"), lit(-1)).alias("end_date_key"),
    col("raw.amount").alias("amount"),
    col("raw.quantity").alias("quantity"),
    col("raw.rental_duration_days").alias("rental_duration_days"),
    col("raw.total_rental_amount").alias("total_rental_amount"),
    col("raw.average_daily_rental_amount").alias(
        "average_daily_rental_amount"
    ),
    col("raw.is_long_rental").alias("is_long_rental"),
)

    # Write Fact Table back to Snowflake
    fact_df.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**snowflake_options) \
        .option("dbtable", "rentals_fact") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process car rental data.')
    parser.add_argument('--date', type=str, required=True)
    parser.add_argument('--sf_url', type=str, required=True)
    parser.add_argument('--sf_user', type=str, required=True)
    parser.add_argument('--sf_password', type=str, required=True)
    parser.add_argument('--sf_database', type=str, required=True)
    parser.add_argument('--sf_schema', type=str, required=True)
    parser.add_argument('--sf_warehouse', type=str, required=True)
    parser.add_argument('--sf_role', type=str, required=True)
    
    args = parser.parse_args()
    process_car_rental_data(args)
