from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)
from pyspark.sql.functions import from_json, col, round, when, window, sum
import time

spark = (
    SparkSession.builder
    .appName("EcommerceKafkaStreaming")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("====================================")
print("Kafka -> PySpark Streaming Started")
print("====================================")


# 1. Define the JSON schema
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])


# 2. Read from Kafka
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "orders_raw")
    .option("startingOffsets", "latest")
    .load()
)

# 3. Convert Kafka value to JSON string
json_stream = raw_stream.select(
    col("value").cast("string").alias("json_value")
)

# 4. Parse JSON
orders = json_stream.select(
    from_json(col("json_value"), order_schema).alias("order")
).select("order.*")

# Remove duplicate orders based on order_id
orders = orders.dropDuplicates(["order_id"])

valid_orders = orders.filter(
    col("order_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("product_id").isNotNull() &
    col("quantity").isNotNull() &
    (col("quantity") > 0) &
    col("price").isNotNull() &
    (col("price") > 0) &
    col("timestamp").isNotNull()
)


windowed_revenue = (
    valid_orders
    .withWatermark("timestamp", "10 minutes")
    .groupBy(
        window(col("timestamp"), "30 seconds")
    )
    .agg(
        sum(
            round(col("quantity") * col("price"), 2)
        ).alias("revenue")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("revenue")
    )
)

def write_revenue_trend(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    print("====================================")
    print(f"Revenue Trend - Batch {batch_id}")
    print("====================================")

    batch_df.orderBy("window_start").show(truncate=False)

    (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/ecommerce")
        .option("dbtable", "revenue_trend")
        .option("user", "admin")
        .option("password", "admin")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


# 5. Keep all parsed orders
# Data quality validation will happen inside foreachBatch()


# Calculate real-time metrics
def calculate_realtime_metrics(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    total_orders = batch_df.count()

    total_revenue = (
        batch_df
        .withColumn(
            "total_amount",
            round(col("quantity") * col("price"), 2)
        )
        .agg({"total_amount": "sum"})
        .collect()[0][0]
    )

    print("====================================")
    print(f"Batch ID       : {batch_id}")
    print(f"Total Orders   : {total_orders}")
    print(f"Total Revenue  : {total_revenue}")
    print("====================================")


# Calculate top 5 products by revenue
def calculate_top_products(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    top_products = (
        batch_df
        .withColumn(
            "total_amount",
            round(col("quantity") * col("price"), 2)
        )
        .groupBy("product_id", "product_name")
        .agg(
            {"total_amount": "sum"}
        )
        .withColumnRenamed("sum(total_amount)", "revenue")
        .orderBy(col("revenue").desc())
        .limit(5)
    )

    print("====================================")
    print(f"Top Products - Batch {batch_id}")
    print("====================================")

    top_products.show(truncate=False)

# Calculate top 5 customers by revenue
def calculate_top_customers(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    top_customers = (
        batch_df
        .withColumn(
            "total_amount",
            round(col("quantity") * col("price"), 2)
        )
        .groupBy("customer_id")
        .agg(
            {"total_amount": "sum"}
        )
        .withColumnRenamed("sum(total_amount)", "revenue")
        .orderBy(col("revenue").desc())
        .limit(5)
    )

    print("====================================")
    print(f"Top Customers - Batch {batch_id}")
    print("====================================")

    top_customers.show(truncate=False)


#error handling and retry mechanism for writing to Postgres
def write_with_retry(df, table_name, max_retries=3):

    for attempt in range(1, max_retries + 1):

        try:
            (
                df.write
                .format("jdbc")
                .option("url", "jdbc:postgresql://postgres:5432/ecommerce")
                .option("dbtable", table_name)
                .option("user", "admin")
                .option("password", "admin")
                .option("driver", "org.postgresql.Driver")
                .mode("append")
                .save()
            )

            print(f"Successfully wrote data to {table_name}")
            return True

        except Exception as e:

            print(
                f"Attempt {attempt}/{max_retries} failed "
                f"for table {table_name}: {e}"
            )

            if attempt < max_retries:
                time.sleep(5)

    print(f"Failed to write data to {table_name} after {max_retries} attempts.")
    return False


# write to postgres
def write_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    # Valid records
    valid_orders = batch_df.filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("product_id").isNotNull() &
        col("quantity").isNotNull() &
        (col("quantity") > 0) &
        col("price").isNotNull() &
        (col("price") > 0) &
        col("timestamp").isNotNull()
    )

    # Invalid records
    invalid_orders = batch_df.filter(
        ~(
            col("order_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("product_id").isNotNull() &
            col("quantity").isNotNull() &
            (col("quantity") > 0) &
            col("price").isNotNull() &
            (col("price") > 0) &
            col("timestamp").isNotNull()
        )
    )

    # Add total_amount to valid records
    valid_orders = valid_orders.withColumn(
        "total_amount",
        round(col("quantity") * col("price"), 2)
    )

    # Write valid records
    if not valid_orders.isEmpty():
        write_with_retry(
            valid_orders,
            "orders"
        )

    # Add error reason to invalid records
    invalid_orders = invalid_orders.withColumn(
        "error_reason",
        when(
            col("order_id").isNull(),
            "Missing order_id"
        ).when(
            col("customer_id").isNull(),
            "Missing customer_id"
        ).when(
            col("product_id").isNull(),
            "Missing product_id"
        ).when(
            col("quantity").isNull(),
            "Missing quantity"
        ).when(
            col("quantity") <= 0,
            "Invalid quantity"
        ).when(
            col("price").isNull(),
            "Missing price"
        ).when(
            col("price") <= 0,
            "Invalid price"
        ).when(
            col("timestamp").isNull(),
            "Missing timestamp"
        ).otherwise(
            "Unknown validation error"
        )
    )

    # Write invalid records
    if not invalid_orders.isEmpty():

        invalid_orders_to_write = invalid_orders.select(
            "order_id",
            "customer_id",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "price",
            "timestamp",
            "error_reason"
        )

        write_with_retry(
            invalid_orders_to_write,
            "bad_orders"
        )

    
    # Calculate real-time metrics
    calculate_realtime_metrics(batch_df, batch_id)

    # Calculate top products
    calculate_top_products(batch_df, batch_id)

    # Calculate top customers
    calculate_top_customers(batch_df, batch_id)

    print(f"Batch {batch_id} processed.")



# 7. Start streaming query
query = (
    orders.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark-checkpoints/ecommerce")
    .start()
)


# 8. Write revenue trend to Postgres

# revenue_query = (
#     windowed_revenue.writeStream
#     .outputMode("update")
#     .foreachBatch(write_revenue_trend)
#     .option(
#         "checkpointLocation",
#         "/opt/spark-checkpoints/revenue-trend"
#     )
#     .start()
# )


query.awaitTermination()
# revenue_query.awaitTermination()