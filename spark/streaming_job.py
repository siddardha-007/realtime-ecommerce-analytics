from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)
from pyspark.sql.functions import from_json, col, round, when

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

# 5. Keep all parsed orders
# Data quality validation will happen inside foreachBatch()


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
        (
            valid_orders.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/ecommerce")
            .option("dbtable", "orders")
            .option("user", "admin")
            .option("password", "admin")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
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
        (
            invalid_orders
            .select(
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
            .write
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/ecommerce")
            .option("dbtable", "bad_orders")
            .option("user", "admin")
            .option("password", "admin")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )

    print(f"Batch {batch_id} processed.")


# 7. Start streaming query
query = (
    orders.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark-checkpoints/ecommerce")
    .start()
)


query.awaitTermination()