from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)
from pyspark.sql.functions import from_json, col, round

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

# 5. Data quality rules
valid_orders = orders.filter(
    col("order_id").isNotNull()
    & col("customer_id").isNotNull()
    & col("product_id").isNotNull()
    & col("quantity").isNotNull()
    & (col("quantity") > 0)
    & col("price").isNotNull()
    & (col("price") > 0)
    & col("timestamp").isNotNull()
)

# 6. Calculate total amount
transformed_orders = valid_orders.withColumn(
    "total_amount",
    round(col("quantity") * col("price"), 2)
)


# write to postgres
def write_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    (
        batch_df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/ecommerce")
        .option("dbtable", "orders")
        .option("user", "admin")
        .option("password", "admin")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    print(f"Batch {batch_id} written to PostgreSQL") 

# 7. Display transformed data
query = (
    transformed_orders.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .start()
)


query.awaitTermination()