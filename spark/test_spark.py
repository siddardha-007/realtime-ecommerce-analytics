from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceTest")
    .master("local[*]")
    .getOrCreate()
)

data = [
    ("ORD001", "Laptop", 55000),
    ("ORD002", "Headphones", 2500),
    ("ORD003", "Book", 599)
]

df = spark.createDataFrame(
    data,
    ["order_id", "product_name", "price"]
)
df.show()

spark.stop()