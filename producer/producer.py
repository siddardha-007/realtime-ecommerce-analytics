from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timezone

#connect to kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda value: json.dumps(value).encode('utf-8')
)

products = [
    {
        "product_id": "P001",
        "product_name": "Laptop",
        "category": "Electronics",
        "price": 55000
    },
    {
        "product_id": "P002",
        "product_name": "Headphones",
        "category": "Electronics",
        "price": 2500
    },
    {
        "product_id": "P003",
        "product_name": "T-Shirt",
        "category": "Clothing",
        "price": 999
    },
    {
        "product_id": "P004",
        "product_name": "Running Shoes",
        "category": "Footwear",
        "price": 2999
    },
    {
        "product_id": "P005",
        "product_name": "Book",
        "category": "Books",
        "price": 599
    }
]

def generate_order(order_number):
    product = random.choice(products)

    order = {
        "order_id": f"ORD{order_number:04d}",
        "customer_id": f"C{random.randint(1, 20):03d}",
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "quantity":random.randint(1, 5),
        "price": product["price"],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    return order

print("Starting Kafka producer...")

order_number = 1

try:

    while True:

        order = generate_order(order_number)

        producer.send(
            "orders_raw",
            value=order
        )

        producer.flush()

        print(f"Sent: {order}")

        order_number += 1

        time.sleep(2)

except KeyboardInterrupt:

    print("\nProducer stopped.")

finally:

    producer.close()