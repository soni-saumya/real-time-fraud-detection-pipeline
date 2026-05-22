from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# ---------------------------------------------------
# CONNECT TO KAFKA
# ---------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---------------------------------------------------
# SAMPLE DATA
# ---------------------------------------------------

merchants = [
    "Amazon",
    "Flipkart",
    "Walmart",
    "Apple",
    "Nike",
    "Target"
]

countries = [
    "India",
    "USA",
    "UK",
    "Germany",
    "Canada"
]

transaction_types = [
    "UPI",
    "CARD",
    "NET_BANKING"
]

# ---------------------------------------------------
# GENERATE LIVE TRANSACTIONS
# ---------------------------------------------------

while True:

    transaction = {

        "transaction_id": random.randint(100000, 999999),

        "customer_id": random.randint(1000, 2000),

        "account_id": random.randint(5000, 7000),

        "amount": round(random.uniform(100, 200000), 2),

        "merchant": random.choice(merchants),

        "transaction_type": random.choice(transaction_types),

        "country": random.choice(countries),

        "device_id": f"DEV{random.randint(1000,9999)}",

        "transaction_time": str(datetime.now())
    }

    # SEND TO KAFKA TOPIC
    producer.send(
        "fraud_transactions",
        value=transaction
    )

    print(f"Transaction Sent: {transaction}")

    time.sleep(2)