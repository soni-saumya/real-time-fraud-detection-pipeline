from faker import Faker
import random
import mysql.connector
from datetime import datetime, timedelta

fake = Faker()

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database="fraud_detection_db"
)

cursor = conn.cursor()

merchants = [
    "Amazon",
    "Flipkart",
    "Walmart",
    "Swiggy",
    "Zomato",
    "Apple Store",
    "Nike",
    "Uber",
    "Netflix",
    "Petrol Pump"
]

transaction_types = [
    "UPI",
    "Card",
    "Net Banking",
    "ATM Withdrawal"
]

countries = [
    "India",
    "USA",
    "Russia",
    "UK",
    "Germany",
    "Singapore"
]

devices = [
    "Mobile",
    "Laptop",
    "ATM",
    "Tablet"
]

transaction_id = 10001

# Generate 10,000 transactions
for _ in range(10000):

    customer_id = random.randint(1, 100)

    # random account id range
    account_id = random.randint(1001, 1200)

    amount = round(random.uniform(100, 100000), 2)

    merchant = random.choice(merchants)

    transaction_type = random.choice(transaction_types)

    country = random.choice(countries)

    device_id = random.choice(devices)

    transaction_time = fake.date_time_between(
        start_date='-30d',
        end_date='now'
    )

    query = """
    INSERT INTO transactions
    (
        transaction_id,
        customer_id,
        account_id,
        amount,
        merchant,
        transaction_type,
        country,
        device_id,
        transaction_time
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        transaction_id,
        customer_id,
        account_id,
        amount,
        merchant,
        transaction_type,
        country,
        device_id,
        transaction_time
    )

    cursor.execute(query, values)

    transaction_id += 1

conn.commit()

print("10,000 transactions inserted successfully!")

cursor.close()
conn.close()