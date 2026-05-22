import random
import mysql.connector

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database="fraud_detection_db"
)

cursor = conn.cursor()

reasons = [
    "Stolen Card",
    "Suspicious Activity",
    "AML Violation",
    "Fake KYC",
    "Multiple Fraud Attempts"
]

# randomly blacklist 15 customers
blacklisted_customers = random.sample(range(1, 101), 15)

for customer_id in blacklisted_customers:

    reason = random.choice(reasons)

    query = """
    INSERT INTO blacklisted_users
    (customer_id, reason)
    VALUES (%s, %s)
    """

    values = (
        customer_id,
        reason
    )

    cursor.execute(query, values)

conn.commit()

print("Blacklisted users inserted successfully!")

cursor.close()
conn.close()