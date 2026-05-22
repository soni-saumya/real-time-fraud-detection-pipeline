from faker import Faker
import random
import mysql.connector

fake = Faker()

# MySQL Connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database="fraud_detection_db"
)

cursor = conn.cursor()

# Generate 100 customers
for customer_id in range(1, 101):

    customer_name = fake.name()
    email = fake.email()
    phone = fake.phone_number()
    city = fake.city()
    country = fake.country()

    query = """
    INSERT INTO customers
    (customer_id, customer_name, email, phone, city, country)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    values = (
        customer_id,
        customer_name,
        email,
        phone,
        city,
        country
    )

    cursor.execute(query, values)

conn.commit()

print("Customers data inserted successfully!")

cursor.close()
conn.close()