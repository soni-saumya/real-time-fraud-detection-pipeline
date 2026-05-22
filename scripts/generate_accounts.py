from faker import Faker
import random
import mysql.connector

fake = Faker()

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234",
    database="fraud_detection_db"
)

cursor = conn.cursor()

account_types = ["Savings", "Current", "Salary"]
status_list = ["Active", "Blocked", "Inactive"]

account_id = 1001

# Generate accounts for 100 customers
for customer_id in range(1, 101):

    # each customer can have 1–3 accounts
    num_accounts = random.randint(1, 3)

    for _ in range(num_accounts):

        account_type = random.choice(account_types)
        balance = round(random.uniform(5000, 500000), 2)
        status = random.choice(status_list)

        query = """
        INSERT INTO accounts
        (account_id, customer_id, account_type, balance, status)
        VALUES (%s, %s, %s, %s, %s)
        """

        values = (
            account_id,
            customer_id,
            account_type,
            balance,
            status
        )

        cursor.execute(query, values)

        account_id += 1

conn.commit()

print("Accounts data inserted successfully!")

cursor.close()
conn.close()