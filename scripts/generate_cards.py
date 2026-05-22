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

card_types = ["Debit", "Credit"]

card_id = 5001

# Generate cards for customers
for customer_id in range(1, 101):

    # each customer can have 1 or 2 cards
    num_cards = random.randint(1, 2)

    for _ in range(num_cards):

        card_type = random.choice(card_types)
        card_limit = round(random.uniform(10000, 500000), 2)

        query = """
        INSERT INTO cards
        (card_id, customer_id, card_type, card_limit)
        VALUES (%s, %s, %s, %s)
        """

        values = (
            card_id,
            customer_id,
            card_type,
            card_limit
        )

        cursor.execute(query, values)

        card_id += 1

conn.commit()

print("Cards data inserted successfully!")

cursor.close()
conn.close()