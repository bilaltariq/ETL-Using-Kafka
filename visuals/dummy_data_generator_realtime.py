# src/dummy_data_generator.py

from mysql import connector
from faker import Faker
import random
from datetime import datetime, timedelta
from decimal import Decimal
import subprocess
import time

def create_connection(db='ecommerce'):
    if db is None:
        return connector.connect(
            host='127.0.0.1',
            port='3315',
            user='root',
            password='1234'
        )
    else:
        return connector.connect(
            host='127.0.0.1',
            port='3315',
            user='root',
            password='1234',
            database=db
        )

def generate_dummy_data_for_realtime():
    fake = Faker()
    conn = create_connection()
    cursor = conn.cursor()

    cities = ['Karachi', 'Lahore', 'Islamabad', 'Rawalpindi', 'Faisalabad']
    product_status = ['active', 'deleted', 'inactive']
    payment_methods = ['COD', 'Wallet', 'Card']
    item_statuses = ['pending', 'shipped', 'payment-pending', 'delivered', 'cancelled', 'refunded']

    # # Generate products
    # for _ in range(100):
    #     name = fake.word()
    #     sku = fake.unique.lexify(text='?????-#####')
    #     price = round(random.uniform(20, 5000), 2)
    #     created_at = fake.date_time_this_year()
    #     modified_at = created_at if random.random() > 0.5 else created_at + timedelta(days=random.randint(1, 10))
    #     status = random.choice(product_status)
    #     cursor.execute("INSERT INTO products (name, description, sku, price, created_at, modified_at, status, fetch_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
    #                    (name, fake.text(), sku, price, created_at, modified_at, status, None))
    # print('Products Inserted.')

    # # Generate customers
    # for _ in range(50):
    #     name = fake.name()
    #     age = random.randint(18, 70)
    #     address = fake.address()
    #     city = random.choice(cities)
    #     created_at = fake.date_time_this_year()
    #     modified_at = created_at if random.random() > 0.5 else created_at + timedelta(days=random.randint(1, 10))
    #     phone_number = fake.phone_number()
    #     cursor.execute("INSERT INTO customers (name, age, address, city, created_at, modified_at, phone_number, fetch_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
    #                    (name, age, address, city, created_at, modified_at, phone_number, None))
    # print('Customers Inserted.')
    
    # Get customer and product IDs
    cursor.execute("SELECT id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT sku, price FROM products WHERE status='active'")
    products = cursor.fetchall()

    time.sleep(5)
    
    # Generate orders and order_items
    while True:
    #for _ in range(200):
        customer_id = random.choice(customer_ids)
        order_create_date = datetime.today()
        billing_address = fake.address()
        shipping_address = fake.address()
        city = random.choice(cities)
        payment_method = random.choice(payment_methods)
        cursor.execute("INSERT INTO orders (order_create_date, billing_address, shipping_address, city, customer_id, payment_method, fetch_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (order_create_date, billing_address, shipping_address, city, customer_id, payment_method, None))
        order_id = cursor.lastrowid

        num_items = random.randint(1, 3)
        for _ in range(num_items):
            product_sku, unit_price = random.choice(products)
            discount_price = round(unit_price * Decimal(-1).from_float(random.uniform(0.1, 0.3)), 2)
            paid_price = unit_price - discount_price
            shipping_fee = round(random.uniform(10, 50), 2)
            item_status = random.choice(item_statuses)
            item_create_date = order_create_date
            item_modified_date = item_create_date #if random.random() > 0.5 else item_create_date + timedelta(days=random.randint(1, 10))
            delivery_date = item_modified_date + timedelta(days=random.randint(1, 10)) if item_status == 'delivered' else '1970-01-01 00:00:00'
            quantity = random.randint(5, 10)
            cursor.execute("""
                INSERT INTO order_items (order_id, unit_price, discount_price, paid_price, shipping_fee, item_status, item_create_date, item_modified_date, delivery_date, quantity, product_sku_code, fetch_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (order_id, unit_price, discount_price, paid_price, shipping_fee, item_status, item_create_date, item_modified_date, delivery_date, quantity, product_sku, None))

        print('Orders and Items table populated.')
        conn.commit()
        #cursor.close()
        #conn.close()
        time.sleep(2)

if __name__ == "__main__":
    generate_dummy_data_for_realtime()
