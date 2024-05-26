# src/dummy_data_generator.py

from mysql import connector  # Import MySQL connector
from faker import Faker  # Import Faker library for generating fake data
import random  # Import random module for generating random data
from datetime import datetime, timedelta  # Import datetime module for handling dates and times
from decimal import Decimal  # Import Decimal for handling decimal numbers
import subprocess  # Import subprocess module for running shell commands
import time  # Import time module for adding delays


def setup_mysql():
    # Create a docker-compose file for MySQL
    docker_compose_content = """
    version: '3.8'
    services:
      mysql-container:
        image: mysql:latest
        environment:
          MYSQL_ROOT_PASSWORD: '1234'
        ports:
          - "3315:3306"
    """
    with open('docker-compose-mysql.yml', 'w') as file:
        file.write(docker_compose_content)

    subprocess.run(["docker-compose", "-f", "docker-compose-mysql.yml", "up", "-d"])

def create_connection(db='ecommerce'):
    """
        Create a connection to the MySQL database.
        
        Args:
            db (str): Name of the database to connect to.
        
        Returns:
            MySQLConnection: Connection object.
    """

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

def delete_all_data():
    """
    Delete all data from tables in the database.
    """
    conn = create_connection()
    cursor = conn.cursor()
    tables = ['order_items', 'orders', 'customers', 'products']

    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        cursor.execute(f"TRUNCATE TABLE {table}")
        print(f"Deleted {count} rows from {table}")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    conn.commit()
    cursor.close()

def create_database(db_name='ecommerce'):
    """
    Create a database if it does not exist.
    
    Args:
        db_name (str): Name of the database to create.
    """

    conn = create_connection(db=None)
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS " + db_name)
    cursor.close()
    conn.close()

def create_tables():
    """
    Create tables in the database if they do not exist.
    """
    conn = create_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description TEXT,
        sku VARCHAR(255) NOT NULL UNIQUE,
        price DECIMAL(10, 2) NOT NULL,
        created_at DATETIME,
        modified_at DATETIME,
        status ENUM('active', 'deleted', 'inactive') NOT NULL,
        fetch_timestamp DATETIME
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        age INT,
        address TEXT,
        city VARCHAR(255),
        created_at DATETIME,
        modified_at DATETIME,
        phone_number VARCHAR(255),
        fetch_timestamp DATETIME
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_create_date DATETIME,
        billing_address TEXT,
        shipping_address TEXT,
        city VARCHAR(255),
        customer_id INT,
        payment_method ENUM('COD', 'Wallet', 'Card'),
        fetch_timestamp DATETIME,
        FOREIGN KEY (customer_id) REFERENCES customers(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS order_items (
        id INT AUTO_INCREMENT PRIMARY KEY,
        order_id INT,
        unit_price DECIMAL(10, 2),
        discount_price DECIMAL(10, 2),
        paid_price DECIMAL(10, 2),
        shipping_fee DECIMAL(10, 2),
        item_status ENUM('pending', 'shipped', 'payment-pending', 'delivered', 'cancelled', 'refunded'),
        item_create_date DATETIME,
        item_modified_date DATETIME,
        delivery_date DATETIME,
        quantity INT,
        product_sku_code VARCHAR(255),
        fetch_timestamp DATETIME,
        FOREIGN KEY (order_id) REFERENCES orders(id),
        FOREIGN KEY (product_sku_code) REFERENCES products(sku)
    )
    """)

    conn.commit()
    cursor.close()
    conn.close()

def generate_dummy_data():
    """
    Generate dummy data using Faker library and insert it into the tables.
    """
    fake = Faker()
    conn = create_connection()
    cursor = conn.cursor()

    cities = ['Karachi', 'Lahore', 'Islamabad', 'Rawalpindi', 'Faisalabad']
    product_status = ['active', 'deleted', 'inactive']
    payment_methods = ['COD', 'Wallet', 'Card']
    item_statuses = ['pending', 'shipped', 'payment-pending', 'delivered', 'cancelled', 'refunded']

    # Generate products
    for _ in range(100):
        name = fake.word()
        sku = fake.unique.lexify(text='?????-#####')
        price = round(random.uniform(20, 5000), 2)
        created_at = fake.date_time_this_year()
        modified_at = created_at if random.random() > 0.5 else created_at + timedelta(days=random.randint(1, 10))
        status = random.choice(product_status)
        cursor.execute("INSERT INTO products (name, description, sku, price, created_at, modified_at, status, fetch_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                       (name, fake.text(), sku, price, created_at, modified_at, status, None))
    print('Products Inserted.')

    # Generate customers
    for _ in range(50):
        name = fake.name()
        age = random.randint(18, 70)
        address = fake.address()
        city = random.choice(cities)
        created_at = fake.date_time_this_year()
        modified_at = created_at if random.random() > 0.5 else created_at + timedelta(days=random.randint(1, 10))
        phone_number = fake.phone_number()
        cursor.execute("INSERT INTO customers (name, age, address, city, created_at, modified_at, phone_number, fetch_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                       (name, age, address, city, created_at, modified_at, phone_number, None))
    print('Customers Inserted.')
    
    # Get customer and product IDs
    cursor.execute("SELECT id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT sku, price FROM products WHERE status='active'")
    products = cursor.fetchall()
    
    # Generate orders and order_items
    for _ in range(200):
        customer_id = random.choice(customer_ids)
        order_create_date = fake.date_time_this_year()
        billing_address = fake.address()
        shipping_address = fake.address()
        city = random.choice(cities)
        payment_method = random.choice(payment_methods)
        cursor.execute("INSERT INTO orders (order_create_date, billing_address, shipping_address, city, customer_id, payment_method, fetch_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                       (order_create_date, billing_address, shipping_address, city, customer_id, payment_method, None))
        order_id = cursor.lastrowid

        num_items = random.randint(1, 6)
        for _ in range(num_items):
            product_sku, unit_price = random.choice(products)
            discount_price = round(unit_price * Decimal(-1).from_float(random.uniform(0.1, 0.3)), 2)
            paid_price = unit_price - discount_price
            shipping_fee = round(random.uniform(10, 50), 2)
            item_status = random.choice(item_statuses)
            item_create_date = order_create_date
            item_modified_date = item_create_date if random.random() > 0.5 else item_create_date + timedelta(days=random.randint(1, 10))
            delivery_date = item_modified_date + timedelta(days=random.randint(1, 10)) if item_status == 'delivered' else '1970-01-01 00:00:00'
            quantity = random.randint(1, 5)
            cursor.execute("""
                INSERT INTO order_items (order_id, unit_price, discount_price, paid_price, shipping_fee, item_status, item_create_date, item_modified_date, delivery_date, quantity, product_sku_code, fetch_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (order_id, unit_price, discount_price, paid_price, shipping_fee, item_status, item_create_date, item_modified_date, delivery_date, quantity, product_sku, None))

    print('Orders and Items table populated.')
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    setup_mysql()
    time.sleep(30)  # Wait for MySQL container to initialize
    create_database()
    create_tables()
    delete_all_data()
    generate_dummy_data()
