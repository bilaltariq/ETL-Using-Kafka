from kafka import KafkaProducer
import mysql.connector
import json
import time
from decimal import Decimal
from datetime import datetime
import threading

class DecimalDateTimeEncoder(json.JSONEncoder):
    """
    JSON Encoder subclass to handle Decimal and datetime objects.
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(DecimalDateTimeEncoder, self).default(obj)

def create_producer():
    """
    Create a Kafka producer.

    Returns:
        KafkaProducer: Kafka producer instance.
    """
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v, cls=DecimalDateTimeEncoder).encode('utf-8')
    )
    return producer

def fetch_data_from_mysql(table_name):
    """
    Fetch data from MySQL table.

    Args:
        table_name (str): Name of the table.

    Returns:
        list: List of rows as dictionaries.
    """
    config = {
        'host':'127.0.0.1',
        'port':'3315',
        'user':'root',
        'password':'1234',
        'database':'ecommerce'
    }
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {table_name}" + " where fetch_timestamp is null order by 1")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def update_fetch_timestamp(table_name, row_id):
    """
    Update fetch_timestamp for a specific row in the MySQL table.

    Args:
        table_name (str): Name of the table.
        row_id (int): ID of the row to update.
    """
    config = {
        'host':'127.0.0.1',
        'port':'3315',
        'user':'root',
        'password':'1234',
        'database':'ecommerce'
    }
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    fetch_timestamp = datetime.now()
    cursor.execute(f"UPDATE {table_name} SET fetch_timestamp = %s WHERE id = %s", (fetch_timestamp, row_id))
    conn.commit()
    cursor.close()
    conn.close()

def send_products_to_kafka(producer, table_name='products', topic_name='products'):
    """
    Send products data from MySQL to Kafka.

    Args:
        producer (KafkaProducer): Kafka producer instance.
        table_name (str): Name of the MySQL table.
        topic_name (str): Name of the Kafka topic.
    """
    print(f"Thread for {table_name} started.")
    while True:
        data = fetch_data_from_mysql(table_name)
        for row in data:
            producer.send(topic_name, row)
            update_fetch_timestamp(table_name, row['id'])
            time.sleep(1)  # simulate real-time data flow
        time.sleep(10)

def send_customers_to_kafka(producer, table_name='customers', topic_name='customers'):
    """
    Send customers data from MySQL to Kafka.

    Args:
        producer (KafkaProducer): Kafka producer instance.
        table_name (str): Name of the MySQL table.
        topic_name (str): Name of the Kafka topic.
    """
    print(f"Thread for {table_name} started.")
    while True:
        data = fetch_data_from_mysql(table_name)
        for row in data:
            producer.send(topic_name, row)
            update_fetch_timestamp(table_name, row['id'])
            time.sleep(1)  # simulate real-time data flow
        time.sleep(10)

def send_orders_to_kafka(producer, table_name='orders', topic_name='orders'):
    """
    Send orders data from MySQL to Kafka.

    Args:
        producer (KafkaProducer): Kafka producer instance.
        table_name (str): Name of the MySQL table.
        topic_name (str): Name of the Kafka topic.
    """
    print(f"Thread for {table_name} started.")
    while True:
        data = fetch_data_from_mysql(table_name)
        for row in data:
            producer.send(topic_name, row)
            update_fetch_timestamp(table_name, row['id'])
            time.sleep(1)  # simulate real-time data flow
        time.sleep(10)

def send_order_items_to_kafka(producer, table_name='order_items', topic_name='order_items'):
    """
    Send order items data from MySQL to Kafka.

    Args:
        producer (KafkaProducer): Kafka producer instance.
        table_name (str): Name of the MySQL table.
        topic_name (str): Name of the Kafka topic.
    """
    print(f"Thread for {table_name} started.")
    while True:
        data = fetch_data_from_mysql(table_name)
        for row in data:
            producer.send(topic_name, row)
            update_fetch_timestamp(table_name, row['id'])
            time.sleep(1)  # simulate real-time data flow
        time.sleep(10)

if __name__ == "__main__":
    # Create a Kafka producer
    producer = create_producer()
    
    # Start separate threads for each data type
    product_thread = threading.Thread(target=send_products_to_kafka, args=(producer, 'products', 'products'))
    customers_thread = threading.Thread(target=send_customers_to_kafka, args=(producer, 'customers', 'customers'))
    orders_thread = threading.Thread(target=send_orders_to_kafka, args=(producer, 'orders', 'orders'))
    order_items_thread = threading.Thread(target=send_order_items_to_kafka, args=(producer, 'order_items', 'order_items'))

    # Start threads
    product_thread.start()
    customers_thread.start()
    orders_thread.start()
    order_items_thread.start()

    # Join threads to wait for them to finish
    product_thread.join()
    customers_thread.join()
    orders_thread.join()
    order_items_thread.join()
    
    # Ensure all messages are sent before exiting
    producer.flush()