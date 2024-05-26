from kafka import KafkaConsumer
import json
import threading
import clickhouse_connect
import time

def create_consumer(topic_name):
    """
    Create a Kafka consumer for a specific topic.

    Args:
        topic_name (str): Name of the Kafka topic.

    Returns:
        KafkaConsumer: Kafka consumer instance.
    """
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def insert_into_clickhouse(client, table, data):
    """
    Insert data into a ClickHouse table.

    Args:
        client (clickhouse_connect.Client): ClickHouse client instance.
        table (str): Name of the ClickHouse table.
        data (dict): Data to insert.
    """
    columns = ", ".join(data.keys())
    values = ", ".join([f"'{str(v)}'" if isinstance(v, str) else str(v) for v in data.values()])
    query = f"INSERT INTO ecommerce.{table} ({columns}) VALUES ({values})"
    if table == 'order_items':
        print(columns, values, query)
    client.command(query)
    #print(data)

def consume_messages(consumer, table):
    """
    Consume messages from Kafka and insert them into ClickHouse.

    Args:
        consumer (KafkaConsumer): Kafka consumer instance.
        table (str): Name of the ClickHouse table to insert data into.
    """
    client = clickhouse_connect.get_client(host='localhost', port=8123, database='ecommerce')
    print(f"Thread for {table} started.")
    while True:
        for message in consumer:
            data = message.value
            data.pop('fetch_timestamp')
            #print(data)
            # Insert data into ClickHouse
            insert_into_clickhouse(client, table, data)
            time.sleep(1)
            #print(f"Inserted data into {table}: {data}")

if __name__ == "__main__":
    # Create and start threads for each consumer
    product_consume_thread = threading.Thread(target=consume_messages, args=(create_consumer('products'), 'products'))
    customers_consume_thread = threading.Thread(target=consume_messages, args=(create_consumer('customers'), 'customers'))
    orders_consume_thread = threading.Thread(target=consume_messages, args=(create_consumer('orders'), 'orders'))
    order_items_consume_thread = threading.Thread(target=consume_messages, args=(create_consumer('order_items'), 'order_items'))

    # Start threads
    product_consume_thread.start()
    customers_consume_thread.start()
    orders_consume_thread.start()
    order_items_consume_thread.start()

    # Join threads to wait for them to finish
    product_consume_thread.join()
    customers_consume_thread.join()
    orders_consume_thread.join()
    order_items_consume_thread.join()
