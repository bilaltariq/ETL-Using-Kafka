import clickhouse_connect
import time
from datetime import datetime

def denormalize_data():
    """
    Denormalize data from the orders and order_items tables and insert aggregated results into the daily_sales table.
    
    Args:
        None

    Returns:
        None
    """
    client = clickhouse_connect.get_client(host='localhost', port=8123, database='ecommerce')

    # Create the daily_sales table if it does not exist
    client.command('''
    CREATE TABLE IF NOT EXISTS ecommerce.daily_sales (
        order_create_date Date,
        total_sales Float64,
        total_discount Float64,
        total_paid Float64,
        total_shipping_fee Float64,
        total_quantity UInt32,
        number_of_orders UInt64,
        number_of_items UInt64
    ) ENGINE = AggregatingMergeTree()
    ORDER BY order_create_date;
    ''')

    while True:
        # Aggregate data from orders and order_items
        query = '''SELECT
            toDate(order_create_date) AS order_create_date,
            SUM(order_items.unit_price * order_items.quantity) AS total_sales,
            SUM(order_items.discount_price * order_items.quantity) AS total_discount,
            SUM(order_items.paid_price * order_items.quantity) AS total_paid,
            SUM(order_items.shipping_fee) AS total_shipping_fee,
            SUM(order_items.quantity) AS total_quantity,
            COUNT(Distinct(orders.id)) AS number_of_orders,
            COUNT(order_items.id) AS number_of_items
        FROM ecommerce.order_items AS order_items
        LEFT JOIN ecommerce.orders AS orders ON orders.id = order_items.order_id
        GROUP BY order_create_date
        ORDER BY order_create_date
        '''

        #print(query)
        
        # Fetch aggregated data
        aggregated_data = client.query(query).result_rows

        if aggregated_data:
            # Insert aggregated data into daily_sales table
            insert_query = '''
            INSERT INTO ecommerce.daily_sales (
                order_create_date, total_sales, total_discount, total_paid, total_shipping_fee,
                total_quantity, number_of_orders, number_of_items
            ) VALUES
            '''
            
            # Prepare the values for insertion
            values = ', '.join(
                f"('{row[0]}', {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]})"
                for row in aggregated_data
            )

            # Execute the insert query
            client.command(insert_query + values)

            # Update last processed time
            last_processed_time = max(row[0] for row in aggregated_data)

            print("Data denormalized and inserted into daily_sales table successfully.")

        # Sleep for a specified interval before checking for new data again
        time.sleep(60)  # Check for new data every 60 seconds

if __name__ == "__main__":
    denormalize_data()
