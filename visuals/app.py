from flask import Flask, render_template
import clickhouse_connect

app = Flask(__name__)

def get_data_from_clickhouse():
    client = clickhouse_connect.get_client(host='localhost', port=8123, database='ecommerce')
    query = 'SELECT distinct * FROM daily_sales order by order_create_date desc'
    result = client.query(query)
    return result.result_rows, result.column_names

@app.route('/')
def index():
    data, column_names = get_data_from_clickhouse()
    return render_template('table.html', column_names=column_names, data=data)

if __name__ == '__main__':
    app.run(debug=True)
