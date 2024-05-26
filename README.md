# ETL-Using-Kafka

### Task Description

We have an e-commerce company that is gathering data for customers, orders and products. Our objective with this project is to create a real-time data ingestion pipeline to process and analyze this data. The project has the following components:

- **MySQL Database**: Contains multiple tables
  - **products**: Information about the products we sell
  - **customers**: Information about our customers
  - **orders**: Information about customer orders
  - **order_items**: Information about the items in each order
- **ClickHouse Data Warehouse**: The destination for storing and analyzing the ingested data.
- **Kafka Server**: To process data in real-time.

The primary programming language for the project is Python.

### Project Structure
The project is organized as follows:

- **Root Folder**: The main directory of the project.

  - **src/**: Contains all the source code for the project.
    - **clickhouse_setup.py**: Create clickhouse docker container and create required tables.
    - **create_clickhouse_tables.sql**: Tables for clickhouse.
    - **denormalize_data.py**: Aggregate table for sales.
    - **dummy_data_generator.py**: Creates dummy data for data ingestion. 
    - **kafka_consumers.py**: Ingesting data from Kafka to Clickhouse.
    - **kafka_producers.py**: Ingesting data from mysql to Kafka Server.
    - **kafka_setup.py**: Creates Kafka docker container.
- **project-setup.bat**: A batch script to install all Docker dependencies and set up the Python environment.
  - **config/**: Contains configuration files.
    - **requirements.txt**: Lists all the necessary Python libraries.
  - **visual/**: Additional Python files for data visualization, using Flask to display the ingested data on a web page. 
    - **dummy_data_generator_realtime.py**: Additional file to produce data in real-time.
    - **app.py**: Web application to view ingested data.

This structure helps in keeping the project organized and maintainable, with separate directories for source code, configuration, and visualization components.

### Running the Pipeline

To run the pipeline, follow these steps:

1. Ensure Docker is installed on your system.
2. Execute `project-setup.bat` located in the root folder. This will create a virtual environment and pull all required Docker images from Docker Hub.
3. Manually run the Python files in the following sequence:
   1. `dummy_data_generator.py`: Starts the MySQL container and generates dummy data.
   2. `kafka_setup.py`: Starts the Kafka and Zookeeper containers.
   3. `kafka_producers.py`: Pulls data from MySQL and pushes it to the Kafka server.
   4. `clickhouse_setup.py`: Starts the ClickHouse Docker container and creates default data tables.
   5. `kafka_consumers.py`: Pulls data from the Kafka server and pushes it to ClickHouse.
   6. `denormalize_data.py`: Creates an aggregate table with important e-commerce KPIs.
   7. `visual/app.py`: Run this to view the ingested data through a web interface.
   
### Screen shots
 1. `dummy_data_generator.py`
    
    ![image](https://github.com/bilaltariq/ETL-Using-Kafka/assets/10683094/b890b040-5b7b-4eed-b366-345f1f6d171d)

 2. `kafka_setup.py`
    
    ![image](https://github.com/bilaltariq/ETL-Using-Kafka/assets/10683094/f9cccf40-1cdc-40ab-b9da-6f6f14541b06)

 3. `clickhouse_setup.py`
    
     ![image](https://github.com/bilaltariq/ETL-Using-Kafka/assets/10683094/fd06265a-0868-405e-b462-f942b01a740f)


     ![image](https://github.com/bilaltariq/ETL-Using-Kafka/assets/10683094/09cd9bee-eda0-4b8f-a8ba-53b68adb7e1f)

  
 5. `visual/app.py`
    
     ![image](https://github.com/bilaltariq/ETL-Using-Kafka/assets/10683094/a234a12d-23b1-41bc-b1ac-dbe55de4ea6b)


