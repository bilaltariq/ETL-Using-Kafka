# src/clickhouse_setup.py
import subprocess
import time

def wait_for_clickhouse():
    """
    Method waits for clickhouse docker container to start.
    """
    while True:
        try:
            result = subprocess.run([
                "docker-compose",
                "-f", "docker-compose-clickhouse.yml",
                "exec",
                "-T",
                "clickhouse-server",
                "clickhouse-client",
                "--query", "SELECT 1"
            ], capture_output=True, text=True)
            if result.returncode == 0:
                print("ClickHouse server is ready.")
                break
        except subprocess.CalledProcessError:
            pass
        print("Waiting for ClickHouse server to start...")
        time.sleep(5)


def setup_clickhouse():
    """
    Create a docker-compose file for ClickHouse
    """
    
    docker_compose_content = """
    version: '3'
    services:
      clickhouse-server:
        image: bitnami/clickhouse:latest
        ports:
         - "8123:8123"
         - "9001:9001"
        environment:
         - ALLOW_EMPTY_PASSWORD=yes
    """
    with open('docker-compose-clickhouse.yml', 'w') as file:
        file.write(docker_compose_content)

    subprocess.run(["docker-compose", "-f", "docker-compose-clickhouse.yml", "up", "-d"])

def initialize_clickhouse():
    setup_clickhouse()
    wait_for_clickhouse()

    # if you are running this file from root folder then edit the path
    # with open('src/create_clickhouse_tables.sql', 'r') as file:
    
    with open('create_clickhouse_tables.sql', 'r') as file:
        sql_commands = file.read().strip().split(';')
        
        for command in sql_commands:
            #print(command)
            if command.strip():
                subprocess.run([
                    "docker-compose",
                    "-f", "docker-compose-clickhouse.yml",
                    "exec",
                    "-T",
                    "clickhouse-server",
                    "clickhouse-client",
                    "--query", command.strip()
                ])


if __name__ == "__main__":
    initialize_clickhouse()
