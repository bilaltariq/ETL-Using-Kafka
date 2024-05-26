# src/kafka_setup.py
import subprocess

def setup_kafka():
    """
    Create a docker-compose file for Kafka
    """
    docker_compose_content = """
    version: '2'
    services:
      zookeeper:
        image: wurstmeister/zookeeper:latest
        ports:
         - "2181:2181"
      kafka:
        image: wurstmeister/kafka:latest
        ports:
         - "9092:9092"
        environment:
          KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
          KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
        volumes:
         - /var/run/docker.sock:/var/run/docker.sock
    """
    with open('docker-compose.yml', 'w') as file:
        file.write(docker_compose_content)

    subprocess.run(["docker-compose", "up", "-d"])

if __name__ == "__main__":
    setup_kafka()
