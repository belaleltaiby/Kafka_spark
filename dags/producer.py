from kafka import KafkaProducer
import csv
import json
import os

# Kafka producer configuration
producer = KafkaProducer(bootstrap_servers='kafka:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Path to the CSV file
csv_file_path = '/opt/airflow/dags/MELBOURNE_HOUSE_PRICES_LESS.csv'

# Check if the file exists
if not os.path.isfile(csv_file_path):
    raise FileNotFoundError(f"The file {csv_file_path} does not exist.")

# Read the CSV file
try:
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Send each row as a message to Kafka topic 'csv_topic'
            producer.send('csv_topic', value=row)

    # Flush the producer to ensure all messages are sent
    producer.flush()
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close()
