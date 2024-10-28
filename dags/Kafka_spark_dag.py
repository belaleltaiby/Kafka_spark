from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='kafka_spark_etl',
    default_args=default_args,
    schedule_interval=None,
)

# Task 1: Create Kafka Topic
create_topic = BashOperator(
    task_id='create_kafka_topic',
    bash_command='docker exec my_kafka_spark-kafka-1 kafka-topics --create --topic csv_topic --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1',
    dag=dag,
)

# Task 2: Kafka Producer (to send CSV data to Kafka topic)
kafka_producer = BashOperator(
    task_id='run_kafka_producer',
    bash_command='python3 /opt/airflow/dags/producer.py',  # Updated to Docker path
    dag=dag,
)

# Task 3: Spark Consumer (to consume and process data from Kafka)
spark_consumer = BashOperator(
    task_id='run_spark_consumer',
    bash_command='/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/airflow/dags/spark_consumer.py',
    #bash_command= 'echo "Hello World"',
    dag=dag,    
)

# Define task dependencies
kafka_producer >> spark_consumer
