# End-to-End Data Engineering Pipeline

This repository contains a complete end-to-end data engineering pipeline built with Spark, Cassandra, Kafka, and Apache Airflow. The pipeline demonstrates data ingestion, processing, and storage in a containerized environment to simplify deployment and management.

## Architecture

![Realtime Data Engineering Project Architecture](https://github.com/user-attachments/assets/7e7cabfa-8bf8-4c29-be4c-176c98fe3f36)

### High-Level Components

1. **Data Source**: Used randomuser.me API to generate random user data.
2. **Apache Airflow**: Orchestrates the data pipeline, schedules, and monitors workflows.
2. **Apache Kafka(with Zookeeper)**: Serves as the message broker, handling the data stream between components.
3. **Apache Spark**: Processes the streamed data from Kafka.
4. **Cassandra**: Stores the processed data.
5. **Docker**: Containers for each component, simplifying deployment and management.

### Detailed Architecture

1. **API Data Ingestion**: 
   - Airflow DAG triggers the ingestion of data from an external API. 
   - The data is pushed into Kafka topics.

2. **Data Processing**:
   - Spark Streaming reads data from Kafka.
   - We have control center to monitor our Kafka streams.
   - Data is processed in real-time using Pyspark and then written to Cassandra.

3. **Data Storage**:
   - Cassandra stores the processed data, making it available for querying/further processing.

## Conclusion

This project demonstrates a complete data engineering pipeline, showcasing the integration of Apache Airflow, Apache Kafka, Apache Spark, and Cassandra. The pipeline is fully containerized, making it easy to deploy and manage in any environment. Here, Airflow schedules this pipeline intermittently for data fetching, optimizing resource usage, and ensuring data freshness. Spark processes the data in manageable chunks, and Cassandra stores it efficiently, making the system robust and cost-effective.
