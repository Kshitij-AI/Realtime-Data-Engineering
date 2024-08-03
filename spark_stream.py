import logging
from datetime import datetime
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Spark session
def create_spark_connection():
    spark_session=None
    # Creating connections - spark with Kafka(source) and cassandra(sink)
    try:
        spark_session = SparkSession.builder \
                            .appName("DataStreaming") \
                            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
                            .config("spark.cassandra.connection.host", "localhost") \
                            .getOrCreate()
        # log messages with severity "ERROR" and higher
        spark_session.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session created successfully!')
    except Exception as e:
        logging.error(f'Spark session creation failed: {e}')
    return spark_session

# Reading data from Kafka
def connect_to_kafka(spark_session):
    spark_df=None
    try:
        spark_df=spark_session.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "broker:9092") \
                    .option("subscribe", "user_info") \
                    .option("startingOffsets", "earliest") \
                    .load()
        logging.info('DataFrame from Kafka created successfully!')
    except Exception as e:
        logging.error(f'Dataframe from Kafka creation failed: {e}')
    return spark_df

# Create keyspace cassandra
def create_keyspace(session):
    print("creating keyspace...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_stream
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("keyspace created!")

# Create table cassandra
def create_table(session):
    print("creating table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)
    print("table created!")

# Cassandra session
def create_cassandra_connection():
    cass_session=None
    try:
        cluster = Cluster(['localhost'])
        cass_session = cluster.connect()
        logging.info('Cassandra session created successfully!')
    except Exception as e:
        logging.error(f'Cassandra connection failed: {e}')
    return cass_session

# Formatting dataframe from kafka to insert in Cassandra DB
def format_kafka_data(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # Casting the value column(in binary) to a string, then extracting all fields from the JSON data into individual columns.
    query = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return query

if __name__=='__main__':
     # create spark connection
    spark_session = create_spark_connection()

    if spark_session is not None:
        # connect to kafka using spark
        spark_df = connect_to_kafka(spark_session)

        # create cassandra session
        cass_session = create_cassandra_connection()
        # formatting dataframe from kafka to insert in Cassandra DB
        selection_df = format_kafka_data(spark_df)

        if cass_session is not None:
            # Create keyspace if not exists
            create_keyspace(cass_session)
            # Create table if not exists
            create_table(cass_session)

            # Streaming data into cassandra
            logging.info("Streaming started...")
            streaming_query = (selection_df.writeStream \
                               .format("org.apache.spark.sql.cassandra") \
                               .option('keyspace', 'spark_stream') \
                               .option('table', 'users') \
                               .start())

            streaming_query.awaitTermination()