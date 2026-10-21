import os
import sys

project_root = os.path.abspath(os.path.dirname(__file__))
hadoop_directory = os.path.join(project_root, "hadoop")
hadoop_bin_directory = os.path.join(hadoop_directory, "bin")


if not os.path.exists(hadoop_bin_directory):
    os.makedirs(hadoop_bin_directory)


os.environ["HADOOP_HOME"] = hadoop_directory
os.environ["hadoop.home.dir"] = hadoop_directory
os.environ["path"] += os.pathsep + hadoop_bin_directory

import logging
import sys
import types
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructField, StructType, StringType


from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

Cluster.connection_class = AsyncioConnection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """)
        logging.info("Keyspace 'spark_streams' verified/created successfully.")
    except Exception as e:
        logging.error(f"Failed to create keyspace: {e}")

def create_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                id text PRIMARY KEY,
                first_name text,
                last_name text,
                gender text,
                address text,
                post_code text,
                email text,
                username text,
                dob text,
                registered_date text,
                phone text,
                picture text 
            )
        """)
        logging.info("Table 'created_users' verified/created successfully.")
    except Exception as e:
        logging.error(f"Failed to create table: {e}")

def insert_data(session, **kwargs):
    logging.info("Inserting manual data item into table")
    try:
        session.execute("""
            INSERT INTO spark_streams.created_users (id, first_name, last_name, gender, address, post_code, email, username, dob, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                kwargs.get('id'), kwargs.get('first_name'), kwargs.get('last_name'),
                kwargs.get('gender'), kwargs.get('address'), kwargs.get('post_code'),
                kwargs.get('email'), kwargs.get('username'), kwargs.get('dob'),
                kwargs.get('registered_date'), kwargs.get('phone'), kwargs.get('picture')
            ))
        logging.info("Data row cataloged successfully")
    except Exception as e:
        logging.error(f"Error inserting manual row context data: {e}")

def create_spark_conn():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark Session Created successfully with packages attached.")
    except Exception as e:
        logging.error(f"Error creating Spark Session: {e}")
        raise e
    return s_conn

def conn_to_kafka(spark_session):
    spark_df = None
    try:
        spark_df = spark_session.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "user_created") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Kafka Dataframe read stream loaded successfully.")
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
    return spark_df
                
def cassandra_conn():
    try:
        Cluster.connection_class = AsyncioConnection
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        logging.info("Cassandra cluster driver connected.")
        return cas_session
    except Exception as e:
        logging.error(f"Error creating Cassandra Connection due to : {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("post_code", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True),
    ])
    
    sel = spark_df.selectExpr("CAST(value AS STRING) as json_value") \
                  .select(from_json(col("json_value"), schema).alias("data")) \
                  .select("data.*")
    return sel


if __name__ == "__main__":
    
    spark_conn = create_spark_conn()
    
    if spark_conn is not None:
        
        session = cassandra_conn()
        if session is not None:
            create_keyspace(session)
            create_table(session)
            session.shutdown() 
            
        
        spark_df = conn_to_kafka(spark_conn)
        select_df = create_selection_df_from_kafka(spark_df)
        
        logging.info("Initiating structural append write-stream directly to Cassandra...")
        streaming_query = (select_df.writeStream \
                        .format("org.apache.spark.sql.cassandra") \
                        .option('keyspace', 'spark_streams') \
                        .option('table', 'created_users') \
                        .outputMode("append") \
                        .start())

        streaming_query.awaitTermination()
