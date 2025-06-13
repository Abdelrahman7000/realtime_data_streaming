import logging
from cassandra.cluster import Cluster 
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.policies import RoundRobinPolicy

def create_keyspace(session): # create the database
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    logging.info("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
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
    logging.info('Table created successfully') 


# def insert_data(session,**kwargs):#insert data from kafka
#     pass 

def create_spark_connection():
    try:
        conn= SparkSession.builder \
                    .appName("StreamingIntoCassandra") \
                    .config("spark.jars", "/usr/local/airflow/include/spark-cassandra-connector_2.12-3.5.0.jar,/usr/local/airflow/include/spark-sql-kafka-0-10_2.12-3.5.1.jar") \
                    .config("spark.cassandra.connection.host", "cassandra") \
                    .getOrCreate()
        
        conn.sparkContext.setLogLevel('ERROR')
        logging.info(f'Spark connection created successfully')
    
    except Exception as e:
        logging.error(f'Error while initializing spark session {e}')
    
    return conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'user_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Error occurred while creating kafka dataframe: {e}")
    return spark_df

def create_cassandra_connection():
    try:
        #cluster = Cluster(['cassandra'])
        cluster = Cluster(
        contact_points=['cassandra'],
        load_balancing_policy=RoundRobinPolicy(),
        protocol_version=5
)
        session = cluster.connect()

        return session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None
    
def create_selection_df_from_kafka(spark_df):
    schema=StructType([
        StructField('id',StringType(),False),
        StructField('first_name',StringType(),False),
        StructField('last_name',StringType(),False),
        StructField('gender',StringType(),False),
        StructField('address',StringType(),False),
        StructField('post_code',StringType(),False),
        StructField('email',StringType(),False),
        StructField('username',StringType(),False),
        StructField('registered_date',StringType(),False),
        StructField('phone',StringType(),False),
        StructField('picture',StringType(),False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)
    return sel
if __name__=='__main__':
    spark_conn=create_spark_connection()

    if spark_conn:
        session=create_cassandra_connection()
        spark_df=connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        
        if session is not None:
            create_keyspace(session)
            create_table(session)
            
            streaming_query = selection_df.writeStream.format("org.apache.spark.sql.cassandra")\
                               .option('checkpointLocation', '/tmp/checkpoint')\
                               .option('keyspace', 'spark_streams')\
                               .option('table', 'created_users')\
                               .start()
            streaming_query.awaitTermination()
            
