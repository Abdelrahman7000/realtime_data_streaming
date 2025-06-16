<h2>Introduction</h2>
This project demonstrates a real-time data ingestion pipeline built with Apache Kafka, Apache Spark, and Apache Cassandra, orchestrated using Apache Airflow and containerized with Docker:
<ul>
<li>Ingests data from a public API using Python and publishes it to Kafka and orchestrated using Airflow.</li>
<li>Uses Kafka for messaging and coordination.</li>
<li>Streams data from Kafka into Cassandra using PySpark Structured Streaming.</li>
<li>Manages infrastructure with Docker Compose.</li>
<li>Includes Confluent Schema Registry and Control Center for Kafka management and observability.</li>
</ul>

<h2>Architecture</h2>
<img src='https://github.com/user-attachments/assets/57948f93-7edc-4605-850a-7088130decdb'>

<h3>Technologies</h3>
<ul>
  <li>Kafka + Zookeeper</li>
  <li>Spark 3.5.1 (Bitnami)</li>
  <li>Cassandra</li>
  <li>Airflow (via Astro CLI)</li>
  <li>Docker Compose</li>
</ul>

<h3>Getting Started</h3>
To run this project, ensure that you have the Astro CLI installed on your machine. Astro CLI makes it easy to initialize and run Apache Airflow along with other necessary services.
<ol>
  <li>Clone the repository:</li>
  
      git clone https://github.com/Abdelrahman7000/realtime_data_streaming.git

  
  <li> Navigate to the project directory.</li>
  
  <li>This project uses an external Docker network called streaming-etl_a8a139_airflow, which is shared across all containers.
Ensure you know the network name where Airflow is running. Then, update the network settings for all containers in the docker-compose.override.yml file accordingly.</li>
  
  <li>Initialize the Astro project and start the services:</li>

      astro dev init
      astro dev start

  <li> Run the stream.py file via Airflow UI</li>
  
  <li>To execute the streaming job, open a terminal inside the Spark master container and run:</li>

      spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0   spark_stream.py

</ol>
