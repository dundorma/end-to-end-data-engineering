# Use the official Airflow image as a base image
FROM apache/airflow:2.7.3

# Install Apache Spark dependencies
USER root
RUN apt-get update && apt-get install -y openjdk-17-jre
RUN mkdir -p /opt/airflow/raw_data
RUN chown -R 0:0 /opt/airflow

# Switch back to the airflow user
USER airflow
RUN pip install pandas pyspark
