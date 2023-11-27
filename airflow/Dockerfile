FROM apache/airflow:2.7.3

USER root
RUN apt-get update -y &&\
    apt-get -y install build-essential binutils &&\
    apt-get -y install gcc-multilib &&\
    apt-get -y install openjdk-17-jre &&\
    apt-get -y install libpq-dev

RUN mkdir -p /opt/airflow/raw_data
RUN chown -R 0:0 /opt/airflow

USER airflow
RUN pip install pandas pyspark SQLAlchemy psycopg2
