from pyspark.sql import SparkSession


def loadPostgre(df, url, table, properties):
  try:
    df.write.jdbc(url=url, table=table, mode="append", properties=properties)
    print("Data successfully loaded to PostgreSQL")
  except Exception as e:
    print(f"Error during data load {table}: {str(e)}")


def main():
  # Create a SparkSession
  spark = SparkSession.builder.appName("Load Data to PostgreSQL").getOrCreate()

  # JDBC URL for your PostgreSQL database
  jdbc_url = "jdbc:postgresql://http://rekdat.postgres.database.azure.com:5432/rekdat"

  # Properties to set for the PostgreSQL connection
  # Replace 'username' and 'password' with your PostgreSQL credentials
  properties = {
      "user": "kelompokq@rekdat",
      "password": "qbsPhPKUZqPtKdVj8U8mEhXgmGtfRCjerZchhGgyNPdyFSPk3r982ED3JE3KwkaG",
      "driver": "org.postgresql.Driver"
  }

  # Specify the table name in the database to write the DataFrame
  table_name = ['air_quality', 'traffic_jam']
  file_path = ["/opt/airflow/transform_data/air_quality_data.csv", "/opt/airflow/transform_data/traffic_jam_data.csv"]

  for path, table in file_path, table_name:
    df = spark.read.csv(path, header=True, inferSchema=True)
    loadPostgre(df, jdbc_url, table, properties)

  # Stop the SparkSession
  spark.stop()

if __name__ == "__main__":
    main()
