from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, lit, lag, lead
from pyspark.sql.window import Window

def replace_outliers_with_mean_adjacent(df, max_values):
    '''handle distinguish value by replace with adjacent mean'''
    windowSpec = Window.orderBy('timestamp_utc')  # Define your timestamp column here

    for col_name in max_values.keys():
        max_val = max_values.get(col_name, float('inf'))
        df = df.withColumn(col_name,
                           when((col(col_name) < 0) | (col(col_name) > max_val),
                                  (lag(col_name, default=0).over(windowSpec)+ lead(col_name, default=0).over(windowSpec)) / 2.0)
                           .otherwise(col(col_name)))

    return df

def consistent_dtype(df, dtype):
    '''make sure the dtype is correct'''
    for col_name in dtype.keys():
        type = dtype.get(col_name)
        df = df.withColumn(col_name, col(col_name).cast(type))
    return df
def saveToCSV(df) :
  try:
    # Save the DataFrame to a CSV file
    df.write.csv('/transform_data', header=True, mode='append')
    print(f"Data successfully saved")
  except IOError as e:
    print(f"Error while saving data: {e}")

def main():
    # Create a SparkSession
  spark = SparkSession.builder.appName("Transform Traffic Jam").getOrCreate()

  # Read CSV file into a Spark DataFrame
  file_path = "/opt/airflow/raw_data/traffic_jam_data.csv"
  df = spark.read.csv(file_path, header=True, inferSchema=True)

  # drop unecessary columns
  columns_to_drop = ['line_coordinates', 
                   'start_location', 
                   'end_location', 
                   'block_alert_id', 
                   'block_alert_type',
                   'block_alert_description',
                   'block_alert_update_datetime_utc',
                   'block_start_datetime_utc',
                   'update_datetime_utc']
  columns_to_drop = ['line_coordinates']
  df = df.drop(*columns_to_drop)

  # rename publish_datetime_utc to timestamp_utc
  df = df.withColumnRenamed("publish_datetime_utc", "timestamp_utc")

  # drom row with all missing value
  df = df.dropna(how ='all')

  # drop duplicates
  df = df.dropDuplicates()

  dtypes = {
    'jam_id': 'integer', 
    'type': 'string', 
    'level': 'integer',
    'severity': 'integer', 
    'speed_kmh': 'double', 
    'length_meters': 'integer',
    'delay_seconds': 'integer',
    'start_location': 'string', 
    'end_location': 'string', 
    'block_alert_id': 'integer', 
    'block_alert_type': 'string',
    'block_alert_description' : 'string',
    'block_alert_update_datetime_utc': 'timestamp',
    'block_start_datetime_utc' :'timestamp',
    'timestamp_utc': 'timestamp',
    'country': 'string',
    'city': 'string',
    'street': 'string'}
  df = consistent_dtype(df, dtypes)

  # consistent datetime format
  df = df.withColumn('timestamp_utc', to_timestamp('timestamp_utc', 'yyyy-MM-dd HH:mm:ss'))
  df = df.withColumn('block_alert_update_datetime_utc', to_timestamp('block_alert_update_datetime_utc', 'yyyy-MM-dd HH:mm:ss'))
  df = df.withColumn('block_start_datetime_utc', to_timestamp('block_start_datetime_utc', 'yyyy-MM-dd HH:mm:ss'))

  # handle Disgusting Values
  max_val = {'co':150000.0, 'no2': 3840.0, 'o3': 1200.0, 'pm10': 600.0, 'pm25': 500.0, 'so2': 800.0}
  # df = replace_outliers_with_mean_adjacent(df, max_val)

  # save df to csv file
  saveToCSV(df)
  spark.stop()

# Still error when saaving csv files or should be transform

# Transformation prcess start
if __name__ == "__main__":
  main()
