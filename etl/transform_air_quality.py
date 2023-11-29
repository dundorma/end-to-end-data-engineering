import pandas as pd
import os

def main():
    df_air_quality = pd.read_csv('/opt/airflow/raw_data/air_quality.csv')
    df_air_quality['timestamp_local'] = pd.to_datetime(df_air_quality['timestamp_local'])
    df_air_quality['timestamp_utc'] = pd.to_datetime(df_air_quality['timestamp_utc'])

    df_air_quality = df_air_quality.rename(columns = {'timestamp_local':'air_timestamp_local', 'timestamp_utc': 'air_timestamp_utc'})

    # Making sure the directory to save the pdf exists
    output_path = "/opt/airflow/transform_data/"
    os.makedirs(output_path, exist_ok=True)

    df_air_quality.to_csv(output_path + 'air_quality.csv', index=False)

if __name__ == "__main__":
    main()
