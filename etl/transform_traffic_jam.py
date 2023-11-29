import pandas as pd
import os
from datetime import datetime
from dateutil import tz

def main():
    jams_df = pd.read_csv('/opt/airflow/raw_data/traffic.csv')
    column_to_drop = ['line_coordinates']
    jams_df_drop = jams_df.drop(column_to_drop, axis=1)
    jams_df_drop['publish_datetime_utc'] = pd.to_datetime(jams_df_drop['publish_datetime_utc'])#.dt.strftime('%Y-%m-%d %H:%M:%S')
    jams_df_drop['update_datetime_utc'] = pd.to_datetime(jams_df_drop['update_datetime_utc'])#.dt.strftime('%Y-%m-%d %H:%M:%S')

    wib = tz.gettz('Asia/Jakarta')
    jams_df_drop['publish_datetime_local'] = jams_df_drop['publish_datetime_utc'].dt.tz_convert(wib)

    print(jams_df_drop.isna().sum())
    jams_df_drop = jams_df_drop.rename(columns = {'publish_datetime_utc':'jams_publish_datetime_utc', 'update_datetime_utc': 'jams_update_datetime_utc', 'publish_datetime_local': 'jams_publish_datetime_local'})

    # Making sure the directory to save the pdf exists
    output_path = "/opt/airflow/transform_data/"
    os.makedirs(output_path, exist_ok=True)

    jams_df_drop.to_csv(output_path + 'traffic.csv', index=False)

if __name__ == "__main__":
    main()
