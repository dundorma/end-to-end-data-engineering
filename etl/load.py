import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import psycopg2 

def main():
    transformed_traffic_df = pd.read_csv('/opt/airflow/transform_data/traffic.csv')
    transformed_air_df = pd.read_csv('/opt/airflow/transform_data/air_quality.csv')

    #Connection String
    db_engine = sqlalchemy.create_engine("postgresql+psycopg2://kelompokq@rekdat:qbsPhPKUZqPtKdVj8U8mEhXgmGtfRCjerZchhGgyNPdyFSPk3r982ED3JE3KwkaG@rekdat.postgres.database.azure.com:5432/rekdat")

    #Load Transformed Traffic Jam
    transformed_traffic_df.to_sql(
	    name="traffic_jam",
	    con=db_engine,
      index=False,
	    if_exists="append"
    )

    #Load Transformed Air Quality
    transformed_air_df.to_sql(
	    name="air_quality",
	    con=db_engine,
      index=False,
	    if_exists="append"
    )

if __name__ == "__main__":
    main()
