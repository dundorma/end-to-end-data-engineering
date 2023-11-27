import pandas as pd
from sqlalchemy import create_engine
import psycopg2 

transformed_traffic_df = pd.read_csv('/opt/airflow/transform_data/traffic_jam_data.csv')
transformed_air_df = pd.read_csv('/opt/airflow/transform_data/air_quality_data.csv')

#Connection String
db_engine = sqlalchemy.create_engine("postgresql+psycopg2://kelompokq@rekdat:qbsPhPKUZqPtKdVj8U8mEhXgmGtfRCjerZchhGgyNPdyFSPk3r982ED3JE3KwkaG@rekdat.postgres.database.azure.com:5432/rekdat")

#Load Transformed Traffic Jam
transformed_traffic_df.to_sql(
	name="traffic_jam_data",
	con=db_engine,
  index=False,
	if_exists="replace"
)

#Load Transformed Air Quality
transformed_air_df.to_sql(
	name="air_quality_data",
	con=db_engine,
  index=False,
	if_exists="replace"
)
