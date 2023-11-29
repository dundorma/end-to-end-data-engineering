import pandas as pd
import os
import requests

def main():
    api_key = 'be11409435msha86e2ce00381450p146ce7jsn7e19a682ab57'
    api_key_2 = 'f7de1c1538msh9828d5bb47e0fafp1d417djsne7bc6e6aadfd'
    api_key_3 = '13ea0f185fmsh1c0a151cd986134p1eb9b9jsnb182941b30a9'

    jams_df_drop = pd.read_csv('/opt/airflow/transform_data/traffic.csv')
    df_traffic_groupby_city = jams_df_drop.groupby('city')[['latitude','longitude']].agg({"mean"})
    df_air_quality = pd.DataFrame()

    url = "https://air-quality.p.rapidapi.com/history/airquality"

    headers = {
	    "X-RapidAPI-Key": api_key_3,
	    "X-RapidAPI-Host": "air-quality.p.rapidapi.com"
    }

    # Loop melalui setiap baris pada DataFrame Group city
    for index, row in df_traffic_groupby_city.iloc[:].iterrows():
        lon = str(row['longitude'].values[0])
        lat = str(row['latitude'].values[0])
        city = index
        print(lon, lat, city)
        querystring = {"lon": lon, "lat": lat}

        # Request untuk mendapatkan data air quality dari setiap city
        response = requests.get(url, headers=headers, params=querystring)

        air_df = response.json()['data']

        print(response.json()['data'])

        temp_df = pd.DataFrame(air_df)
        temp_df['latitude'] = lat
        temp_df['longitude'] = lon
        temp_df['city'] = city
        df_air_quality = pd.concat([df_air_quality, temp_df], ignore_index=True)

    # AIR QUALITY EXTRACT SAVE CSV
    # Making sure the directory to save the pdf exists
    output_path = "/opt/airflow/raw_data/"
    os.makedirs(output_path, exist_ok=True)

    df_air_quality.to_csv(output_path + 'air_quality.csv', index=False)

if __name__ == "__main__":
    main()
