import pandas as pd
import os
import json
import requests
import numpy as np

def traffic_jam_square_querystring(lonlat, n_instance, different):
    # Mengambil nilai longitude dan latitude dari lonlat
    lon = float(lonlat["lon"])
    lat = float(lonlat["lat"])

    querystring = {f"bottom_left":"","top_right":"","max_alerts":{str(n_instance)},"max_jams":{str(n_instance)}}
    # Menyusun titik tengah sebagai bottom_left dan top_right
    querystring["bottom_left"] = f"{lat-different},{lon-different}"
    querystring["top_right"] = f"{lat+different},{lon+different}"

    return querystring

def main():
    api_key = '8081f01e1emsh3e955d0505d9bd1p17ae86jsn4cf8e2b5cf2c'
    api_key_2 = 'f7de1c1538msh9828d5bb47e0fafp1d417djsne7bc6e6aadfd'
    api_key_3 = '13ea0f185fmsh1c0a151cd986134p1eb9b9jsnb182941b30a9'

    lonlat = {"lon":"106.816666", "lat":"-6.200000"} #Jakarta
    updated_querystring = traffic_jam_square_querystring(lonlat, 300, 0.4)

    url = "https://waze.p.rapidapi.com/alerts-and-jams"
    headers = {
	    "X-RapidAPI-Key": api_key,
	    "X-RapidAPI-Host": "waze.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=updated_querystring)

    json_formatted_str = json.dumps(response.json()['data']['jams'], indent=2)
    jams_df = pd.DataFrame(response.json()["data"]["jams"])
    array_jams = response.json()["data"]["jams"]

    for jams in array_jams:
      latitudes = np.mean(np.array([coord['lat'] for coord in jams['line_coordinates']]))
      longitudes = np.mean(np.array([coord['lon'] for coord in jams['line_coordinates']]))
      jams['latitude'] = latitudes
      jams['longitude'] = longitudes

    jams_df = pd.DataFrame(array_jams)

    # EXTRACT SAVE CSV
    # Making sure the directory to save the pdf exists
    output_path = "/opt/airflow/raw_data/"
    os.makedirs(output_path, exist_ok=True)

    jams_df.to_csv(output_path + 'traffic.csv', index=False)

if __name__ == "__main__":
    main()
