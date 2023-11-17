import requests
import os
import pandas as pd

def saveToCSV(data) :
  data_to_save = data['data']['jams']
  df = pd.DataFrame(data_to_save)

  current_directory = os.path.dirname(os.path.abspath(__file__))
  raw_data_folder = os.path.join(current_directory, '../raw_data')
  csv_file_path = os.path.join(raw_data_folder, 'traffic_jam_data.csv')

  mode = 'a' if os.path.exists(csv_file_path) else 'w'

  try:
    # Save the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False, mode= mode)
    print(f"Data successfully saved to {csv_file_path}")
  except IOError as e:
    print(f"Error while saving data: {e}")

def fetchData(api):
  try:
      response = requests.get(api['url'], headers = api["headers"], params = api['params'])
      if response.status_code == 200:
          data = response.json()
          print("Data retrieved successfully:")
          return data
      else:
          print(f"Request failed with status code {response.status_code}")
#   
  except requests.RequestException as e:
      print(f"Request encountered an error: {e}")

if __name__ == "__main__":
  trafficJams = {
    'url': 'https://waze.p.rapidapi.com/alerts-and-jams',
    'params' : {"bottom_left":"-6.37477,106.6651","top_right":"-6.05441,106.9804","max_alerts":"20","max_jams":"20"}, 
    'headers' : {
    	"X-RapidAPI-Key": "37757523d9mshedf13855fb8cf03p11c892jsnaafe180e14ab",
    	"X-RapidAPI-Host": "waze.p.rapidapi.com"
    }
  }
  data = fetchData(trafficJams)
  saveToCSV(data)
