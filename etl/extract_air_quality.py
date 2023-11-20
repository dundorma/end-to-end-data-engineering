import requests
import os
import pandas as pd

def saveToCSV(data) :
  data_to_save = data.get('data',[])
  df = pd.DataFrame(data_to_save)

  current_directory = os.path.dirname(os.path.abspath(__file__))
  raw_data_folder = os.path.join(current_directory, '../raw_data')
  csv_file_path = os.path.join(raw_data_folder, 'air_quality_data.csv')

  try:
    # Save the DataFrame to a CSV file
    df.to_csv(csv_file_path, index=False, mode= 'w', header = True)
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
  airPolution = {
  'url': 'https://air-quality.p.rapidapi.com/history/airquality',
  'params' : {"lon":"106.816666","lat":"-6.200000"}, 
  'headers' : {
	  "X-RapidAPI-Key": "37757523d9mshedf13855fb8cf03p11c892jsnaafe180e14ab",
	  "X-RapidAPI-Host": "air-quality.p.rapidapi.com"
    }
  }
  data = fetchData(airPolution)
  saveToCSV(data)
