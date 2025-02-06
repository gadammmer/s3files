import csv
import requests
import datetime
import argparse
import os
import json
import shutil
import time


def download_forecast(archivo_urls, dias_forecast, token, workdir):
    with open(archivo_urls, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if reader.line_num == 1:
                continue
            CITYCODE, COUNTRYCODE, longitud, latitud = row
            fichero_salida_json = f"{CITYCODE}-{COUNTRYCODE}-fcst.json"
            fichero_salida_csv = f"{CITYCODE}-{COUNTRYCODE}-fcst.csv"
            url = (f"https://api.open-meteo.com/v1/forecast?apikey={token}"
               f"&latitude={latitud}&longitude={longitud}&forecast_days={dias_forecast}"
               f"&daily=weather_code,temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
               f"apparent_temperature_max,apparent_temperature_min,apparent_temperature_mean,"
               f"sunrise,sunset,daylight_duration,sunshine_duration,precipitation_sum,rain_sum,"
               f"snowfall_sum,precipitation_hours,wind_speed_10m_max,wind_gusts_10m_max,"
               f"wind_direction_10m_dominant&time_mode=time_interval&timezone=auto")
            response = requests.get(url)

            #with open(f'{workdir}/{fichero_salida_json}', 'w') as outfile:
            #    outfile.write(response.text)

            if response.status_code != 200:
                print(f"[{time.strftime('%H:%M:%S')}, call nº {reader.line_num}] Error: {response.text}")
                raise Exception(f"Error: {response.text}")


            data = json.loads(response.text)
            daily_data = data.get('daily', {})
            dates = daily_data.get('time', [])

            headers = ['date'] + list(daily_data.keys())[1:]
            rows = []

            for i in range(len(dates)):
                row = [dates[i]]
                for key in headers[1:]:
                    row.append(daily_data[key][i])
                rows.append(row)

            with open(f'{workdir}/{fichero_salida_csv}', 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(headers)
                writer.writerows(rows)


def transform_forecast(workdir):
    for filename in os.listdir(workdir):
        if filename.endswith('.json'):
            json_file_path = os.path.join(workdir, filename)
            csv_file_path = os.path.join(workdir, filename.replace('.json', '.csv'))
            
            with open(json_file_path, 'r') as json_file:
                data = json.load(json_file)
                
                daily_data = data.get('daily', {})
                dates = daily_data.get('time', [])
                
                headers = ['date'] + list(daily_data.keys())[1:]
                rows = []
                
                for i in range(len(dates)):
                    row = [dates[i]]
                    for key in headers[1:]:
                        row.append(daily_data[key][i])
                    rows.append(row)
                
                with open(csv_file_path, 'w', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerow(headers)
                    writer.writerows(rows)

    


def upload_files_to_s3(path, bucket_name, s3_prefix, profile_name='default'):
    import boto3
    session = boto3.Session(profile_name=profile_name)
    s3_client = session.client('s3', region_name='eu-west-1')
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".csv") or file.endswith(".txt"):
                local_file_path = os.path.join(root, file)
                s3_file_path = os.path.join(s3_prefix, os.path.relpath(local_file_path, path))
                try:
                    s3_client.upload_file(local_file_path, bucket_name, s3_file_path)
                    print(f"Uploaded: {local_file_path} to s3://{bucket_name}/{s3_file_path}")
                except Exception as e:
                    print(f"Failed to upload {local_file_path}: {e}")


def clear_local_folder(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)      


# python openmeteo_forecast.py city_forecast.csv 14  

if __name__ == "__main__":

    token = "OfA6OO1iDippHsJH"
    workdir = '/opt/GEAOSP_PY_POC_TURISMO/scripts/files/meteo/meteo_forecast'
    if not os.path.exists(workdir):
        os.makedirs(workdir)
    clear_local_folder(workdir)

    parser = argparse.ArgumentParser(description='Fetch weather forecast data.')
    parser.add_argument('archivo_urls', type=str, help='Path to the CSV file with city data.')
    parser.add_argument('dias_forecast', type=int, help='Number of forecast days.')

    args = parser.parse_args()

    print(f"*" * 80)
    print(f" procesando archivo_urls: {args.archivo_urls} y dias_forecast: {args.dias_forecast}")
    print(f"*" * 80)

    #archivo_urls = './prueba.csv'
    #dias_forecast = 14
    #dias_past = 1

    archivo_urls = args.archivo_urls
    dias_forecast = args.dias_forecast   
    download_forecast(archivo_urls, dias_forecast, token, workdir)   

    #transform_forecast(workdir)

    bucket_name = 'osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake'
    s3_prefix_base = 'innvatur/openmeteo/weather/forecast/'
    profile_name = 'default'  # Replace with your AWS profile name
    upload_files_to_s3(workdir, bucket_name, s3_prefix_base, profile_name)

    clear_local_folder(workdir)
