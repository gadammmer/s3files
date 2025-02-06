import csv
import requests
import datetime
import argparse
import os
import json
import shutil
import time
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry



def download_forecast(archivo_urls, dias_forecast, token, workdir,apikey):

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    with open(archivo_urls, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if reader.line_num == 1:
                continue
            CITYCODE, COUNTRYCODE, longitud, latitud = row
            fichero_salida_csv = f"{CITYCODE}-{COUNTRYCODE}-fcst.csv"
            url = "https://customer-api.open-meteo.com/v1/forecast"
            params = {
                "latitude": latitud,
                "longitude": longitud,
                "forecast_days": dias_forecast,
                "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min","temperature_2m_mean", 
                          "apparent_temperature_max","apparent_temperature_min","apparent_temperature_mean",
                          "sunrise","sunset","daylight_duration","sunshine_duration","precipitation_sum",
                          "rain_sum","snowfall_sum","precipitation_hours","wind_speed_10m_max",
                          "wind_gusts_10m_max","wind_direction_10m_dominant"],
                "timezone": "auto",
                "apikey": apikey
            }
            
            try:
                responses = openmeteo.weather_api(url, params=params)
                response = responses[0]
                #print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
                #print(f"Elevation {response.Elevation()} m asl")
                #print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
                #print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")
            except Exception as e:
                print(f"Error processing {CITYCODE}-{COUNTRYCODE}: {e}")
                continue

            # Process daily data. The order of variables needs to be the same as requested.
            daily = response.Daily()
            daily_weather_code = daily.Variables(0).ValuesAsNumpy()
            daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
            daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
            daily_temperature_2m_mean = daily.Variables(3).ValuesAsNumpy()
            daily_apparent_temperature_max = daily.Variables(4).ValuesAsNumpy()
            daily_apparent_temperature_min = daily.Variables(5).ValuesAsNumpy()
            daily_apparent_temperature_mean = daily.Variables(6).ValuesAsNumpy()
            daily_sunrise = daily.Variables(7).ValuesAsNumpy()
            daily_sunset = daily.Variables(8).ValuesAsNumpy()
            daily_daylight_duration = daily.Variables(9).ValuesAsNumpy()
            daily_sunshine_duration = daily.Variables(10).ValuesAsNumpy()
            daily_precipitation_sum = daily.Variables(11).ValuesAsNumpy()
            daily_rain_sum = daily.Variables(12).ValuesAsNumpy()
            daily_snowfall_sum = daily.Variables(13).ValuesAsNumpy()
            daily_precipitation_hours = daily.Variables(14).ValuesAsNumpy()
            daily_wind_speed_10m_max = daily.Variables(15).ValuesAsNumpy()
            daily_wind_gusts_10m_max = daily.Variables(16).ValuesAsNumpy()
            daily_wind_direction_10m_dominant = daily.Variables(17).ValuesAsNumpy()


            daily_data = {"date": pd.date_range(
                start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
                end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
                freq = pd.Timedelta(seconds = daily.Interval()),
                inclusive = "left"
            )}
            daily_data["weather_code"] = daily_weather_code
            daily_data["temperature_2m_max"] = daily_temperature_2m_max
            daily_data["temperature_2m_min"] = daily_temperature_2m_min
            daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
            daily_data["apparent_temperature_max"] = daily_apparent_temperature_max
            daily_data["apparent_temperature_min"] = daily_apparent_temperature_min
            daily_data["apparent_temperature_mean"] = daily_apparent_temperature_mean
            daily_data["sunrise"] = pd.to_datetime(daily_sunrise, unit = "s", utc = True)
            daily_data["sunset"] = pd.to_datetime(daily_sunset, unit = "s", utc = True)
            daily_data["daylight_duration"] = pd.to_timedelta(daily_daylight_duration, unit = "s")
            daily_data["sunshine_duration"] = pd.to_timedelta(daily_sunshine_duration, unit = "s")
            daily_data["precipitation_sum"] = daily_precipitation_sum
            daily_data["rain_sum"] = daily_rain_sum
            daily_data["snowfall_sum"] = daily_snowfall_sum
            daily_data["precipitation_hours"] = daily_precipitation_hours
            daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
            daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
            daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant


            daily_dataframe = pd.DataFrame(data = daily_data)
            #print(daily_dataframe)
            daily_dataframe.to_csv(f'{workdir}/{fichero_salida_csv}', index=False)



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
    #clear_local_folder(workdir)

    parser = argparse.ArgumentParser(description='Fetch weather forecast data.')
    parser.add_argument('archivo_urls', type=str, help='Path to the CSV file with city data.')
    parser.add_argument('dias_forecast', type=int, help='Number of forecast days.')

    args = parser.parse_args()

    print(f"*" * 80)
    print(f" procesando archivo_urls: {args.archivo_urls} y dias_forecast: {args.dias_forecast}")
    print(f"*" * 80)
    #dias_past = 1

    archivo_urls = args.archivo_urls
    dias_forecast = args.dias_forecast   
    download_forecast(archivo_urls, dias_forecast, token, workdir,token)   

    bucket_name = 'osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake'
    s3_prefix_base = 'innvatur/openmeteo/weather/forecast/'
    profile_name = 'default'  # Replace with your AWS profile name
    upload_files_to_s3(workdir, bucket_name, s3_prefix_base, profile_name)

    clear_local_folder(workdir)
