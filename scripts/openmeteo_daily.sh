#!/bin/bash

archivo_urls=$1
fecha_ini="2023-12-01"
fecha_fin="2024-09-25"
token="OfA6OO1iDippHsJH"
while IFS="," read -r  CITYCODE COUNTRYCODE longitud latitud; do
    fichero_salida="$CITYCODE-$COUNTRYCODE.json"
    curl "https://customer-archive-api.open-meteo.com/v1/archive?apikey="$token"&latitude="$latitud"&longitude="$longitud"&start_date="$fecha_ini"&end_date="$fecha_fin"&daily=weather_code,temperature_2m_max,temperature_2m_min,temperature_2m_mean,apparent_temperature_max,apparent_temperature_min,apparent_temperature_mean,sunrise,sunset,daylight_duration,sunshine_duration,precipitation_sum,rain_sum,snowfall_sum,precipitation_hours,wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant&time_mode=time_interval&timezone=auto"  > /opt/GEAOSP_PY_POC_TURISMO/scripts/files/meteo/meteo_daily_json/$fichero_salida
    #echo"https://customer-archive-api.open-meteo.com/v1/archive?apikey="$token"&latitude="$latitud"&longitude="$longitud"&start_date="$fecha_ini"&end_date="$fecha_fin"&daily=weather_code,temperature_2m_max,temperature_2m_min,apparent_temperature_max,apparent_temperature_min,sunrise,sunset,daylight_duration,sunshine_duration,precipitation_sum,rain_sum,snowfall_sum,precipitation_hours,wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant&time_mode=time_interval&timezone=auto"  > ./meteo/$fichero_salida

done < "$archivo_urls"
