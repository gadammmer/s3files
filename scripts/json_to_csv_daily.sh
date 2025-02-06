#!/bin/bash
echo "-----------------------------"
echo "transformando ficheros json a csv"
for i in $(ls /opt/GEAOSP_PY_POC_TURISMO/scripts/files/meteo/meteo_daily_json/*.json); do jq -r '[.daily[]] | transpose[] | @csv' $i > /opt/GEAOSP_PY_POC_TURISMO/scripts/files/meteo/meteo_daily_csv/`(basename $i)`.2.csv; done;
echo "-----------------------------"

# aws s3 sync /opt/GEAOSP_PY_POC_TURISMO/scripts/files/meteo/meteo_daily_csv/ s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/openmeteo/daily/
