#!/bin/bash

# Obtener la fecha proporcionada y la encuesta
fecha_ini=$1
fecha_fin=$2
survey=$3
categories="28,29,30,31,32,33,80,34,35,36,37,38,40,41,83,44,45,46,47,48,49,50,51,52,74,55,81,78,56,57,59,60,61,62,63,65,85,66,67,79,84,69,77,82,70,71,72,73,75,76"

# Crear la fecha de inicio a las 00:00:01
start_date=$(date -d "$fecha_ini 00:00:01" +"%s")

# Crear la fecha de fin a las 23:59:59
end_date=$(date -d "$fecha_fin 23:59:59" +"%s")

echo "start_date: $start_date"
echo "end_date: $end_date"

echo curl -H "Authorization: Bearer 4My49ikmiDUHVSXApj2ZRMcg74cagE8xbdp9ftZmlu0=" "https://app.atribus.com/api/invattur/${survey}?source=&categories=${categories}&searchs=&startTime=${start_date}&endTime=${end_date}&idApp=1"

