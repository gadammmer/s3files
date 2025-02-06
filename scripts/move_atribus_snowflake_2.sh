#!/bin/bash

# Verificar si se proporcionó la fecha como argumento
if [ $# -ne 3 ]; then
    echo "Por favor, proporciona una fecha en formato YYYYMMDD como argumento y el tipo de encuesta (interest, reputation, wordcloud, demographics)."
    exit 1
fi

# Obtener la fecha proporcionada y la encuesta
fecha_ini=$1
fecha_fin=$2
survey=$3
categories="28,29,30,31,32,33,80,34,35,36,37,38,40,41,83,44,45,46,47,48,49,50,51,52,74,55,81,78,56,57,59,60,61,62,63,65,85,66,67,79,84,69,77,82,70,71,72,73,75,76"

# Validar el tipo de encuesta
if [ "$survey" != "interests" ] && [ "$survey" != "reputation" ] && [ "$survey" != "wordcloud" ] && [ "$survey" != "demographics" ]; then
    echo "El tipo de encuesta debe ser 'interest', 'reputation', 'wordcloud' o 'demographics'."
    exit 1
fi

# Crear la fecha de inicio a las 00:00:01
start_date=$(date -d "$fecha_ini 00:00:01" +"%s")000

# Crear la fecha de fin a las 23:59:59
end_date=$(date -d "$fecha_fin 23:59:59" +"%s")000

echo "start_date: $start_date"
echo "end_date: $end_date"

# Crear la carpeta para el tipo de encuesta si no existe
folder="./$survey"
mkdir -p "$folder"
cd $folder

# Ejecutar la solicitud curl con las variables correctamente expandidas
timestamp=$(date +%Y%m%d%H%M%S)
json_file="tmp_${timestamp}.json"
file_csv="${survey}_${fecha_ini}_${fecha_fin}.csv"
staging="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/atribus_stagging/${survey}"
curl_output=$(curl -H "Authorization: Bearer 4My49ikmiDUHVSXApj2ZRMcg74cagE8xbdp9ftZmlu0=" "https://app.atribus.com/api/invattur/${survey}?source=&categories=${categories}&searchs=&startTime=${start_date}&endTime=${end_date}&idApp=1")

echo curl -H "Authorization: Bearer 4My49ikmiDUHVSXApj2ZRMcg74cagE8xbdp9ftZmlu0=" "https://app.atribus.com/api/invattur/${survey}?source=&categories=${categories}&searchs=&startTime=${start_date}&endTime=${end_date}&idApp=1"
# Guardar la salida del comando curl en el archivo JSON
echo "$curl_output" > "$json_file"

echo
echo "Registros DE en $json_file"
grep -o -i 'DE-' $json_file | wc -l


# Verificar si curl ha generado algún error
if [ $? -ne 0 ]; then
    echo "Error al ejecutar curl. Por favor, verifica la conexión a internet y la URL proporcionada."
    exit 1
fi

# Procesar el archivo JSON descargado y guardarlo en un archivo CSV
jq_output=$(jq --version)
echo "jq version: $jq_output"

if [ "$survey" == "interests" ]; then
    cat "$json_file" | jq -r --arg start_date "$fecha_ini" --arg end_date "$fecha_fin" '.sources[] | .source as $source | .categories[] | .idCategory as $idCategory | .counties[] | .countyCode as $countyCode | .interests[] | .tag as $tag | .subtags[] | select(.negative or .interest or .neutral or .reputation or .positive) | [$source, $idCategory, $tag, .subtag, $countyCode, .interest // 0, .reputation // 0, .positive // 0, .negative // 0, .neutral // 0, $start_date, $end_date] | @csv' > ${staging}/tmp.csv
elif [ "$survey" == "reputation" ]; then
    cat "$json_file" | jq -r --arg start_date "$fecha_ini" --arg end_date "$fecha_fin" '.sources[] | .source as $source | .categories[] | .idCategory as $idCategory | .tags[] | .subtag as $subtag | .tag as $tag | .counties[] | [$source, $idCategory, $subtag, $tag, .countyCode, .reputation, .evolutionReputation[].negative, .evolutionReputation[].name, .evolutionReputation[].neutral, .evolutionReputation[].positive, $start_date, $end_date] | @csv' > ${staging}/tmp.csv
elif [ "$survey" == "wordcloud" ]; then
    cat "$json_file" | jq -r --arg start_date "$fecha_ini" --arg end_date "$fecha_fin" '.sources[] | .source as $source | .categories[] | .idCategory as $idCategory | .tags[] | .subtag as $subtag | .tag as $tag | .counties[] | .county_code as $countyCode | if .wordcloud then .wordcloud[] else empty end | [$source, $idCategory, $tag, $subtag, $countyCode, .count, .word, $start_date, $end_date] | @csv' > ${staging}/tmp.csv
elif [ "$survey" == "demographics" ]; then
    cat "$json_file" | jq -r --arg start_date "$fecha_ini" --arg end_date "$fecha_fin" '["countyCode", "idCategory", "organization_count", "organization_years", "man_count", "man_years", "women_count", "women_years", "start_date", "end_date"], (.categories[] | select(length > 0) | .idCategory as $id_category | .counties[] | [.countyCode, ($id_category // "null"), (.demographics.organization[0].count // "null"), (.demographics.organization[0].years // "null"), (.demographics.man[0].count // "null"), (.demographics.man[0].years // "null"), (.demographics.women[0].count // "null"), (.demographics.women[0].years // "null"), $start_date, $end_date]) | @csv' > ${staging}/tmp.csv
fi

if cmp -s ${staging}/tmp.csv ${staging}/${file_csv}; then
    echo "............................................................................sin cambios en el fichero $file_csv"
else
    echo " el fichero $file_csv está listo en ${staging}"
    echo
    mv ${staging}/tmp.csv  /opt/GEAOSP_PY_POC_TURISMO/scripts/files/atribus_stagging/${survey}/$file_csv
    head /opt/GEAOSP_PY_POC_TURISMO/scripts/files/atribus_stagging/${survey}/${file_csv}
    echo
    echo "Registros DE en /opt/GEAOSP_PY_POC_TURISMO/scripts/files/atribus_stagging/${survey}/${file_csv}"
    echo
    grep -o -i 'DE-' /opt/GEAOSP_PY_POC_TURISMO/scripts/files/atribus_stagging/${survey}/${file_csv} | wc -l
    aws s3 cp ${staging}/${file_csv}   s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/${survey}/
fi

cd ..
rm -rf $folder
#rm ${staging}/*

