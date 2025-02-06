#!/bin/bash

# Obtener la fecha proporcionada y la encuesta
fecha_ini=$1
fecha_fin=$2
survey=$3
categories="28,29,30,31,32,33,80,34,35,36,37,38,40,41,83,44,45,46,47,48,49,50,51,52,74,55,81,78,56,57,59,60,61,62,63,65,85,66,67,79,84,69,77,82,70,71,72,73,75,76"

# Convertir las fechas a formato que pueda entender el bucle
current_date=$(date -I -d "$fecha_ini")  # Inicialmente la fecha de inicio
end_day=$(date -I -d "$fecha_fin")      # La fecha de fin en formato ISO (AAAA-MM-DD)

# Crear la carpeta para el tipo de encuesta si no existe
folder="./$survey"
mkdir -p "$folder"

# Bucle para recorrer desde fecha_ini hasta fecha_fin
while [ "$current_date" != "$(date -I -d "$end_day + 1 day")" ]; do
    # Crear la fecha de inicio del día a las 00:00:01
    start_date=$(date -d "$current_date 00:00:01" +"%s")000

    # Crear la fecha de fin del día a las 23:59:59
    end_date=$(date -d "$current_date 23:59:59" +"%s")000

    # Imprimir las fechas de cada día
    echo "start_date: $start_date"
    echo "end_date: $end_date"

    cd $folder
    
    # Ejecutar la solicitud curl con las variables correctamente expandidas
    timestamp=$(date +%Y%m%d%H%M%S)
    json_file="tmp_${timestamp}.json"
    file_csv="${survey}_${fecha_ini}_${fecha_fin}.csv"
    staging="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/atribus_stagging/${survey}"
    if [ "$survey" == "subtags" ]; then 
        curl_output=$(curl -H "Authorization: Bearer 4My49ikmiDUHVSXApj2ZRMcg74cagE8xbdp9ftZmlu0=" "https://app.atribus.com/api/invattur/${survey}?idTag=1")
        echo curl -H "Authorization: Bearer 4My49ikmiDUHVSXApj2ZRMcg74cagE8xbdp9ftZmlu0=" "https://app.atribus.com/api/invattur/${survey}?idTag=1"
    else
        curl_output=$(curl -H "Authorization: Bearer 4My49ikmiDUHVSXApj2ZRMcg74cagE8xbdp9ftZmlu0=" "https://app.atribus.com/api/invattur/${survey}?source=&categories=${categories}&searchs=&startTime=${start_date}&endTime=${end_date}&idApp=1")
        echo curl -H "Authorization: Bearer 4My49ikmiDUHVSXApj2ZRMcg74cagE8xbdp9ftZmlu0=" "https://app.atribus.com/api/invattur/${survey}?source=&categories=${categories}&searchs=&startTime=${start_date}&endTime=${end_date}&idApp=1"
    fi
    # Guardar la salida del comando curl en el archivo JSON
    echo "$curl_output" > "$json_file"
    echo
    cat "$json_file" | jq -r
    echo

    #echo
    #echo "Registros DE en $json_file"
    #grep -o -i 'DE-' $json_file | wc -l

    # Verificar si curl ha generado algún error
    if [ $? -ne 0 ]; then
        echo "Error al ejecutar curl. Por favor, verifica la conexión a internet y la URL proporcionada."
        exit 1
    fi

    # Procesar el archivo JSON descargado y guardarlo en un archivo CSV
    jq_output=$(jq --version)
    echo
    echo ".............................................................."
    echo
    echo "jq version: $jq_output"

    if [ "$survey" == "sources" ]; then
        mkdir -p ${staging}
        cat "$json_file" | jq -r '.sources[] | .name as $name | .idSource as $idSource | [$idSource, $name] | @csv' > ${staging}/tmp.csv
    elif [ "$survey" == "categories" ]; then
        mkdir -p ${staging}
        cat "$json_file" | jq -r '.categories[] | [.idCategory, .idApp, .paused, .dateCreated, .nameApp, .name, .active] | @csv' > ${staging}/tmp.csv
    elif [ "$survey" == "tags" ]; then
        mkdir -p ${staging}
        cat "$json_file" | jq -r '.tags[] | [.idTag, .tag] | @csv' > ${staging}/tmp.csv
    elif [ "$survey" == "subtags" ]; then
        mkdir -p ${staging}
        cat "$json_file" | jq -r '.subtags[] | [.idSubtag, .subtag, .idTag, (.filterBlackuser | join(";")), (.sources | join(";")), .filterMinFollowers, .filterAvatar, (.filterWhitelist | join(";")), (.filterDescription | join(";")), (.filterBlacklist | join(";")), .filterVerified, (.filterBlackuser | join(";"))] | @csv' > ${staging}/tmp.csv
    elif [ "$survey" == "geolocations" ]; then
        mkdir -p ${staging}
        cat "$json_file" | jq -r --arg start_date "$start_date" --arg end_date "$end_date" '.categories[] | .idCategory as $idCategory | .geolocations[] | ["1", $start_date, $end_date, "source_placeholder", $idCategory, "tag_placeholder", "subtag_placeholder", .countryCode, .country, .countyCode, .county, .count] | @csv' > ${staging}/tmp.csv
    elif [ "$survey" == "interests" ]; then
        mkdir -p ${staging}
        cat "$json_file" | jq -r --arg start_date "$start_date" --arg end_date "$end_date" '.sources[] | .source as $source | .categories[] | .idCategory as $idCategory | .counties[] | .countyCode as $countyCode | .interests[] | .tag as  $tag | .subtags[] | select(.negative or .interest or .neutral or .reputation or .positive) | ["1", $start_date, $end_date, $source, $idCategory, $tag, .subtag, $countyCode, .interest // 0, .reputation // 0, .positive // 0, .negative // 0, .neutral // 0] | @csv' > ${staging}/tmp.csv
    elif [ "$survey" == "reputation" ]; then
        mkdir -p ${staging}
        cat "$json_file" | jq -r --arg start_date "$start_date" --arg end_date "$end_date" '.sources[] | .source as $source | .categories[] | .idCategory as $idCategory | .tags[] | .subtag as $subtag | .tag as $tag | .counties[] | ["1",$start_date, $end_date,$source, $idCategory, $tag, $subtag, .countyCode, .reputation, .evolutionReputation[].name, .reputation, .evolutionReputation[].positive, .evolutionReputation[].negative, .evolutionReputation[].neutral] | @csv' > ${staging}/tmp.csv
    elif [ "$survey" == "probabiltyofrepeat" ]; then
        mkdir -p ${staging}
        cat "$json_file" | jq -r --arg start_date "$start_date" --arg end_date "$end_date" '.sources[] | .source as $source | .categories[] | .idCategory as $idCategory | .tags[] | .subtag as $subtag | .tag as $tag | .counties[] | ["1", $start_date, $end_date, $source, $idCategory, $tag, $subtag, .countyCode, .probabiltyofrepeat, .positive, .negative, .neutral] | @csv' > ${staging}/tmp.csv
    elif [ "$survey" == "wordcloud" ]; then
        mkdir -p ${staging}
        cat "$json_file" | jq -r --arg start_date "$fecha_ini" --arg end_date "$fecha_fin" '.sources[] | .source as $source | .categories[] | .idCategory as $idCategory | .tags[] | .subtag as $subtag | .tag as $tag | .counties[] | .county_code as $countyCode | if .wordcloud then .wordcloud[] else empty end | ["1",$start_date, $end_date, $source, $idCategory, $tag, $subtag, $countyCode, .word,.count] | @csv' > ${staging}/tmp.csv
    elif [ "$survey" == "demographics" ]; then
        mkdir -p ${staging}
        cat "$json_file" | jq -r --arg start_date "$start_date" --arg end_date "$end_date" '
        .categories[] | .idCategory as $idCategory | select(.counties != null) | .counties[] | .countyCode as $countyCode |
        [
        (.demographics.organization[] | "1,\($start_date),\($end_date),\($idCategory),\($countyCode),organization,\(.years),\(.count)"),
        (.demographics.man[] | "1,\($start_date),\($end_date),\($idCategory),\($countyCode),man,\(.years),\(.count)"),
        (.demographics.women[] | "1,\($start_date),\($end_date),\($idCategory),\($countyCode),women,\(.years),\(.count)")
        ] | .[]' > ${staging}/tmp.csv
    elif [ "$survey" == "stat" ]; then
        mkdir -p ${staging}
        cat "$json_file" | jq -r --arg start_date "$start_date" --arg end_date "$end_date" '.categories[] |  .idCategory as $idCategory | select(.counties != null) | .counties[] | .countyCode as $countyCode | .statistics | ["1", $start_date, $end_date, $idCategory, $countyCode, .engagement, .impression, .interaction, .reach, .value] | @csv' > ${staging}/tmp.csv
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
        #aws s3 cp ${staging}/${file_csv}   s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/${survey}/
    fi

    # Avanzar al siguiente día
    current_date=$(date -I -d "$current_date + 1 day")
done

cd ..
rm -rf $folder





