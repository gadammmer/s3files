#!/bin/bash
echo "----------------------------------------------------------"
echo "procesando ficheros ATRIBUS                               "
echo "----------------------------------------------------------"


fechaini=$1
fechafin=$2

starttime=$(date -d "$fechaini 00:00:00" +"%s")000
endtime=$(date -d "$fechafin 23:59:59" +"%s")000
echo $starttime
echo $endtime

token="Authorization: Bearer 4My49ikmiDUHVSXApj2ZRMcg74cagE8xbdp9ftZmlu0="
name=$3
staging="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/atribus_stagging"


url="https://app.atribus.com/api/invattur/${name}?source=&categories=28,29,30,31,32,33,80,34,35,36,37,38,40,41,83,44,45,46,47,48,49,50,51,52,74,55,81,78,56,57,59,60,61,62,63,65,85,66,67,79,84,69,77,82,70,71,72,73,75,76&searchs=&startTime=${starttime}&endTime=${endtime}&idApp=1"
file_csv="${fechaini}_${fechafin}_${3}.csv"

curl -H "$token" "$url" > ${staging}/tmp.json
jq -r '.sources[] | .source as $source | .categories[] | .idCategory as $idCategory | .tags[] | .subtag as $subtag | .tag as $tag | .counties[] | [$source, $idCategory, $subtag, $tag, .countyCode, .reputation, .evolutionReputation[].negative, .evolutionReputation[].name, .evolutionReputation[].neutral, .evolutionReputation[].positive] | @csv' ${staging}/tmp.json > ${staging}/tmp.csv

if cmp -s ${staging}/tmp.csv ${staging}/${file_csv}; then
 echo "............................................................................sin cambios en el fichero $ID"
else
 mv ${staging}/tmp.csv  /opt/GEAOSP_PY_POC_TURISMO/scripts/files/atribus_stagging/$file_csv
 aws s3 cp ${staging}/${file_csv}   s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/  
fi


