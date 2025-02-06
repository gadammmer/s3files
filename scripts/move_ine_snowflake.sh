#!/bin/bash
echo "----------------------------------------------------------"
echo "procesando ID's del fichero $1 con profundidad nult= $2"
echo "----------------------------------------------------------"
archivo_urls=$1
link="https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/"

while  IFS="," read -r FAMILIA ID Nombre Codigo FK_Periodicidad FK_Publicacion FK_Periodo_ini Anyo_Periodo_ini FechaRef_fin Ultima_Modificacion; do
        fecha=$(date +"%d%m%y")
        familia_clean=${FAMILIA//\"/}
        dep="?nult=$2"
        url="$link$ID$dep"
        file_json="$ID.json"
        #file_csv="$2_${ID}.csv"
        file_csv="$2_${ID}_${familia_clean}.csv"
        curl "$url" > $file_json
	echo "obteniendo $url"
        jq -r '.[] | .COD as $cod | .Nombre as $nombre | .FK_Unidad as $unidad | .FK_Escala as $escala | .Data[] | [$cod, $nombre, $unidad, $escala, .Fecha, .FK_TipoDato, .FK_Periodo, .Anyo, .Valor, .Secreto] | @csv' $file_json > tmp.csv
        #jq -r '(now | strftime("%Y.%m.%d")) as $date | .[] | .COD as $cod | .Nombre as $nombre | .FK_Unidad as $unidad | .FK_Escala as $escala | .Data[] | [$cod, $nombre, $unidad, $escala, .Fecha, .FK_TipoDato, .FK_Periodo, .Anyo, .Valor, .Secreto, $date] | @csv' $file_json > tmp.csv
        if cmp -s tmp.csv /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/$file_csv; then
          echo ".....................................................................................sin cambios en el fichero $ID nult=$2"
        else
           mv tmp.csv /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/$file_csv
           aws s3 cp /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/$file_csv   s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine/  
        fi
        rm $file_json
done < "$archivo_urls"
