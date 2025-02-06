#!/bin/bash
echo "----------------------------------------------------------"
echo "procesando ID's del fichero $1 con profundidad nult= $2"
echo "----------------------------------------------------------"
archivo_urls=$1
ruta_staging="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/tpx/"
nult=$2

#aws s3 cp /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/$file_csv   s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine/  
# FAMILIA,Id,Nombre,FK_Periodicidad,FK_Publicacion,Ultima_Modificacion,Anyo_Periodo_ini,Anyo_Periodo_fin,FK_Periodo_ini,FK_Periodo_fin,FechaRef_ini,FechaRef_fin,Codigo
while IFS=, read -r familia id nombre  FK_Periodicidad FK_Publicacion Ultima_Modificacion _; do
    if [ "$familia" == "TPX" ]; then
        url="https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/$id?nult=1"
        echo "Descargando datos de la URL $url..."
        file_csv="${ruta_staging}/${nult}_${id}_${familia}.csv"
	    file_json="${ruta_staging}/${nult}_${id}_${familia}.json"
        tmp_csv="./tmp.csv"
        curl -o $file_json "$url"
        # COD,Nombre,FK_Unidad,FK_Escala,Fecha,FK_TipoDato,FK_Periodo,Anyo,Valor,Secreto
        jq --arg Ultima_Modificacion "$Ultima_Modificacion" -r '.[] | .Nombre as $nombre | .Data[] | ["TPX", $nombre, "", "", $Ultima_Modificacion, "", "", "", .Valor, .Secreto] | @csv' "$file_json" > "$tmp_csv"
    fi
    if [ "$familia" != "TPX" ]; then
        url="https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/$id?nult=1"
        echo "Descargando datos de la URL $url..."
        file_csv="${ruta_staging}/${nult}_${id}_${familia}.csv"
	    file_json="${ruta_staging}/${nult}_${id}_${familia}.json"
        tmp_csv="./tmp.csv"
        curl -o $file_json "$url"
        jq -r '.[] | .COD as $cod | .Nombre as $nombre | .FK_Unidad as $unidad | .FK_Escala as $escala | .Data[] | [$cod, $nombre, $unidad, $escala, .Fecha, .FK_TipoDato, .FK_Periodo, .Anyo, .Valor, .Secreto] | @csv' "$file_json" > "$tmp_csv"
    fi



    if cmp -s tmp.csv $file_csv; then
	  echo ".....................................................................................sin cambios en el fichero $ID nult=$2"
        else
           mv tmp.csv  $file_csv
	      #aws s3 cp $file_csv   s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine/tpx/  --profile partner3
        fi
	rm $file_json

done < "$archivo_urls"
