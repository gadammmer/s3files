#!/bin/bash

FILE_PATH=$1
dep=$2
WORKING_DIR="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_urls/"

# limpiar el directorio de trabajo
for file in "$WORKING_DIR"*.csv; do
    if [[ -f "$file" ]]; then
        rm "$file"
    fi
done


while IFS=, read -r FILE_OUTPUT FILE_TMP URL; do
    # Omitir la primera línea del fichero
    if [ "$FILE_OUTPUT" == "FILE_OUTPUT" ]; then
        continue
    fi

    FILE_OUTPUT=$(echo "$FILE_OUTPUT" | sed "s/{dep}/$dep/g")
    FILE_TMP=$(echo "$FILE_TMP" | sed "s/{dep}/$dep/g")
    URL=$(echo "$URL" | sed "s/{dep}/$dep/g")

    file_tmp_json="$WORKING_DIR$FILE_TMP.json"
    file_tmp_csv="$WORKING_DIR$FILE_TMP.csv"
    file_output_csv="$WORKING_DIR$FILE_OUTPUT.csv"

    # Hacer curl de la URL y guardar el contenido en FILE_TMP
    #echo "Descargando datos de la URL $URL..."
    curl -s "$URL" -o "$file_tmp_json"
    jq -r '.[] | .COD as $cod | .Nombre as $nombre | .FK_Unidad as $unidad | .FK_Escala as $escala | .Data[] | [$cod, $nombre, $unidad, $escala, .Fecha, .FK_TipoDato, .FK_Periodo, .Anyo, .Valor, .Secreto] | @csv' ${file_tmp_json} > ${file_tmp_csv}
    rm $file_tmp_json
    echo "añadiendo datos de $file_tmp_csv a $file_output_csv..."
    cat "${file_tmp_csv}" >> "${file_output_csv}"
    rm "$file_tmp_csv"
done < "$FILE_PATH"

# Recorrer todos los FILE_OUTPUTS creados y copiarlos a S3
for file in "$WORKING_DIR"*.csv; do
    if [[ -f "$file" ]]; then
	aws s3 cp $file   s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine/
        echo "Copiando $file a S3..."
    fi
done
