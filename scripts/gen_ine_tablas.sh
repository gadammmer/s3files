#!/bin/bash
echo "----------------------------------------------------------"
echo "           obteniendo tablas por familia                  "
echo "----------------------------------------------------------"
# ./etl_familias.sh ID_FAMILIAS_2.txt

link="https://servicios.ine.es/wstempus/js/ES/TABLAS_OPERACION/"
archivo_urls=$1
file_csv="/opt/GEAOSP_PY_POC_TURISMO/scripts/INE_TABLAS.csv"
rm -f "$file_csv"
echo "\"FAMILIA\",\"Id\",\"Nombre\",\"Codigo\",\"FK_Periodicidad\",\"FK_Publicacion\",\"FK_Periodo_ini\",\"Anyo_Periodo_ini\",\"FechaRef_fin\",\"Ultima_Modificacion\"" > "$file_csv"

while IFS="," read -r FAMILIA ID; do
        fecha=$(date +"%d%m%y")
        url="$link$ID"
        file_json="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_familias/$FAMILIA.json"
        curl "$url" > $file_json
        #jq -r --arg familia "$FAMILIA" '[$familia] + (.[0] | keys_unsorted), map([$familia, .[ keys_unsorted[] ]])[] | @csv' "$file_json" >> "$file_csv"
        jq -r --arg familia "$FAMILIA" '[$familia] + (.[0] | keys_unsorted), map([$familia, .[ keys_unsorted[] ]])[] | @csv' "$file_json" | tail -n +2 >> "$file_csv"


done < "$archivo_urls"
