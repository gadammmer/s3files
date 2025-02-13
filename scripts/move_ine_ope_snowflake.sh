#!/bin/bash
echo "-----------------------------------------------------------------"
echo "           obteniendo datos para ST_INE_TAB_OPE                  "
echo "-----------------------------------------------------------------"

linkOPE="https://servicios.ine.es/wstempus/js/ES/TABLAS_OPERACION/"
linkSER="https://servicios.ine.es/wstempus/jsCache/ES/SERIES_TABLA/"
archivo_urls="/opt/GEAOSP_PY_POC_TURISMO/scripts/INE_FAMILIAS.csv"
extra_info_file="/opt/GEAOSP_PY_POC_TURISMO/scripts/extra_info_ine_ope.csv"
extra_info_file_ser="/opt/GEAOSP_PY_POC_TURISMO/scripts/extra_info_ine_ser.csv"
fecha=$(date +"%d%m%y")

# Eliminar archivo extra_info.csv si existe
sudo rm -f "$extra_info_file"
sudo touch $extra_info_file
sudo chmod 777 $extra_info_file

sudo rm -f "$extra_info_file_ser"
sudo touch $extra_info_file_ser
sudo chmod 777 $extra_info_file_ser


sudo rm /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_ope/*
sudo rm /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_ser/*
sudo rm /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_ser/*
sudo rm /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_val_var/*

while IFS="," read -r FAMILIA ID; do
        url="$linkOPE$ID"
        rut="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_ope"
        file_json="${rut}/$FAMILIA.json"
	file_csv="${FAMILIA}_${ID}.csv"

        curl "$url" > "$file_json"

	cd $rut
        # Añadir el ID correspondiente del archivo INE_FAMILIAS.csv al CSV generado

        ID_FAMILIA=$(awk -F',' -v fam="$FAMILIA" '$1==fam {print $2}' "$archivo_urls")

    	# Obtener los IDs del JSON
    	IDS_JSON=$(jq -r '.[].Id' "$file_json")

        jq -r --arg ID_FAMILIA "$ID_FAMILIA" '.[] | [.Id, .Nombre, .Codigo, .FK_Periodicidad, .FK_Publicacion, .FK_Periodo_ini, .Anyo_Periodo_ini, .Ultima_Modificacion, $ID_FAMILIA] | @csv' "$file_json" > tmp.csv

	if cmp -s tmp.csv /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_ope/$file_csv; then
		echo ".....................................................................................sin cambios en el fichero $file_csv"
	else
		mv tmp.csv /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_ope/$file_csv
		#aws s3 cp /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_ope/$file_csv  s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine_ope_tab/
	fi

	#rm "$file_json"
	echo
	echo "Procesado ${FAMILIA}.."
	echo
	echo
	cd /opt/GEAOSP_PY_POC_TURISMO/scripts/

	# Iterar sobre los IDs del JSON y mostrarlos junto con el ID de la familia
	while IFS= read -r ID_JSON; do
		file_serial="$ID_JSON,$ID_FAMILIA,$FAMILIA"
		echo "$file_serial" >> "$extra_info_file"
	done <<< "$IDS_JSON"

done < "$archivo_urls"

echo "------------------------------------------------------------------"
echo "           obteniendo datos para ST_INE_SER_TAB                   "
echo "------------------------------------------------------------------"


while IFS="," read -r  SER_ID OPE_ID FAMILIA; do

	echo "OPE: ${OPE_ID} + ${FAMILIA}"
	ID_SERIAL=$(awk -F',' -v fam="$SER_ID" '$1==fam {print $1; exit}' "$extra_info_file")
	echo "$ID_SERIAL"

	url="$linkSER$ID_SERIAL?tip=M"
	rut="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_ser"
	file_json="${rut}/$FAMILIA.json"
	file_csv="${rut}/${FAMILIA}_${SER_ID}.csv"

	curl "$url" > "$file_json"

	#VARIABLE_ID_JSON=$(jq -r '.[] | (.MetaData | map([.Variable.Id]) [])'")


        #print("VARIABLE_ID_JSON : ${VARIABLE_ID_JSON}")

	jq -r --arg ID_SERIAL "$ID_SERIAL" '.[] | [.Id, .COD, .FK_Operacion, .Nombre, .Decimales, .FK_Periodicidad, .FK_Publicacion, .FK_Clasificacion, .FK_Escala, .FK_Unidad] + (.MetaData | map([.Id, .Nombre, .Codigo, .Variable.Id, .Variable.Nombre, .Variable.Codigo, $ID_SERIAL])[]) | @csv' "$file_json" > tmp.csv

	if cmp -s tmp.csv $file_csv; then
		echo ".....................................................................................sin cambios en el fichero $file_csv"
	else
		mv tmp.csv $file_csv
		#aws s3 cp $file_csv s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine_ser_tab/
	fi
	#rm "$file_json"

	

done < "$extra_info_file"

echo "------------------------------------------------------------------"
echo "           obteniendo datos para ST_INE_VAL_VAR                   "
echo "------------------------------------------------------------------"

