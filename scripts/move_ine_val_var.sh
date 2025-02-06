#!/bin/bash

echo "------------------------------------------------------------------"
echo "           obteniendo datos para ST_INE_VAL_VAR                   "
echo "------------------------------------------------------------------"

archivo_resultados="/opt/GEAOSP_PY_POC_TURISMO/scripts/resultados_ine_val_var.csv"
sudo rm /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_val_var/*


while IFS="," read -r  METADATA_VARIABLE_ID FK_OPERACION; do
	urls="https://servicios.ine.es/wstempus/js/ES/VALORES_VARIABLEOPERACION/${METADATA_VARIABLE_ID}/${FK_OPERACION}"
	echo "URL: $urls"
	rut="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_val_var/"
	file_json="${rut}/${METADATA_VARIABLE_ID}_${FK_OPERACION}.json"
	file_csv="${rut}/${METADATA_VARIABLE_ID}_${FK_OPERACION}.csv"

	curl "$urls" > "$file_json"

	#FK_OPERACION=$(awk -F',' -v fam="$FK_OPERACION" '$1==fam {print $2}' "$archivo_resultados")
	FK_OPERACION_VAR="$FK_OPERACION"
	METADATA_VARIABLE_ID_VAR="$METADATA_VARIABLE_ID"

	echo "FK_OPERACION: $FK_OPERACION"

	jq -r  --arg FK_OPERACION_VAR "$FK_OPERACION_VAR" --arg METADATA_VARIABLE_ID_VAR "$METADATA_VARIABLE_ID_VAR" '.[] | [.Id,$METADATA_VARIABLE_ID_VAR,.Nombre,.Codigo,$FK_OPERACION_VAR] | @csv' "$file_json" > tmp.csv

	if cmp -s tmp.csv $file_csv; then
		echo ".....................................................................................sin cambios en el fichero $file_csv"
	else
		mv tmp.csv $file_csv
		aws s3 cp $file_csv s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine_val_var/
	fi
	
done < "$archivo_resultados"


