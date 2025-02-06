
#!/bin/bash
echo "----------------------------------------------------------"
echo "procesando ficheros tempus grandes   "
echo "----------------------------------------------------------"


files_and_urls=(
	"1_5300_INEEXTINT_115:5.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:5"
	"1_5300_INEEXTINT_115:12.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:12"
	"1_5300_INEEXTINT_115:15.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:15"
	"1_5300_INEEXTINT_115:19.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:19"
	"1_5300_INEEXTINT_115:22.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:22"
	"1_5300_INEEXTINT_115:24.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:24"
	"1_5300_INEEXTINT_115:30.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:30"
	"1_5300_INEEXTINT_115:41.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:41"
	"1_5300_INEEXTINT_115:23.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:23"
	"1_5300_INEEXTINT_115:44.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:44"
	"1_5300_INEEXTINT_115:50.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:50"
	"1_5300_INEEXTINT_115:33.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:33"
	"1_5300_INEEXTINT_115:8.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:8"
	"1_5300_INEEXTINT_115:35.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:35"
	"1_5300_INEEXTINT_115:38.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:38"
	"1_5300_INEEXTINT_115:39.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:39"
	"1_5300_INEEXTINT_115:6.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:6"
	"1_5300_INEEXTINT_115:10.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:10"
	"1_5300_INEEXTINT_115:25.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:25"
	"1_5300_INEEXTINT_115:34.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:34"
	"1_5300_INEEXTINT_115:37.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:37"
	"1_5300_INEEXTINT_115:40.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:40"
	"1_5300_INEEXTINT_115:42.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:42"
	"1_5300_INEEXTINT_115:47.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:47"
	"1_5300_INEEXTINT_115:49.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:49"
	"1_5300_INEEXTINT_115:3.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:3"
	"1_5300_INEEXTINT_115:14.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:14"
	"1_5300_INEEXTINT_115:17.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:17"
	"1_5300_INEEXTINT_115:20.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:20"
	"1_5300_INEEXTINT_115:45.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:45"
	"1_5300_INEEXTINT_115:9.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:9"
	"1_5300_INEEXTINT_115:18.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:18"
	"1_5300_INEEXTINT_115:26.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:26"
	"1_5300_INEEXTINT_115:43.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:43"
	"1_5300_INEEXTINT_115:4.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:4"
	"1_5300_INEEXTINT_115:13.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:13"
	"1_5300_INEEXTINT_115:46.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:46"
	"1_5300_INEEXTINT_115:7.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:7"
	"1_5300_INEEXTINT_115:11.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:11"
	"1_5300_INEEXTINT_115:16.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:16"
	"1_5300_INEEXTINT_115:28.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:28"
	"1_5300_INEEXTINT_115:53.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:53"
	"1_5300_INEEXTINT_115:36.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:36"
	"1_5300_INEEXTINT_115:29.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:29"
	"1_5300_INEEXTINT_115:31.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:31"
	"1_5300_INEEXTINT_115:32.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:32"
	"1_5300_INEEXTINT_115:2.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:2"
	"1_5300_INEEXTINT_115:48.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:48"
	"1_5300_INEEXTINT_115:21.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:21"
	"1_5300_INEEXTINT_115:27.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:27"
	"1_5300_INEEXTINT_115:51.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:51"
	"1_5300_INEEXTINT_115:52.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53000?nult=1&tv=115:52"
    # Add more tuples as needed
)


  for entry in "${files_and_urls[@]}"; do
      IFS=',' read -r nombre_fichero url <<< "$entry"
  	file_csv="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/${nombre_fichero}.csv"
  	file_json="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/${nombre_fichero}.json"
      curl "$url" > $file_json
      jq -r '.[] | .COD as $cod | .Nombre as $nombre | .FK_Unidad as $unidad | .FK_Escala as $escala | .Data[] | [$cod, $nombre, $unidad, $escala, .Fecha, .FK_TipoDato, .FK_Periodo, .Anyo, .Valor, .Secreto] | @csv' $file_json > $file_csv 
      rm $file_json
  done
 
 
output_file="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/1_53000_INEEXTINT.csv"

 for entry in "${files_and_urls[@]}"; do
 	IFS=',' read -r nombre_fichero url <<< "$entry"
 	file_csv="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/${nombre_fichero}.csv"
 	if [ -f "$file_csv" ]; then
 		tail -n +2 "$file_csv" >> "$output_file"
 		rm "$file_csv"
 		echo "File $file_csv appended to $output_file"
 	fi
 done
 aws s3 cp $output_file s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine/

files_and_urls=(
 "1_5301_INEEXTINT_96:6.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:6"
"1_5301_INEEXTINT_96:3.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:3"
"1_5301_INEEXTINT_96:4.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:4"
"1_5301_INEEXTINT_96:5.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:5"
"1_5301_INEEXTINT_96:2.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:2"
"1_5301_INEEXTINT_96:33.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:33"
"1_5301_INEEXTINT_96:7.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:7"
"1_5301_INEEXTINT_96:9.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:9"
"1_5301_INEEXTINT_96:48.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:48"
"1_5301_INEEXTINT_96:10.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:10"
"1_5301_INEEXTINT_96:11.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:11"
"1_5301_INEEXTINT_96:12.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:12"
"1_5301_INEEXTINT_96:15.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:15"
"1_5301_INEEXTINT_96:39.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:39"
"1_5301_INEEXTINT_96:13.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:13"
"1_5301_INEEXTINT_96:51.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:51"
"1_5301_INEEXTINT_96:14.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:14"
"1_5301_INEEXTINT_96:16.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:16"
"1_5301_INEEXTINT_96:17.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:17"
"1_5301_INEEXTINT_96:21.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:21"
"1_5301_INEEXTINT_96:18.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:18"
"1_5301_INEEXTINT_96:19.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:19"
"1_5301_INEEXTINT_96:20.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:20"
"1_5301_INEEXTINT_96:22.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:22"
"1_5301_INEEXTINT_96:23.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:23"
"1_5301_INEEXTINT_96:24.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:24"
"1_5301_INEEXTINT_96:25.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:25"
"1_5301_INEEXTINT_96:26.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:26"
"1_5301_INEEXTINT_96:28.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:28"
"1_5301_INEEXTINT_96:30.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:30"
"1_5301_INEEXTINT_96:29.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:29"
"1_5301_INEEXTINT_96:52.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:52"
"1_5301_INEEXTINT_96:31.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:31"
"1_5301_INEEXTINT_96:32.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:32"
"1_5301_INEEXTINT_96:53.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:53"
"1_5301_INEEXTINT_96:34.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:34"
"1_5301_INEEXTINT_96:36.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:36"
"1_5301_INEEXTINT_96:27.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:27"
"1_5301_INEEXTINT_96:37.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:37"
"1_5301_INEEXTINT_96:40.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:40"
"1_5301_INEEXTINT_96:41.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:41"
"1_5301_INEEXTINT_96:42.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:42"
"1_5301_INEEXTINT_96:43.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:43"
"1_5301_INEEXTINT_96:44.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:44"
"1_5301_INEEXTINT_96:45.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:45"
"1_5301_INEEXTINT_96:46.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:46"
"1_5301_INEEXTINT_96:47.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:47"
"1_5301_INEEXTINT_96:49.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:49"
"1_5301_INEEXTINT_96:50.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:50"
"1_5301_INEEXTINT_96:8.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:8"
"1_5301_INEEXTINT_96:35.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:35"
"1_5301_INEEXTINT_96:38.csv,https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/53001?nult=1&tv=96:38"
)



  for entry in "${files_and_urls[@]}"; do
      IFS=',' read -r nombre_fichero url <<< "$entry"
        file_csv="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/${nombre_fichero}.csv"
        file_json="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/${nombre_fichero}.json"
      curl "$url" > $file_json
      jq -r '.[] | .COD as $cod | .Nombre as $nombre | .FK_Unidad as $unidad | .FK_Escala as $escala | .Data[] | [$cod, $nombre, $unidad, $escala, .Fecha, .FK_TipoDato, .FK_Periodo, .Anyo, .Valor, .Secreto] | @csv' $file_json > $file_csv 
      rm $file_json
  done


output_file="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/1_53001_INEEXTINT.csv"

 for entry in "${files_and_urls[@]}"; do
        IFS=',' read -r nombre_fichero url <<< "$entry"
        file_csv="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_staging_tablas/${nombre_fichero}.csv"
        if [ -f "$file_csv" ]; then
                tail -n +2 "$file_csv" >> "$output_file"
                rm "$file_csv"
                echo "File $file_csv appended to $output_file"
        fi
 done

 aws s3 cp $output_file s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine/
