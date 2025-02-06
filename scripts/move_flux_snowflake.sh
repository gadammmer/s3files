#!/bin/bash

curl -k -u "aprieto@opensistemas.com:Anton\$12"  "https://download.flux-vision.orange-business.com/api_file/v1/INVA/" -o "/opt/GEAOSP_PY_POC_TURISMO/scripts/FLUX_IDS.json"
jq -r 'map([.file_last_modified_date, .delivery_code_ref, .orga_name, .orga_code_ref, .file_name, .url, .study_name, .study_code_ref, .delivery_name] | @csv) | join("\n")' /opt/GEAOSP_PY_POC_TURISMO/scripts/FLUX_IDS.json > /opt/GEAOSP_PY_POC_TURISMO/scripts/FLUX_IDS.csv
echo "file_last_modified_date,delivery_code_ref,orga_name,orga_code_ref,file_name,url,study_name,study_code_ref,delivery_name" | cat - /opt/GEAOSP_PY_POC_TURISMO/scripts/FLUX_IDS.csv > tmpfile && mv tmpfile /opt/GEAOSP_PY_POC_TURISMO/scripts/FLUX_IDS.csv

archivo_urls="/opt/GEAOSP_PY_POC_TURISMO/scripts/FLUX_IDS.csv"
yesterday=$(date -d "today 10:00:00" +%Y-%m-%dT%H:%M:%S.%N%z)
#yesterday=$(date -d "yesterday 00:00:00" +%Y-%m-%dT%H:%M:%S.%N%z)
#hoy=$(date -d "yesterday 23:59:59" +%Y-%m-%dT%H:%M:%S.%N%z)
fini=$1
ffin=$2

while IFS="," read -r file_last_modified_date delivery_code_ref orga_name orga_code_ref file_name url study_name study_code_ref delivery_name; do
        file_name=$(echo "$file_name" | tr -d '"')
        delivery_code_ref=$(echo "$delivery_code_ref" | tr -d '"')
        url=$(echo "$url" | sed 's/"//g')
        file="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/flux_staging/${file_name}"
	if [[ "$file_last_modified_date" > "$fini" && "$file_last_modified_date" < "$ffin" && "$delivery_code_ref" == "INVA-1080-6430"  ]]; then
          echo "${file_name} ${file_last_modified_date}"
          curl -k -u "aprieto@opensistemas.com:Anton\$12" "${url}" -o  "${file}"
	  dir_name="$file_name"
	  mkdir  "/opt/GEAOSP_PY_POC_TURISMO/scripts/files/flux_staging/staging_csv/${dir_name}"
          unzip -n -d "/opt/GEAOSP_PY_POC_TURISMO/scripts/files/flux_staging/staging_csv/${dir_name}"  ${file}
	  aws s3 sync /opt/GEAOSP_PY_POC_TURISMO/scripts/files/flux_staging/staging_csv/${dir_name} s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/flux/continua/${dir_name}/
	  rm  ${file}
        fi
done < "$archivo_urls"

