#!/bin/bash
# uso:    "$(date -d yesterday +'%Y-%m-%dT00:00:00.%N%z')" "$(date -d yesterday +'%Y-%m-%dT23:59:59.%N%z')"

fini=$1
ffin=$2
archivo_urls="FLUX2_IDS.csv"
dir="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/"
files_continua="${dir}flux_staging/continua/"
files_nocontinua="${dir}flux_staging/nocontinua/"
echo "recuperando ficheros desde $fini a $ffin"

curl -k -u "aprieto@opensistemas.com:Anton\$12"  "https://download.flux-vision.orange-business.com/api_file/v1/INVA/" -o "FLUX2_IDS.json"
jq -r 'map([.file_last_modified_date, .delivery_code_ref, .orga_name, .orga_code_ref, .file_name, .url, .study_name, .study_code_ref, .delivery_name] | @csv) | join("\n")' FLUX2_IDS.json > $archivo_urls
echo "file_last_modified_date,delivery_code_ref,orga_name,orga_code_ref,file_name,url,study_name,study_code_ref,delivery_name" | cat - $archivo_urls > tmpfile && mv tmpfile $archivo_urls


# distinto a continua
grep -v "INVA-1080-6430" "$archivo_urls" | while IFS="," read -r file_last_modified_date delivery_code_ref orga_name orga_code_ref file_name url study_name study_code_ref delivery_name; do
    if [[ "$file_last_modified_date" > "$fini" && "$file_last_modified_date" < "$ffin" ]]; then
        file_name=$(echo "$file_name" | tr -d '"')
        file_name_clean="${file_name// /_}" # Reemplazar espacios por guiones bajos
        file="${files_nocontinua}${file_name_clean//\"/}"
        url="${url//\"/}"
        url_encoded=$(echo "$url" | sed 's/ /%20/g')
        echo "descarga fichero no continua: ${file_last_modified_date} ${file_name} ${url_encoded}"
        curl -s -k -u "aprieto@opensistemas.com:Anton\$12" "${url_encoded}" -o "${file}"
	delivery_code_ref=$(echo "$delivery_code_ref" | tr -d '"')
	if [ ! -d "${files_nocontinua}${delivery_code_ref}/${file_name_clean}" ]; then
	   mkdir "${files_nocontinua}${delivery_code_ref}"
	   mkdir "${files_nocontinua}${delivery_code_ref}/${file_name_clean}"
        fi
        echo "unzip fichero ${files_nocontinua}${delivery_code_ref}/${file_name_clean}"
        unzip -q -o "$file" -d "${files_nocontinua}${delivery_code_ref}/${file_name_clean}" || echo "Error al descomprimir el archivo ${file}"
        rm "$file"
    fi
done

echo ${files_nocontinua} a S3
aws s3 sync ${files_nocontinua} s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/flux/nocontinuo/  



