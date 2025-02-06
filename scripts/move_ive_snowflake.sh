#!/bin/bash

archivo_urls="/opt/GEAOSP_PY_POC_TURISMO/scripts/IDsIVE.txt"

while IFS="," read -r ID url; do
        #fecha=date +"%d-%m-%y"
	#echo $ID
	#echo $url
	file_csv="$ID.csv"
	curl "$url"  > $file_csv
	aws s3 cp $file_csv s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ive/
        rm $file_csv
done < "$archivo_urls"


