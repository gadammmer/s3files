echo "--------------------------------------------------------"
echo "`date` -- descargando ficheros de transparent"
aws s3 sync  s3://seetransparent-invattur/  /opt/GEAOSP_PY_POC_TURISMO/scripts/files/transparent_stagging/ --endpoint-url https://minio.seetransparent.com   --profile b2b-transparent
echo "`date` -- copiando ficheros de transparent al bucket innvatur"
aws s3 sync /opt/GEAOSP_PY_POC_TURISMO/scripts/files/transparent_stagging/ s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/transparent/
echo "--------------------------------------------------------"
