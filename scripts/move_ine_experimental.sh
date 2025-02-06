curl "https://www.ine.es/experimental/turismo_moviles/exp_tmov_receptor_mun_2024.xlsx" > /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/exp_tmov_receptor_mun_2024.xlsx
curl "https://www.ine.es/experimental/turismo_moviles/exp_tmov_receptor_mun_2025.xlsx" > /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/exp_tmov_receptor_mun_2025.xlsx
curl "https://www.ine.es/experimental/turismo_moviles/exp_tmov_interno_mun_2024.xlsx" > /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/exp_tmov_interno_num_2024.xlsx
curl "https://www.ine.es/experimental/turismo_moviles/exp_tmov_interno_mun_2025.xlsx" > /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/exp_tmov_interno_num_2025.xlsx
curl "https://www.ine.es/experimental/viv_turistica/exp_viv_turistica_tabla5_$(date --date='-1 months' +%^b%Y).xlsx?nocab=1" >/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_viviendas/exp_viv_turistica_tabla5_$(date --date='-1 months' +%^b%Y).xlsx
curl "https://www.seg-social.es/descargas/STAT/MUNCNAE$(date --date='-1 months' +%m%y).xlsx" >/opt/GEAOSP_PY_POC_TURISMO/scripts/files/ss/MUNCNAE$(date --date='-1 months' +%m%y).xlsx
 
/home/ec2-user/.local/bin/xlsx2csv -a /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/exp_tmov_receptor_mun_2024.xlsx /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/internacional/exp_tmov_receptor_mun_2024.csv
/home/ec2-user/.local/bin/xlsx2csv -a /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/exp_tmov_receptor_mun_2025.xlsx /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/internacional/exp_tmov_receptor_mun_2025.csv
/home/ec2-user/.local/bin/xlsx2csv -a /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/exp_tmov_interno_num_2024.xlsx /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/nacional/exp_tmov_interno_mun_2024.csv
/home/ec2-user/.local/bin/xlsx2csv -a /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/exp_tmov_interno_num_2025.xlsx /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/nacional/exp_tmov_interno_mun_2025.csv
/home/ec2-user/.local/bin/xlsx2csv -a /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_viviendas/exp_viv_turistica_tabla5_$(date --date='-1 months' +%^b%Y).xlsx /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_viviendas/exp_viv_turistica_tabla5_$(date --date='-1 months' +%^b%Y).csv
/home/ec2-user/.local/bin/xlsx2csv -a /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ss/MUNCNAE$(date --date='-1 months' +%m%y).xlsx /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ss/MUNCNAE$(date --date='-1 months' +%m%y).csv 

aws s3 sync  /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/internacional/exp_tmov_receptor_mun_2024.csv/   s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine_experimental/internacional/
aws s3 sync  /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/internacional/exp_tmov_receptor_mun_2025.csv/   s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine_experimental/internacional/
aws s3 sync  /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/nacional/exp_tmov_interno_mun_2024.csv/   s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine_experimental/nacional/
aws s3 sync  /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_turismo_moviles/nacional/exp_tmov_interno_mun_2025.csv/   s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine_experimental/nacional/
aws s3 sync  /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ine_viviendas/exp_viv_turistica_tabla5_$(date --date='-1 months' +%^b%Y).csv/ s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ine_experimental/viviendas/$(date --date='-1 months' +%^b%Y)/
aws s3 sync  /opt/GEAOSP_PY_POC_TURISMO/scripts/files/ss/MUNCNAE$(date --date='-1 months' +%m%y).csv/  s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/ss/  

