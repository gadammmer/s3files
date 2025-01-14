~/bin/snowsql -a osp-b2b -u ETL_PRO--private-key-path /home/ec2-user/RSA_KEY/rsa_key.p8 -c prosit -f ~/SQL/create_invattur_pap2.sh pro -o log_file=/home/ec2-user/SQL/logs/snowsql_log.log



~/bin/snowsql -a osp-b2b -u ETL_DEV --private-key-path /home/ec2-user/RSA_KEY/rsa_key.p8 -c desit -f ~/SQL/files/dev/FLUX/LOAD_DM_FLUX_PAP2.sql -o log_file=/home/ec2-user/SQL/logs/snowsql_log.log


PTLINT-102
PTLINT-104
PTLINT-116
PTLINT-138
PTLINT-91
PTLINT-70
PTLINT-60
PTLINT-75
PTLINT-67



/home/ec2-user/SQL/files/pro/FLUX/LOAD_FC_FLX_PLACE_HIST_PAP2.sql
/home/ec2-user/SQL/files/pro/FLUX/LOAD_FC_FLX_AGE_HIST_PAP2.sql


LOAD_FC_FLX_PLACE_PART2_HIST_PAP2.sql
