~/bin/snowsql -a osp-b2b -u ETL_PRE --private-key-path /home/ec2-user/RSA_KEY/rsa_key.p8 -c preprocess -f //home/ec2-user/git/s3files/fix_week/FC_FLX_PLACE_BY_CITY_WEEK.sql -o log_file=/home/ec2-user/SQL/logs/snowsql_log.log



Executing file -> /home/ec2-user/SQL/files/pre/FLUX/TASK_LOAD_AX_FLX_PLACE_V1_PAP2.sql

* SnowSQL * v1.3.1
Type SQL statements or !help
001003 (42000): SQL compilation error:                                          
syntax error line 20 at position 21 unexpected 'LugarActividad_DiaVisita'.
syntax error line 421 at position 48 unexpected 'YYYYMMDD'.
syntax error line 421 at position 56 unexpected ''''.
syntax error line 421 at position 61 unexpected 'period_key'.
parse error line 1,250 at position 2 near '<EOF>'.

Goodbye!                                                                        
Executing file -> /home/ec2-user/SQL/files/pre/FLUX/TASK_LOAD_AX_FLX_PLACE_V2_PAP2.sql

* SnowSQL * v1.3.1
Type SQL statements or !help
001003 (42000): SQL compilation error:                                          
syntax error line 19 at position 17 unexpected 'LugarActividad_DiaVisita'.
syntax error line 400 at position 42 unexpected 'YYYYMMDD'.
syntax error line 400 at position 50 unexpected ''''.
syntax error line 400 at position 55 unexpected 'period_key'.

Goodbye!                                                                        
Executing file -> /home/ec2-user/SQL/files/pre/FLUX/TASK_LOAD_AX_FLX_REC_PAP2.sql

* SnowSQL * v1.3.1
Type SQL statements or !help
001003 (42000): SQL compilation error:                                          
syntax error line 19 at position 17 unexpected 'LugarActividad_DiaVisita'.
syntax error line 399 at position 52 unexpected 'YYYYMMDD'.
syntax error line 399 at position 60 unexpected ''''.
syntax error line 399 at position 65 unexpected 'period_key'.
