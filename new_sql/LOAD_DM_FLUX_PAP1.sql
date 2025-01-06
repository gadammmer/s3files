 
 --- FLUX
EXECUTE IMMEDIATE '
    BEGIN

        CREATE or replace TABLE SIT.DM_FLX_VISITOR_ORIGIN_MLG(
            VISITOR_ORIGIN_KEY NUMBER(16),
            LANGUAGE_KEY NUMBER(16),
            VISITOR_ORIGIN_DES VARCHAR(100),
            LOAD_TIME TIMESTAMP_LTZ(9)
        );


        copy into SIT.DM_FLX_VISITOR_ORIGIN_MLG (VISITOR_ORIGIN_KEY,LANGUAGE_KEY,VISITOR_ORIGIN_DES,LOAD_TIME) 
        FROM (select $1,$2,$3,to_timestamp_ntz(current_timestamp)  from ''@s3_stage/flux_dt/DM_FLX_VISITOR_ORIGIN_MLG_V3.csv'') file_format = ''csv_format_dm'' force= true on_error=''continue''; 


        CREATE or replace TABLE SIT.DM_FLX_NIGHTS_STAY_MLG (
            NIGHTS_STAY_KEY NUMBER(16),
            LANGUAGE_KEY NUMBER(16),
            NIGHTS_STAY_DES VARCHAR(100),
            LOAD_TIME TIMESTAMP_LTZ(9)
        );

        copy into SIT.DM_FLX_NIGHTS_STAY_MLG (NIGHTS_STAY_KEY,LANGUAGE_KEY,NIGHTS_STAY_DES,LOAD_TIME) 
        FROM (select $1,$2,$3,to_timestamp_ntz(current_timestamp)  from ''@s3_stage/flux_dt/DM_FLX_NIGHTS_STAY_MLG_V1.csv'') file_format = ''csv_format_dm'' force= true on_error=''continue''; 



        CREATE or replace TABLE SIT.DM_FLX_HOURS_STAY_MLG (
            HOURS_STAY_KEY NUMBER(16),
            LANGUAGE_KEY NUMBER(16),
            HOURS_STAY_DES VARCHAR(100),
            LOAD_TIME TIMESTAMP_LTZ(9)
        );

        copy into SIT.DM_FLX_HOURS_STAY_MLG (HOURS_STAY_KEY,LANGUAGE_KEY,HOURS_STAY_DES,LOAD_TIME) 
        FROM (select $1,$2,$3,to_timestamp_ntz(current_timestamp)  from ''@s3_stage/flux_dt/DM_FLX_HOURS_STAY_MLG_V1.csv'') file_format = ''csv_format_dm'' force= true on_error=''continue''; 

        RETURN ''Update SIT.DM_FLX_VISITOR_ORIGIN_MLG | SIT.DM_FLX_NIGHTS_STAY_MLG | SIT.DM_FLX_HOURS_STAY_MLG'';

    END;
';

DELETE FROM SIT.DM_FLX_ZONE_MLG WHERE ZONE_KEY IS NULL
  AND LANGUAGE_KEY IS NULL
  AND ZONE_DES IS NULL
  AND LOAD_TIME IS NOT NULL;
