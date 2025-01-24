EXECUTE IMMEDIATE '
BEGIN
create or replace task SIT.LOAD_FC_INE_OCCUPSURVEY_OFFER
	schedule=''USING CRON 00 12 * * * Europe/Madrid''
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
	as BEGIN
                  create or replace TABLE SIT.FC_INE_OCCUPSURVEY (
                        PERIOD_KEY NUMBER(9,0),
                        ACCOMMODATION_TYPE_KEY NUMBER(16,0),
                        RESIDENCE_KEY NUMBER(16,0),
                        REGION_KEY NUMBER(16,0),
                        PROVINCE_KEY NUMBER(16,0),
                        CITY_KEY NUMBER(16,0),
                        TOURISTIC_ZONE_ID NUMBER(16,0),
                        KPI_KEY	NUMBER(16,0),
                        KPI_VALUE_NUM FLOAT,
                        LOAD_TIME TIMESTAMP_LTZ(0)
                    );

                    INSERT INTO SIT.FC_INE_OCCUPSURVEY  
                        (
                            SELECT PERIOD_KEY,   
                                ACCOMMODATION_TYPE_KEY,
                                RESIDENCE_KEY,
                                NVL(r.Region_KEY, -3) Region_KEY,
                                province_key,
                                CITY_KEY,
                                TOURISTIC_ZONE_ID,
                                KPI_KEY,
                                KPI_VALUE_NUM,
                                CURRENT_TIMESTAMP(0) LOAD_TIME
                            FROM
                                PROCESS.AX_INE_OCCUPSURVEY a
                                LEFT JOIN SIT.DM_GLB_REGION r
                                ON a.region_des = R.REGION_NORM_DES
                            WHERE
                                A.FILTER_ID = ''OCCUPSURVEY''
                        );

                  create or replace TABLE SIT.FC_INE_OFFER (
                    PERIOD_KEY NUMBER(9,0),
                    ACCOMMODATION_TYPE_KEY NUMBER(16,0),
                    REGION_KEY NUMBER(16,0),
                    PROVINCE_KEY NUMBER(16,0),
                    CITY_KEY NUMBER(16,0),
                    TOURISTIC_ZONE_ID NUMBER(16,0),
                    KPI_KEY	NUMBER(16,0),
                    KPI_VALUE_NUM FLOAT,
                    LOAD_TIME TIMESTAMP_LTZ(0)
                );  

                INSERT INTO SIT.FC_INE_OFFER 
                    (
                        SELECT PERIOD_KEY,   
                            ACCOMMODATION_TYPE_KEY,
                            NVL(r.Region_KEY, -3) Region_KEY,
                            province_key,
                            CITY_KEY,
                            TOURISTIC_ZONE_ID,
                            KPI_KEY,
                            KPI_VALUE_NUM,
                            CURRENT_TIMESTAMP(0) LOAD_TIME
                        FROM
                            PROCESS.AX_INE_OCCUPSURVEY a
                            LEFT JOIN SIT.DM_GLB_REGION r
                            ON a.region_des = R.REGION_NORM_DES
                        WHERE
                        A.FILTER_ID = ''OFFER''
                );
              END;
    END;';
    