EXECUTE IMMEDIATE '
    BEGIN
        CREATE OR REPLACE TASK SIT.LOAD_FC_IVE_FRO_ALL_NO_COMPLETO
        SCHEDULE = ''USING CRON 15 11 10 * * Europe/Madrid''
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = ''XLARGE''
        AS BEGIN
            CREATE OR REPLACE TABLE SIT.FC_IVE_FRO_FOREIGNER AS
            SELECT 
                period_key,
                KPI_TYPE_KEY,
                kpi_key,
                NVL(b.province_key, -3) AS province_key,
                NVL(c.city_key, -3) AS city_key,
                KPI_VALUE_NUM,
                CURRENT_TIMESTAMP(0) AS LOAD_TIME       
            FROM 
                PROCESS.AX_IVE_FRO_FOREIGNER_ALL_TODAY a
            LEFT JOIN SIT.DM_GLB_PROVINCE b 
                ON a.province_des = TRIM(SUBSTR(b.PROVINCE_DES, 1, REGEXP_INSTR(b.PROVINCE_DES, ''/'') - 1))
            LEFT JOIN SIT.DM_GLB_CITY c 
                ON a.city_DES = c.city_DES;

    END;';
