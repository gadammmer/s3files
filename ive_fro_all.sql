EXECUTE IMMEDIATE '
    BEGIN
        CREATE OR REPLACE TASK ''SIT.LOAD_FC_IVE_FRO_ALL
        SCHEDULE = ''USING CRON 15 11 10 * * Europe/Madrid''
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = ''XLARGE''
        AS BEGIN
            -- Crear tabla SIT.FC_IVE_FRO_FOREIGNER
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

            -- Crear tabla SIT.FC_IVE_FRO_HOME_COUNTRY
            CREATE OR REPLACE TABLE SIT.FC_IVE_FRO_HOME_COUNTRY AS
            SELECT 
                TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(year_id || CASE
                    WHEN UPPER(month_des) = ''ENERO'' THEN ''01''
                    WHEN UPPER(month_des) = ''FEBRERO'' THEN ''02''
                    WHEN UPPER(month_des) = ''MARZO'' THEN ''03''
                    WHEN UPPER(month_des) = ''ABRIL'' THEN ''04''
                    WHEN UPPER(month_des) = ''MAYO'' THEN ''05''
                    WHEN UPPER(month_des) = ''JUNIO'' THEN ''06''
                    WHEN UPPER(month_des) = ''JULIO'' THEN ''07''
                    WHEN UPPER(month_des) = ''AGOSTO'' THEN ''08''
                    WHEN UPPER(month_des) = ''SEPTIEMBRE'' THEN ''09''
                    WHEN UPPER(month_des) = ''OCTUBRE'' THEN ''10''
                    WHEN UPPER(month_des) = ''NOVIEMBRE'' THEN ''11''
                    WHEN UPPER(month_des) = ''DICIEMBRE'' THEN ''12''
                    ELSE ''00''
                END, ''YYYYMM'')), ''YYYYMMDD'')) AS PERIOD_KEY,
                KPI_TYPE_KEY,
                kpi_key,
                NVL(COUNTRY_KEY, -3) AS COUNTRY_KEY,
                CAST(KPI_VALUE_NUM AS FLOAT) AS KPI_VALUE_NUM,
                CURRENT_TIMESTAMP(0) AS LOAD_TIME
            FROM 
                PROCESS.AX_IVE_FRO_HOME_COUNTRY_ALL_TODAY a
            LEFT JOIN 
                SIT.DM_GLB_COUNTRY b ON UPPER(a.country_des) = UPPER(b.country_des)
            WHERE 
                KPI_VALUE_NUM != ''-'';

            -- Crear tabla SIT.FC_IVE_FRO_GATEWAY
            CREATE OR REPLACE TABLE SIT.FC_IVE_FRO_GATEWAY AS
            SELECT 
                TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(year_id || CASE
                    WHEN UPPER(month_des) = ''ENERO'' THEN ''01''
                    WHEN UPPER(month_des) = ''FEBRERO'' THEN ''02''
                    WHEN UPPER(month_des) = ''MARZO'' THEN ''03''
                    WHEN UPPER(month_des) = ''ABRIL'' THEN ''04''
                    WHEN UPPER(month_des) = ''MAYO'' THEN ''05''
                    WHEN UPPER(month_des) = ''JUNIO'' THEN ''06''
                    WHEN UPPER(month_des) = ''JULIO'' THEN ''07''
                    WHEN UPPER(month_des) = ''AGOSTO'' THEN ''08''
                    WHEN UPPER(month_des) = ''SEPTIEMBRE'' THEN ''09''
                    WHEN UPPER(month_des) = ''OCTUBRE'' THEN ''10''
                    WHEN UPPER(month_des) = ''NOVIEMBRE'' THEN ''11''
                    WHEN UPPER(month_des) = ''DICIEMBRE'' THEN ''12''
                    ELSE ''00''
                END, ''YYYYMM'')), ''YYYYMMDD'')) AS PERIOD_KEY,
                kpi_key,
                GATEWAY_TYPE_KEY,
                CAST(KPI_VALUE_NUM AS FLOAT) AS KPI_VALUE_NUM,
                CURRENT_TIMESTAMP(0) AS LOAD_TIME
            FROM 
                PROCESS.AX_IVE_FRO_GATEWAY_ALL_TODAY
            WHERE 
                KPI_VALUE_NUM != ''-'';

            -- Crear tabla SIT.FC_IVE_FRO_HOUSING
            CREATE OR REPLACE TABLE SIT.FC_IVE_FRO_HOUSING AS
            SELECT 
                TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(year_id || CASE
                    WHEN UPPER(month_des) = ''ENERO'' THEN ''01''
                    WHEN UPPER(month_des) = ''FEBRERO'' THEN ''02''
                    WHEN UPPER(month_des) = ''MARZO'' THEN ''03''
                    WHEN UPPER(month_des) = ''ABRIL'' THEN ''04''
                    WHEN UPPER(month_des) = ''MAYO'' THEN ''05''
                    WHEN UPPER(month_des) = ''JUNIO'' THEN ''06''
                    WHEN UPPER(month_des) = ''JULIO'' THEN ''07''
                    WHEN UPPER(month_des) = ''AGOSTO'' THEN ''08''
                    WHEN UPPER(month_des) = ''SEPTIEMBRE'' THEN ''09''
                    WHEN UPPER(month_des) = ''OCTUBRE'' THEN ''10''
                    WHEN UPPER(month_des) = ''NOVIEMBRE'' THEN ''11''
                    WHEN UPPER(month_des) = ''DICIEMBRE'' THEN ''12''
                    ELSE ''00''
                END, ''YYYYMM'')), ''YYYYMMDD'')) AS PERIOD_KEY,
                kpi_key,
                HOUSING_TYPE_KEY,
                CAST(KPI_VALUE_NUM AS FLOAT) AS KPI_VALUE_NUM,
                CURRENT_TIMESTAMP(0) AS LOAD_TIME
            FROM 
                PROCESS.AX_IVE_FRO_HOUSING_ALL_TODAY
            WHERE 
                KPI_VALUE_NUM != ''-'';

            -- Crear tabla SIT.FC_IVE_FRO_TRAVEL_REASON
            CREATE OR REPLACE TABLE SIT.FC_IVE_FRO_TRAVEL_REASON AS
            SELECT 
                TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(year_id || CASE
                    WHEN UPPER(month_des) = ''ENERO'' THEN ''01''
                    WHEN UPPER(month_des) = ''FEBRERO'' THEN ''02''
                    WHEN UPPER(month_des) = ''MARZO'' THEN ''03''
                    WHEN UPPER(month_des) = ''ABRIL'' THEN ''04''
                    WHEN UPPER(month_des) = ''MAYO'' THEN ''05''
                    WHEN UPPER(month_des) = ''JUNIO'' THEN ''06''
                    WHEN UPPER(month_des) = ''JULIO'' THEN ''07''
                    WHEN UPPER(month_des) = ''AGOSTO'' THEN ''08''
                    WHEN UPPER(month_des) = ''SEPTIEMBRE'' THEN ''09''
                    WHEN UPPER(month_des) = ''OCTUBRE'' THEN ''10''
                    WHEN UPPER(month_des) = ''NOVIEMBRE'' THEN ''11''
                    WHEN UPPER(month_des) = ''DICIEMBRE'' THEN ''12''
                    ELSE ''00''
                END, ''YYYYMM'')), ''YYYYMMDD'')) AS PERIOD_KEY,
                kpi_key,
                TRAVEL_REASON_KEY,
                CAST(KPI_VALUE_NUM AS FLOAT) AS KPI_VALUE_NUM,
                CURRENT_TIMESTAMP(0) AS LOAD_TIME
            FROM 
                PROCESS.AX_IVE_FRO_TRAVEL_REASON_ALL_TODAY
            WHERE 
                KPI_VALUE_NUM != ''-'';

            -- Crear tabla SIT.FC_IVE_EGA_INTER_SPEND
            CREATE OR REPLACE TABLE SIT.FC_IVE_EGA_INTER_SPEND AS
            SELECT 
                TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(year_id || CASE
                    WHEN UPPER(month_des) = ''ENERO'' THEN 1
                    WHEN UPPER(month_des) = ''FEBRERO'' THEN 2
                    WHEN UPPER(month_des) = ''MARZO'' THEN 3
                    WHEN UPPER(month_des) = ''ABRIL'' THEN 4
                    WHEN UPPER(month_des) = ''MAYO'' THEN 5
                    WHEN UPPER(month_des) = ''JUNIO'' THEN 6
                    WHEN UPPER(month_des) = ''JULIO'' THEN 7
                    WHEN UPPER(month_des) = ''AGOSTO'' THEN 8
                    WHEN UPPER(month_des) = ''SEPTIEMBRE'' THEN 9
                    WHEN UPPER(month_des) = ''OCTUBRE'' THEN 10
                    WHEN UPPER(month_des) = ''NOVIEMBRE'' THEN 11
                    WHEN UPPER(month_des) = ''DICIEMBRE'' THEN 12
                    ELSE 0
                END, ''YYYYMM'')), ''YYYYMMDD'')) AS PERIOD_KEY,
                kpi_key,
                SPEND_TYPE_KEY,
                KPI_VALUE_NUM,
                CURRENT_TIMESTAMP(0) LOAD_TIME       
            FROM 
                PROCESS.AX_IVE_EGA_INTER_SPEND_ALL_TODAY a
            WHERE 
                KPI_VALUE_NUM != ''-'';

        END;
    END;';

