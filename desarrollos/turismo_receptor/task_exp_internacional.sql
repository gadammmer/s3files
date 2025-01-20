CREATE OR REPLACE TASK PROCESS.LOAD_AX_INE_EXP_INTERNATIONAL
    schedule='USING CRON  30 11 * * * Europe/Madrid'
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XLARGE'
	as BEGIN

            ------------------------------- AX_INE_EXP_INTERNATIONAL_ALL_TODAY -------------------------------

            CREATE OR REPLACE TABLE PROCESS.AX_INE_EXP_INTERNATIONAL_ALL_TODAY (
                YEAR_MONTH_KEY	     NUMBER(10,0),
                COUNTRY_ORIGIN_ID   VARCHAR(10),
                COUNTRY_ORIGIN_DES  VARCHAR(50),
                CITY_DEST_ID	     VARCHAR(10),
                CITY_DEST_DES       VARCHAR(50),
                PROVINCE_DEST_ID    VARCHAR(10),
                PROVINCE_DEST_DES   VARCHAR(50),
                TOURIST_NUM         NUMBER(10,0),
                DUPLICATE_FILTER	 NUMBER(5,0),
                LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
            );

            INSERT INTO PROCESS.AX_INE_EXP_INTERNATIONAL_ALL_TODAY
            SELECT 
                REPLACE(MES,'-','') AS MES,
                DECODE(PAIS_ORIG_COD, '040', '499', PAIS_ORIG_COD),
                PAIS_ORIG,
                MUN_DEST_COD,
                MUN_DEST,
                PROV_DEST_COD,
                PROV_DEST,
                TURISTAS,
                ROW_NUMBER() OVER (PARTITION BY MES, PAIS_ORIG, MUN_DEST_COD, PROV_DEST_COD ORDER BY ST.LOAD_DATE DESC) AS DUPLICATE_FILTER,
                current_timestamp() LOAD_TIME
            FROM STAGING.ST_INE_EXP_INTERNACIONAL ST
            INNER JOIN (SELECT MAX(LOAD_DATE) LOAD_DATE FROM STAGING.ST_INE_EXP_INTERNACIONAL) LD
            ON ST.LOAD_DATE = LD.LOAD_DATE ;


            ------------------------------- AX_INE_EXP_INTERNATIONAL -------------------------------

            CREATE OR REPLACE TABLE PROCESS.AX_INE_EXP_INTERNATIONAL (
                YEAR_MONTH_KEY NUMBER(10,0),
                COUNTRY_ORIGIN_KEY NUMBER(5,0),
                CITY_DEST_KEY NUMBER(5,0),
                TOURIST_NUM NUMBER(10,0),
                LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
            );

            INSERT INTO PROCESS.AX_INE_EXP_INTERNATIONAL(
            WITH FC_TABLE AS(
                SELECT 
                    FC.*, CONCAT(FC.YEAR_MONTH_KEY, C.ID_EXPERIMENTAL, CY.CITY_ID) AS INNER_KEY
                FROM SIT.FC_INE_EXP_INTERNATIONAL FC
                LEFT JOIN SIT.DM_GLB_COUNTRY C
                    ON FC.COUNTRY_ORIGIN_KEY = C.COUNTRY_KEY     
                LEFT JOIN SIT.DM_GLB_CITY CY
                    ON FC.CITY_DEST_KEY = CY.CITY_KEY
            )
            SELECT
                YEAR_MONTH_KEY,
                NVL(C.COUNTRY_KEY,-3) AS COUNTRY_ORIGIN_KEY ,
                NVL(CY.CITY_KEY,-3) AS CITY_DEST_KEY,
                TOURIST_NUM,
                current_timestamp() LOAD_TIME
            FROM PROCESS.AX_INE_EXP_INTERNATIONAL_ALL_TODAY EXPER
            ------------------------PAÍS
            LEFT JOIN SIT.DM_GLB_COUNTRY C
                ON EXPER.COUNTRY_ORIGIN_ID = C.ID_EXPERIMENTAL
            
            ------------------------MUNICIPIO      
            LEFT JOIN SIT.DM_GLB_CITY CY
                ON EXPER.CITY_DEST_ID = CY.CITY_ID
            WHERE DUPLICATE_FILTER = 1    
            UNION ALL
            SELECT   
                FC.YEAR_MONTH_KEY,
                FC.COUNTRY_ORIGIN_KEY ,
                FC.CITY_DEST_KEY,
                FC.TOURIST_NUM,
                FC.LOAD_TIME
            FROM FC_TABLE FC
            LEFT JOIN PROCESS.AX_INE_EXP_INTERNATIONAL_ALL_TODAY AX
                ON FC.INNER_KEY = CONCAT(AX.YEAR_MONTH_KEY, AX.COUNTRY_ORIGIN_ID, AX.CITY_DEST_ID)
            WHERE CONCAT(AX.YEAR_MONTH_KEY, AX.COUNTRY_ORIGIN_ID, AX.CITY_DEST_ID) IS NULL);
    
    END;