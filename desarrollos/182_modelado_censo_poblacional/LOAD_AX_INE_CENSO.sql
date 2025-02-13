EXECUTE IMMEDIATE '
    BEGIN

    CREATE OR REPLACE TASK PROCESS.LOAD_AX_INE_CENSO
        schedule=''USING CRON  30 11 * * * Europe/Madrid''
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
        as BEGIN
            CREATE or REPLACE TABLE PROCESS.AX_INE_CENSO_PRE_ALL_TODAY
            (
                PERIOD_KEY	      NUMBER(10,0)
                ,FAMILIA          VARCHAR(20)
                ,COD              VARCHAR(50)
                ,TABLA            NUMBER(8,0)
                ,NOMBRE           VARCHAR(200)
                ,KPI_ID           VARCHAR(50)    
                ,DUPLICATE_FILTER NUMBER(6,0)
                ,KPI_VALUE_NUM    FLOAT
                ,LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
            ) ;


            INSERT INTO PROCESS.AX_INE_CENSO_PRE_ALL_TODAY

            SELECT
                TO_NUMBER(TO_CHAR(FECHA,''YYYYMMDD'')) PERIOD_KEY,
                FAMILIA,
                COD,
                TABLA,
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(NOMBRE),
                    ''Usansolo. Dato base. Hombres. Todas las edades. Total.'',''Usansolo. Hombre. Total. Todas las edades. Dato base.''),
                    ''Usansolo. Dato base. Mujeres. Todas las edades. Total.'',''Usansolo. Mujer. Total. Todas las edades. Dato base.''),
                    ''Usansolo. Dato base. Total. Todas las edades. Total.'',''Usansolo. Total. Total. Todas las edades. Dato base.''),
                    ''Hombres'',''Hombre''),
                    ''Mujeres'',''Mujer'') NOMBRE,
                ''Censo anual'' AS KPI_ID,
                ROW_NUMBER() OVER (PARTITION BY NOMBRE, FECHA, TABLA ORDER BY VALOR DESC) AS DUPLICATE_FILTER,
                VALOR,
                current_timestamp() LOAD_TIME
            FROM STAGING.ST_INE_ALL 
            WHERE TABLA =''68065'' ;


            --------------------------------------------
            -- ------AX_INE_CENSO_ALL_TODAY------------
            --------------------------------------------

            CREATE or REPLACE TABLE PROCESS.AX_INE_CENSO_ALL_TODAY
            (
                PERIOD_KEY	      NUMBER(10,0)
                ,FILTER_ID        VARCHAR(20)
                ,KPI_ID           VARCHAR(50)  
                ,SCOPE_DES        VARCHAR(50)     
                ,CITY_DES         VARCHAR(50)
                ,GENDER_DES       VARCHAR(50)     
                ,AGE_DES          VARCHAR(50)    
                ,KPI_VALUE_NUM    FLOAT
                ,LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
            ) ;


            INSERT INTO PROCESS.AX_INE_CENSO_ALL_TODAY

            SELECT
                PERIOD_KEY,
                FAMILIA FILTER_ID,
                KPI_ID,  
                TRIM(REPLACE(SPLIT(NOMBRE, ''.'')[2],''"'','''')) AS SCOPE_DES,    
                TRANSLATE(UPPER(REPLACE(SPLIT(NOMBRE, ''.'')[0],''"'','''')),''ÁÀÄÉÈËÍÌÏÓÒÖÚÙÜ-/ ,'',''AAAEEEIIIOOOUUU'') AS CITY_DES,
                TRIM(REPLACE(SPLIT(NOMBRE, ''.'')[1],''"'','''')) AS GENDER_DES, 
                TRIM(REPLACE(SPLIT(NOMBRE, ''.'')[3],''"'','''')) AS AGE_DES,
                KPI_VALUE_NUM,
                current_timestamp() LOAD_TIME
            FROM PROCESS.AX_INE_CENSO_PRE_ALL_TODAY
            WHERE DUPLICATE_FILTER = 1;

            --------------------------------------------
            -- --------------AX_INE_CENSO---------------
            --------------------------------------------


            CREATE or REPLACE TABLE PROCESS.AX_INE_CENSO
            (
                PERIOD_KEY	      NUMBER(8,0)
                ,FILTER_ID        VARCHAR(20)
                ,CITY_KEY         NUMBER(6,0)
                ,GENDER_KEY       NUMBER(6,0)
                ,AGE_KEY          NUMBER(6,0)
                ,KPI_KEY          NUMBER(6,0)
                ,KPI_VALUE_NUM    FLOAT
                ,LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
            ) ;

            INSERT INTO PROCESS.AX_INE_CENSO

            WITH CITY AS (SELECT REPLACE(CITY_NORM_DES, '' '','''') CITY_NORM_DES, MIN(CITY_KEY) CITY_KEY 
                        FROM SIT.DM_GLB_CITY 
                        GROUP BY REPLACE(CITY_NORM_DES, '' '','''') 
                        )

            SELECT
                PERIOD_KEY,
                FILTER_ID,
                NVL(CY.CITY_KEY, -3),
                NVL(G.GENDER_KEY, -3),
                NVL(A.AGE_KEY, -3),
                NVL(KPI.KPI_KEY, -3),
                KPI_VALUE_NUM,
                current_timestamp() LOAD_TIME
            FROM PROCESS.AX_INE_CENSO_ALL_TODAY CENSO

            ------------------------MUNICIPIO
            LEFT JOIN CITY CY
                ON REPLACE(CENSO.CITY_DES, char(9),'''') = CY.CITY_NORM_DES
                
            ------------------------GÉNERO
            LEFT JOIN SIT.DM_ATR_GENDER G
                ON CENSO.GENDER_DES = G.GENDER_DES

            ------------------------EDAD
            LEFT JOIN SIT.DM_ATR_AGE A
                ON CENSO.AGE_DES = A.AGE_DES

            ------------------------KPI
            LEFT JOIN SIT.DM_IVE_KPI_MASTER KPI
                ON CENSO.KPI_ID = KPI.KPI_ID
                
            WHERE UPPER(CENSO.GENDER_DES) <> ''TOTAL'' AND CENSO.CITY_DES <> ''TOTALNACIONAL'';

    END;
END;';