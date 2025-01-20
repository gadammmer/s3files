
CREATE OR REPLACE TASK PROCESS.LOAD_AX_INE_EXP_TURISMO
    schedule='USING CRON  30 11 * * * Europe/Madrid'
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XLARGE'
	as BEGIN
            ------------------------------- AX_INE_TURISMO_INTERNACIONAL -------------------------------

            CREATE OR REPLACE TABLE PROCESS.AX_INE_TURISMO_INTERNACIONAL (
                FECHA DATE,
                VALOR FLOAT,
                FAMILIA VARCHAR(50),
                TABLA VARCHAR(10),
                LOAD_TIME DATE,
                COD VARCHAR(30),
                NOMBRE VARCHAR(300),
                COMUNIDADES_Y_CIUDADES_AUTONOMAS VARCHAR(37),
                CONCEPTO_TURISTICO VARCHAR(62),
                CONTINENTES VARCHAR(32),
                PAISES VARCHAR(120),
                PROVINCIAS VARCHAR(37),
                RESIDENCIA_ORIGEN VARCHAR(39),
                TIPO_DE_DATO VARCHAR(39),
                TOTAL_NACIONAL VARCHAR(43),
                ZONAS_GEOGRAFICAS_DEL_RESTO_DEL_MUNDO VARCHAR(24),
                LOAD_DATA_TIME TIMESTAMP_LTZ(0)
            );

            INSERT INTO PROCESS.AX_INE_TURISMO_INTERNACIONAL (
            SELECT 
                S.FECHA,
                S.VALOR,
                S.FAMILIA,
                S.TABLA,
                S.LOAD_TIME,
                S.COD,
                S.NOMBRE,
                TRIM(D.COMUNIDADES_Y_CIUDADES_AUTONOMAS),
                TRIM(D.CONCEPTO_TURISTICO),
                TRIM(D.CONTINENTES),
                TRIM(D.PAISES),
                TRIM(D.PROVINCIAS),
                TRIM(D.RESIDENCIA_ORIGEN),
                TRIM(D.TIPO_DE_DATO),
                TRIM(D.TOTAL_NACIONAL),
                TRIM(D.ZONAS_GEOGRAFICAS_DEL_RESTO_DEL_MUNDO),
                current_timestamp() LOAD_DATA_TIME
            FROM STAGING.ST_INE_ALL S
            INNER JOIN PROCESS.RL_INE_TURISMO_INTERNACIONAL D
            ON S.FAMILIA = D.FAMILIA
            AND S.COD = D.COD
            AND S.TABLA =  D.TABLA
            );

            ------------------------------- AX_INE_EXP_TOURISM_ALL_TODAY -------------------------------


            CREATE OR REPLACE TABLE PROCESS.AX_INE_EXP_TOURISM_ALL_TODAY(
                FILTER_ID VARCHAR(20),
                PERIOD_KEY NUMBER(9,0),
                CODE_DES VARCHAR(200),
                KPI_ID VARCHAR(200),
                REGION_DES VARCHAR(200),
                PROVINCE_DES VARCHAR(200),
                COUNTRY_DES VARCHAR(200),
                RESIDENCE_ID VARCHAR(200),
                KPI_VALUE_NUM FLOAT,
                TABLE_ID VARCHAR(10),
                LOAD_TIME TIMESTAMP_LTZ(0)
            );

            insert into PROCESS.AX_INE_EXP_TOURISM_ALL_TODAY 
                (
                        SELECT 
                            'INCOMING TOURISM' FILTER_ID,
                            TO_NUMBER(to_char(A.FECHA,'YYYYMMDD')) PERIOD_KEY,
                            COD CODE_DES,
                            DECODE(CONCEPTO_TURISTICO,
                                'Duración media de los viajes', 'Estancia media', 
                                'Turistas', 'Viajeros',
                                CONCEPTO_TURISTICO) KPI_ID,
                            DECODE(TRANSLATE(UPPER(COMUNIDADES_Y_CIUDADES_AUTONOMAS), 'ÁÉÍÓÚ', 'AEIOU')
                                        ,'ASTURIAS, PRINCIPADO DE','ASTURIAS'
                                        ,'CASTILLA - LA MANCHA','CASTILLA LA MANCHA'
                                        ,'BALEARS, ILLES','ISLAS BALEARES'
                                        ,'NAVARRA, COMUNIDAD FORAL DE','NAVARRA'
                                        ,'COMUNITAT VALENCIANA','VALENCIA'
                                        ,'MADRID, COMUNIDAD DE','MADRID'
                                        ,'RIOJA, LA','LA RIOJA'
                                        ,'MURCIA, REGION DE','MURCIA'
                                        ,NVL(TRANSLATE(UPPER(COMUNIDADES_Y_CIUDADES_AUTONOMAS), 'ÁÉÍÓÚ', 'AEIOU'),'**********')
                        ) AS REGION_DES,
                                TRANSLATE(UPPER(DECODE(PROVINCIAS
                                        ,'Asturias (Principado de)' ,'ASTURIAS'
                                        ,'Baleares (Illes)' ,'ISLAS BALEARES'
                                        ,'Murcia (Región de)','MURCIA'
                                        ,'Madrid (Comunidad de)','MADRID'
                                        ,'Navarra (Comunidad Foral de)','NAVARRA'
                                        ,'Rioja (La)','LA RIOJA'
                                        ,'Vizcaya','BIZKAIA'
                                        ,'Santa Cruz Tenerife','SANTA CRUZ DE TENERIFE'
                                        ,'Guipúzcoa','GIPUZKOA'
                                        ,'Alicante/Alacant','ALICANTE'
                                        ,'Araba/Álava','ALAVA'
                                        ,'Balears, Illes','ISLAS BALEARES'
                                        ,'Coruña, A','A CORUÑA'
                                        ,'Valencia/València','VALENCIA'
                                        ,'Palmas, Las','LAS PALMAS'
                                        ,'Castellón/Castelló','CASTELLON'
                                        ,'Rioja, La','LA RIOJA'
                                        ,NVL(PROVINCIAS,'**********'))),'ÁÉÍÓÚÜ()-/','AEIOUU')  PROVINCE_DES
                                ,COALESCE(PAISES, ZONAS_GEOGRAFICAS_DEL_RESTO_DEL_MUNDO, CONTINENTES) COUNTRY_DES
                                ,NVL(RESIDENCIA_ORIGEN,'**********') RESIDENCE_ID
                                ,NVL(A.VALOR,0) KPI_VALUE_NUM
                                ,TABLA
                                ,CURRENT_TIMESTAMP(0) LOAD_TIME
                                
                        FROM PROCESS.AX_INE_TURISMO_INTERNACIONAL A
                        
                        WHERE UPPER(NVL(RESIDENCIA_ORIGEN,'**********'))!='TOTAL' --Eliminamos los totales 
                        AND UPPER(NVL(TOTAL_NACIONAL,'**********'))!='TOTAL NACIONAL' --Eliminamos los totales 
                        );

            -- ------RL_COUNTRY_TRANSLATOR----------------------------------------------------------------------------------------------------------------------------------
     
            INSERT INTO RL_COUNTRY_TRANSLATOR (

            WITH AX_TABLE AS (
                SELECT 
                TRANSLATE(UPPER(COUNTRY_DES),'ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’', 'AEIOUAEIOUAEIOUO') TRA_COUNTRY_DES_JOIN,
                COUNTRY_DES 
                FROM AX_INE_EXP_TOURISM_ALL_TODAY WHERE COUNTRY_DES IS NOT NULL),
                
            DM_COUNTRY AS (
                SELECT DISTINCT 
                    TRANSLATE(UPPER(A.COUNTRY_DES),'ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’', 'AEIOUAEIOUAEIOUO') COUNTRY_DES_JOIN,
                    A.COUNTRY_DES,
                    COUNTRY_ID 
                FROM SIT.DM_GLB_COUNTRY A)
                
            SELECT 
                A.COUNTRY_DES RAW_COUNTRY_DES,
                'AX_INE_EXP_TOURISM_ALL_TODAY' ORIGIN_DATA_ID,
                NVL(MAX(B.COUNTRY_DES),'*********') COUNTRY_DES,
                NVL(MAX(B.COUNTRY_ID),'***') COUNTRY_ID
                ,CURRENT_TIMESTAMP() LOAD_TIME 
            FROM AX_TABLE A
            LEFT JOIN DM_COUNTRY B
            ON A.TRA_COUNTRY_DES_JOIN=B.COUNTRY_DES_JOIN
            
            LEFT JOIN RL_COUNTRY_TRANSLATOR TRA
            ON A.COUNTRY_DES=TRA.RAW_COUNTRY_DES
            
            WHERE TRA.RAW_COUNTRY_DES IS NULL
            GROUP BY A.COUNTRY_DES

            
            );

            ------------------------------- AX_INE_EXP_TOURISM -------------------------------

            CREATE OR REPLACE TABLE PROCESS.AX_INE_EXP_TOURISM (
                FILTER_ID VARCHAR(20),
                PERIOD_KEY NUMBER(9,0),
                RESIDENCE_KEY NUMBER(16,0),
                REGION_KEY NUMBER(16,0),
                PROVINCE_KEY NUMBER(16,0),
                COUNTRY_KEY NUMBER(16,0),
                KPI_KEY NUMBER(16,0),
                KPI_VALUE_NUM FLOAT,
                LOAD_TIME DATETIME
            );


            insert into PROCESS.AX_INE_EXP_TOURISM 
                (
                select 
                    FILTER_ID,
                    PERIOD_KEY,
                    NVL(RO.ID,-3) RESIDENCE_KEY ,
                    NVL(R.REGION_KEY, -3) REGION_KEY,
                    NVL(P.PROVINCE_KEY,-3) PROVINCE_KEY,
                    NVL(CY.COUNTRY_KEY, -3) COUNTRY_KEY,
                    NVL(B.KPI_KEY,-3) KPI_KEY,
                    KPI_VALUE_NUM,
                    CURRENT_TIMESTAMP(0) LOAD_TIME
                    
                    FROM PROCESS.AX_INE_EXP_TOURISM_ALL_TODAY A
                    LEFT JOIN SIT.DM_IVE_KPI_MASTER B
                    ON A.KPI_ID=B.KPI_ID

                    LEFT JOIN SIT.DM_INE_RESIDENCIA_ORIGEN RO
                    ON A.RESIDENCE_ID=RO.NOMBRE

                    LEFT JOIN SIT.DM_GLB_REGION R
                    ON A.REGION_DES = R.REGION_NORM_DES

                    LEFT JOIN PROCESS.RL_COUNTRY_TRANSLATOR TRA
                    ON A.COUNTRY_DES = TRA.RAW_COUNTRY_DES

                    LEFT JOIN SIT.DM_GLB_COUNTRY CY
                    ON TRA.COUNTRY_ID=CY.COUNTRY_ID

                    LEFT JOIN SIT.DM_GLB_PROVINCE P
                    ON A.PROVINCE_DES = P.PROVINCE_NORM_DES
                );

    END;