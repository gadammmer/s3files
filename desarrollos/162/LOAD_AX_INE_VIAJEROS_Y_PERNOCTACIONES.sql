EXECUTE IMMEDIATE '
BEGIN

create or replace task PROCESS.LOAD_AX_INE_VIAJEROS_Y_PERNOCTACIONES
	schedule=''USING CRON  30 11 * * * Europe/Madrid''
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
	as BEGIN

              create or replace TABLE PROCESS.AX_INE_VIAJEROS_Y_PERNOCTACIONES (
                FECHA DATE,
                VALOR FLOAT,
                FAMILIA VARCHAR(50),
                TABLA VARCHAR(10),
                LOAD_TIME DATE,
                COD VARCHAR(30),
                NOMBRE VARCHAR(300),
                COMUNIDADES_Y_CIUDADES_AUTONOMAS VARCHAR(37),
                CONCEPTO_TURISTICO VARCHAR(62),
                PROVINCIAS VARCHAR(32),
                PUNTOS_TURISTICOS VARCHAR(37),
                RESIDENCIA_ORIGEN VARCHAR(37),
                TIPO_DE_CATEGORIA VARCHAR(39),
                TIPO_DE_DATO VARCHAR(39),
                TIPO_DE_ESTABLECIMIENTO VARCHAR(43),
                TOTALES_TERRITORIALES VARCHAR(24),
                TOTAL_NACIONAL VARCHAR(24),
                ZONAS_TURISTICAS VARCHAR(77),
                LOAD_DATA_TIME TIMESTAMP_LTZ(0)
            );

              INSERT INTO PROCESS.AX_INE_VIAJEROS_Y_PERNOCTACIONES (
                SELECT 
                    S.FECHA,
                    S.VALOR,
                    S.FAMILIA,
                    S.TABLA,
                    S.LOAD_TIME,
                    S.COD,
                    S.NOMBRE,
                    D.COMUNIDADES_Y_CIUDADES_AUTONOMAS,
                    D.CONCEPTO_TURISTICO,
                    D.PROVINCIAS,
                    D.PUNTOS_TURISTICOS,
                    D.RESIDENCIA_ORIGEN,
                    D.TIPO_DE_CATEGORIA,
                    D.TIPO_DE_DATO,
                    D.TIPO_DE_ESTABLECIMIENTO,
                    D.TOTALES_TERRITORIALES,
                    D.TOTAL_NACIONAL,
                    D.ZONAS_TURISTICAS,
                    current_timestamp() LOAD_DATA_TIME
                FROM STAGING.ST_INE_ALL S
                INNER JOIN PROCESS.RL_INE_VIAJEROS_Y_PERNOCTACIONES D
                ON S.FAMILIA = D.FAMILIA
                AND S.COD = D.COD
                AND S.TABLA =  D.TABLA
                );

        END;
    END;';