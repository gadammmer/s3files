
CREATE OR REPLACE TASK SIT.LOAD_FC_INE_EXP_TURISMO
    schedule='USING CRON  30 11 * * * Europe/Madrid'
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XLARGE'
	as BEGIN

            CREATE OR REPLACE TABLE SIT.FC_INE_EXP_TOURISM(
                PERIOD_KEY NUMBER(9,0),
                RESIDENCE_KEY NUMBER(16,0),
                REGION_KEY NUMBER(16,0),
                PROVINCE_KEY NUMBER(16,0),
                COUNTRY_KEY NUMBER(16,0),
                KPI_KEY NUMBER(16,0),
                KPI_VALUE_NUM FLOAT,
                LOAD_TIME	TIMESTAMP_NTZ(9)
            );

            INSERT INTO SIT.FC_INE_EXP_TOURISM  
                (
                    SELECT
                        PERIOD_KEY,
                        RESIDENCE_KEY,
                        REGION_KEY,
                        PROVINCE_KEY,
                        COUNTRY_KEY,
                        KPI_KEY,
                        KPI_VALUE_NUM,
                        CURRENT_TIMESTAMP(0) LOAD_TIME
                    FROM PROCESS.AX_INE_EXP_TOURISM a             
                );
                END;