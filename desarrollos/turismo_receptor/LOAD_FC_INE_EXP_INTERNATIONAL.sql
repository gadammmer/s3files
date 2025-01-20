CREATE OR REPLACE TASK SIT.LOAD_FC_INE_EXP_INTERNATIONAL
    schedule='USING CRON  30 11 * * * Europe/Madrid'
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XLARGE'
	as BEGIN

            CREATE OR REPLACE TABLE SIT.FC_INE_EXP_INTERNATIONAL(
                YEAR_MONTH_KEY NUMBER(10,0),
                COUNTRY_ORIGIN_KEY NUMBER(5,0),
                CITY_DEST_KEY NUMBER(5,0),
                TOURIST_NUM NUMBER(10,0),
                LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
            );

            INSERT INTO SIT.FC_INE_EXP_INTERNATIONAL(
                SELECT
                    YEAR_MONTH_KEY,
                    COUNTRY_ORIGIN_KEY,
                    CITY_DEST_KEY,
                    TOURIST_NUM,
                    LOAD_TIME
                FROM PROCESS.AX_INE_EXP_INTERNATIONAL
            WHERE COUNTRY_ORIGIN_KEY>0 AND CITY_DEST_KEY>0 );

    END;