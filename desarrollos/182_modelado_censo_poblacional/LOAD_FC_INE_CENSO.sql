EXECUTE IMMEDIATE '
    BEGIN

CREATE OR REPLACE TASK SIT.LOAD_FC_INE_CENSO
    schedule=''USING CRON  30 11 * * * Europe/Madrid''
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
	as BEGIN

        --------------------------------------------
        -- ------FC_INE_CENSO---------------
        --------------------------------------------

        CREATE or REPLACE TABLE SIT.FC_INE_CENSO
        (
            PERIOD_KEY	      NUMBER(8,0)
            ,CITY_KEY         NUMBER(6,0)
            ,GENDER_KEY       NUMBER(6,0)
            ,AGE_KEY          NUMBER(6,0)
            ,KPI_KEY          NUMBER(6,0)
            ,KPI_VALUE_NUM    FLOAT
            ,LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
        ) ;


        INSERT INTO SIT.FC_INE_CENSO
        SELECT
            PERIOD_KEY	    
            ,CITY_KEY       
            ,GENDER_KEY     
            ,AGE_KEY        
            ,KPI_KEY        
            ,KPI_VALUE_NUM  
            ,current_timestamp() LOAD_TIME
        FROM PROCESS.AX_INE_CENSO;
    END;

END;';