EXECUTE IMMEDIATE '
    BEGIN

                create or replace TABLE SIT.FC_FLX_VISITOR_DEPARTURES (
                        PERIOD_KEY NUMBER(16,0),
                        ZONE_KEY NUMBER(16,0),
                        VISITOR_ORIGIN_KEY NUMBER(16,0),
                        VISITOR_CATEGORY_KEY NUMBER(16,0),
                        COUNTRY_KEY NUMBER(16,0),
                        KPI_VALUE_NUM FLOAT,
                        LOAD_TIME TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
                );

        RETURN ''Create table SIT.FC_FLX_VISITOR_DEPARTURES'';

    END;
';


/*
EXECUTE IMMEDIATE '
    BEGIN

		create or replace TABLE SIT.FC_FLX_VISITOR_DEPARTURES (
			PERIOD_KEY NUMBER(16,0),
			ZONE_KEY NUMBER(16,0),
			VISITOR_ORIGIN_KEY NUMBER(16,0),
			VISITOR_CATEGORY_KEY NUMBER(16,0),
			COUNTRY_KEY NUMBER(16,0),
			KPI_VALUE_NUM FLOAT,
			LOAD_TIME TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
		);

       	RETURN ''Create table SIT.FC_FLX_VISITOR_DEPARTURES'';

    END;
';

EXECUTE IMMEDIATE '
    BEGIN

		create or replace TABLE SIT.FC_FLX_BY_AGE (
			PERIOD_KEY NUMBER(16,0),
			ZONE_KEY NUMBER(16,0),
			VISITOR_ORIGIN_KEY NUMBER(16,0),
			VISITOR_CATEGORY_KEY NUMBER(16,0),
			AGE_KEY NUMBER(16,0),
			KPI_KEY NUMBER(16,0),
			KPI_VALUE_NUM FLOAT,
			LOAD_TIME TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
		);

       	RETURN ''Create table SIT.FC_FLX_BY_AGE'';

    END;
';

EXECUTE IMMEDIATE '
    BEGIN

		create or replace TABLE SIT.FC_FLX_BY_GENDER (
			PERIOD_KEY NUMBER(16,0),
			ZONE_KEY NUMBER(16,0),
			VISITOR_ORIGIN_KEY NUMBER(16,0),
			VISITOR_CATEGORY_KEY NUMBER(16,0),
			GENDER_KEY NUMBER(16,0),
			KPI_KEY NUMBER(16,0),
			KPI_VALUE_NUM FLOAT,
			LOAD_TIME TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
		);

       	RETURN ''Create table SIT.FC_FLX_BY_GENDER'';

    END;
';
*/

EXECUTE IMMEDIATE '
    BEGIN

		create or replace task SIT.LOAD_FC_FLX_ALL
			schedule=''USING CRON  00 12 * * * Europe/Madrid''
			USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
			as BEGIN

				INSERT INTO SIT.FC_FLX_VISITOR_DEPARTURES 
				(
				select a.period_key,a.zone_key,a.visitor_origin_key,a.visitor_category_key,nvl(c.country_key,-3) country_key,a.KPI_VALUE_NUM,current_timestamp()
				from 
				PROCESS.AX_FLX_VISITOR_DEPARTURES_ALL_TODAY a
				,PROCESS.RL_COUNTRY_TRANSLATOR TRA
				,SIT.DM_GLB_COUNTRY c
				where a.country_des=tra.raw_country_des(+)
				and tra.country_id=c.country_id(+)
				);
			/*
				TRUNCATE TABLE SIT.FC_FLX_BY_AGE;

				INSERT INTO SIT.FC_FLX_BY_AGE 
				(
				select a.period_key,a.zone_key,a.visitor_origin_key,a.visitor_category_key,a.age_key, a.kpi_key,a.KPI_VALUE_NUM,current_timestamp()
				from 
				PROCESS.AX_FLX_BY_AGE a
				);

				TRUNCATE TABLE SIT.FC_FLX_BY_GENDER;

				INSERT INTO SIT.FC_FLX_BY_GENDER 
				(
				select a.period_key,a.zone_key,a.visitor_origin_key,a.visitor_category_key,a.GENDER_KEY, a.kpi_key,a.KPI_VALUE_NUM,current_timestamp()
				from 
				PROCESS.AX_FLX_BY_GENDER a
				);
			*/

		END;

		ALTER TASK SIT.LOAD_FC_FLX_ALL RESUME; 

       	RETURN ''Create task SIT.LOAD_FC_FLX_ALL & RESUME'';

    END;
';

