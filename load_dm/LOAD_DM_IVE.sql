-- CREATE TABLES --

EXECUTE IMMEDIATE '
      BEGIN	

		create or replace TABLE SIT.DM_IVE_KPI_MASTER (
			KPI_KEY NUMBER(16,0),
			KPI_ID VARCHAR(100),
			KPI_DES VARCHAR(100),
			LOAD_TIME TIMESTAMP_NTZ(9)
		);

            RETURN ''Created table SIT.DM_IVE_KPI_MASTER'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN	

		create or replace TABLE SIT.DM_IVE_KPI_MASTER_MLG (
			KPI_KEY NUMBER(16,0),
			LANGUAGE_KEY NUMBER(16,0),
			KPI_DES VARCHAR(100),
			LOAD_TIME TIMESTAMP_NTZ(9)
		);


            RETURN ''Created table SIT.DM_IVE_KPI_MASTER_MLG'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN	

		create or replace TABLE SIT.DM_IVE_ACCOMMODATION_TYPE (
			ACCOMMODATION_TYPE_KEY NUMBER(16,0),
			ACCOMMODATION_TYPE_ID VARCHAR(50),
			ACCOMMODATION_TYPE_DES VARCHAR(50),
			LOAD_TIME TIMESTAMP_NTZ(9)
		);

            RETURN ''Created table SIT.DM_IVE_ACCOMMODATION_TYPE'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN	

		create or replace TABLE SIT.DM_IVE_ACCOMMODATION_TYPE_MLG (
			ACCOMMODATION_TYPE_KEY NUMBER(16,0),
			LANGUAGE_KEY NUMBER(16,0),
			ACCOMMODATION_TYPE_DES VARCHAR(100),
			LOAD_TIME TIMESTAMP_NTZ(9)
		);

            RETURN ''Created table SIT.DM_IVE_ACCOMMODATION_TYPE_MLG'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN	

		create or replace TABLE SIT.DM_IVE_HOUSING_TYPE (
			HOUSING_TYPE_KEY NUMBER(16,0),
			HOUSING_TYPE_ID VARCHAR(200),
			HOUSING_TYPE_DES VARCHAR(200),
			PARENT_HOUSING_TYPE_KEY NUMBER(16,0),
			LOAD_TIME TIMESTAMP_LTZ(0)
		);

            RETURN ''Created table SIT.DM_IVE_HOUSING_TYPE'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN

		create or replace TABLE SIT.DM_IVE_GATEWAY_TYPE (
			GATEWAY_TYPE_KEY NUMBER(16,0),
			GATEWAY_TYPE_ID VARCHAR(200),
			GATEWAY_TYPE_DES VARCHAR(200),
			PARENT_GATEWAY_TYPE_KEY NUMBER(16,0),
			LOAD_TIME TIMESTAMP_LTZ(0)
		);

            RETURN ''Created table SIT.DM_IVE_GATEWAY_TYPE'';

      END;
';


EXECUTE IMMEDIATE '
      BEGIN

		create or replace TABLE SIT.DM_IVE_TRAVEL_REASON (
			TRAVEL_REASON_KEY NUMBER(16,0),
			TRAVEL_REASON_ID VARCHAR(200),
			TRAVEL_REASON_DES VARCHAR(200),
			LOAD_TIME TIMESTAMP_LTZ(0)
		);

            RETURN ''Created table SIT.DM_IVE_TRAVEL_REASON'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN

		create or replace TABLE SIT.DM_IVE_SPEND_TYPE (
			SPEND_TYPE_KEY NUMBER(16,0),
			SPEND_TYPE_ID VARCHAR(200),
			SPEND_TYPE_DES VARCHAR(200),
			LOAD_TIME TIMESTAMP_LTZ(0)
		);

            RETURN ''Created table SIT.DM_IVE_SPEND_TYPE'';

      END;
';

-- INSERT DATA --

EXECUTE IMMEDIATE '
      BEGIN

		copy into SIT.DM_IVE_KPI_MASTER (KPI_KEY, KPI_ID, KPI_DES, LOAD_TIME) 
		FROM (
		    SELECT 
		        $1, 
		        $2, 
		        $3,
		        to_timestamp_ntz(current_timestamp)
		    FROM ''@s3_stage/ive_dt/DM_IVE_KPI_MASTER_V4.csv''
		)
		file_format = ''csv_format_dm''
		on_error = ''continue'';

            RETURN ''Insert data into SIT.DM_IVE_KPI_MASTER'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN


		copy into SIT.DM_IVE_KPI_MASTER_MLG (KPI_KEY, LANGUAGE_KEY, KPI_DES, LOAD_TIME) 
		FROM (
		    SELECT 
		        $1, 
		        $2, 
		        $3,
		        to_timestamp_ntz(current_timestamp)
		    FROM ''@s3_stage/ive_dt/DM_IVE_KPI_MASTER_MLG_V1.csv''
		)
		file_format = ''csv_format_dm''
		on_error = ''continue'';

            RETURN ''Insert data into SIT.DM_IVE_KPI_MASTER_MLG'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN

		copy into SIT.DM_IVE_ACCOMMODATION_TYPE (ACCOMMODATION_TYPE_KEY, ACCOMMODATION_TYPE_ID, ACCOMMODATION_TYPE_DES, LOAD_TIME) 
		FROM (
		    SELECT 
		        $1, 
		        $2, 
		        $3,
		        to_timestamp_ntz(current_timestamp)
		    FROM ''@s3_stage/ive_dt/DM_IVE_ACCOMMODATION_TYPE_V1.csv''
		)
		file_format = ''csv_format_dm''
		on_error = ''continue'';

            RETURN ''Insert data into SIT.DM_IVE_ACCOMMODATION_TYPE'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN

		copy into SIT.DM_IVE_ACCOMMODATION_TYPE_MLG (ACCOMMODATION_TYPE_KEY, LANGUAGE_KEY, ACCOMMODATION_TYPE_DES, LOAD_TIME) 
		FROM (
		    SELECT 
		        $1, 
		        $2, 
		        $3,
		        to_timestamp_ntz(current_timestamp)
		    FROM ''@s3_stage/ive_dt/DM_IVE_ACCOMMODATION_TYPE_MLG_V1.csv''
		)
		file_format = ''csv_format_dm''
		on_error = ''continue'';
            
            RETURN ''Insert data into SIT.DM_IVE_ACCOMMODATION_TYPE_MLG'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN

		copy into SIT.DM_IVE_ACCOMMODATION_TYPE_MLG (ACCOMMODATION_TYPE_KEY, LANGUAGE_KEY, ACCOMMODATION_TYPE_DES, LOAD_TIME) 
		FROM (
		    SELECT 
		        $1, 
		        $2, 
		        $3,
		        to_timestamp_ntz(current_timestamp)
		    FROM ''@s3_stage/ive_dt/DM_IVE_ACCOMMODATION_TYPE_MLG_V1.csv''
		)
		file_format = ''csv_format_dm''
		on_error = ''continue'';

            RETURN ''Insert data into SIT.DM_IVE_ACCOMMODATION_TYPE_MLG'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN

		copy into SIT.DM_IVE_HOUSING_TYPE (HOUSING_TYPE_KEY, HOUSING_TYPE_ID, HOUSING_TYPE_DES, PARENT_HOUSING_TYPE_KEY, LOAD_TIME) 
		FROM (
		    SELECT 
		        $1, 
		        $2, 
		        $3,
		        $4,
		        to_timestamp_ntz(current_timestamp)
		    FROM ''@s3_stage/ive_dt/DM_IVE_HOUSING_TYPE_V1.csv''
		)
		file_format = ''csv_format_dm''
		on_error = ''continue'';

            RETURN ''Insert data into SIT.DM_IVE_HOUSING_TYPE'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN

		copy into SIT.DM_IVE_GATEWAY_TYPE (GATEWAY_TYPE_KEY, GATEWAY_TYPE_ID, GATEWAY_TYPE_DES, PARENT_GATEWAY_TYPE_KEY, LOAD_TIME) 
		FROM (
		    SELECT 
		        $1, 
		        $2, 
		        $3,
		        $4,
		        to_timestamp_ntz(current_timestamp)
		    FROM ''@s3_stage/ive_dt/DM_IVE_GATEWAY_TYPE_V1.csv''
		)
		file_format = ''csv_format_dm''
		on_error = ''continue'';

            RETURN ''Insert data into SIT.DM_IVE_GATEWAY_TYPE'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN

		copy into SIT.DM_IVE_TRAVEL_REASON (TRAVEL_REASON_KEY, TRAVEL_REASON_ID, TRAVEL_REASON_DES, LOAD_TIME) 
		FROM (
		    SELECT 
		        $1, 
		        $2, 
		        $3,
		        to_timestamp_ntz(current_timestamp)
		    FROM ''@s3_stage/ive_dt/DM_IVE_TRAVEL_REASON_V1.csv''
		)
		file_format = ''csv_format_dm''
		on_error = ''continue'';

            RETURN ''Insert data into SIT.DM_IVE_TRAVEL_REASON'';

      END;
';

EXECUTE IMMEDIATE '
      BEGIN

		copy into SIT.DM_IVE_SPEND_TYPE (SPEND_TYPE_KEY, SPEND_TYPE_ID, SPEND_TYPE_DES, LOAD_TIME) 
		FROM (
		    SELECT 
		        $1, 
		        $2, 
		        $3,
		        to_timestamp_ntz(current_timestamp)
		    FROM ''@s3_stage/ive_dt/DM_IVE_SPEND_TYPE_V1.csv''
		)
		file_format = ''csv_format_dm''
		on_error = ''continue'';

            RETURN ''Insert data into SIT.DM_IVE_SPEND_TYPE'';

      END;
';


