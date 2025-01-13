CREATE or REPLACE TABLE SIT.DM_GLB_WEEK(
    WEEK_YEAR_KEY NUMBER(16,0),
    WEEK_KEY NUMBER(16,0),
    WEEK_YEAR_SHORT_DES VARCHAR(16),
    MONTH_YEAR_KEY NUMBER(16,0),
    MONTH_KEY NUMBER(16,0),
    MONTH_YEAR_SHORT_DES VARCHAR(16),
    YEAR_WEEK_KEY NUMBER(16,0),
    LOAD_TIME TIMESTAMP_LTZ(9)
);

copy into SIT.DM_GLB_WEEK (WEEK_YEAR_KEY,WEEK_KEY,WEEK_YEAR_SHORT_DES,MONTH_YEAR_KEY,MONTH_KEY,MONTH_YEAR_SHORT_DES,YEAR_WEEK_KEY,LOAD_TIME)
FROM (select $1,$2,$3,$4,$5,$6,$7,to_timestamp_ntz(current_timestamp)  from '@s3_stage/global/DM_GLB_WEEK_V1.csv') file_format = 'csv_format_dm' force= true on_error='continue';

create or replace table SIT.DM_GLB_DAY (
    DAY_KEY NUMBER(16,0),
    DAY_DATE DATE,
    WEEK_YEAR_KEY NUMBER(16,0),
    MONTH_YEAR_KEY NUMBER(16,0),
    MONTH_KEY NUMBER(16,0),
    DAY_WEEK_KEY NUMBER(16,0),
    YEAR_KEY NUMBER(16,0),
    SEMESTER_KEY NUMBER(16,0),
    FOURMONTH_KEY NUMBER(16,0),
    QUARTER_KEY NUMBER(16,0),
    FORTNIGHT_KEY NUMBER(16,0),
    WEEK_KEY NUMBER(16,0),
    PREV_DAY_KEY NUMBER(16,0),
    NEXT_DAY_KEY NUMBER(16,0),
    SAMEDAY_LASTWEEK_KEY NUMBER(16,0),
    SAMEDAY_LASTMONTH_KEY NUMBER(16,0),
    SAMEDAY_LASTYEAR_KEY NUMBER(16,0),
    LASTDAY_LASTWEEK_KEY NUMBER(16,0),
    LASTDAY_LASTMONTH_KEY NUMBER(16,0),
    LASTDAY_LASTYEAR_KEY NUMBER(16,0),
    SAMEWEEKDAY_LASTYEAR_KEY NUMBER(16,0),
    PUBLIC_HOLIDAY_FLG NUMBER(16,0),
    DAY_WEEK_DES VARCHAR(16),
    DAY_WEEK_SHORT_DES VARCHAR(16),
    MONTH_YEAR_SHORT_DES VARCHAR(16),
    WEEK_YEAR_SHORT_DES VARCHAR(16) ,
    SEMESTER_SHORT_DES VARCHAR(16),
    SEMESTER_YEAR_SHORT_DES VARCHAR(16),
    QUARTER_SHORT_DES VARCHAR(16),
    QUARTER_YEAR_SHORT_DES VARCHAR(16),
    LOAD_TIME TIMESTAMP_NTZ(9)
);


copy into SIT.DM_GLB_DAY (DAY_KEY,DAY_DATE,WEEK_YEAR_KEY,MONTH_YEAR_KEY,MONTH_KEY,DAY_WEEK_KEY,YEAR_KEY,SEMESTER_KEY,
FOURMONTH_KEY,QUARTER_KEY,FORTNIGHT_KEY,WEEK_KEY,PREV_DAY_KEY,NEXT_DAY_KEY,SAMEDAY_LASTWEEK_KEY,SAMEDAY_LASTMONTH_KEY,
SAMEDAY_LASTYEAR_KEY,LASTDAY_LASTWEEK_KEY,LASTDAY_LASTMONTH_KEY,LASTDAY_LASTYEAR_KEY,SAMEWEEKDAY_LASTYEAR_KEY,
PUBLIC_HOLIDAY_FLG,DAY_WEEK_DES,DAY_WEEK_SHORT_DES,MONTH_YEAR_SHORT_DES,WEEK_YEAR_SHORT_DES,
SEMESTER_SHORT_DES,SEMESTER_YEAR_SHORT_DES,QUARTER_SHORT_DES,QUARTER_YEAR_SHORT_DES,LOAD_TIME)
FROM (select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,to_timestamp_ntz(current_timestamp)  from '@s3_stage/global/DM_GLB_DAY_V6.csv') file_format = 'csv_format_dm' force= true on_error='continue';

create or replace TABLE SIT.DM_GLB_COUNTRY_MLG (
	COUNTRY_KEY NUMBER(16,0) NOT NULL,
	LANGUAGE_KEY NUMBER(16,0) NOT NULL,
	COUNTRY_DES VARCHAR(75),
	ID_INE NUMBER(16,0),
	LOAD_TIME TIMESTAMP_NTZ(9)
);

copy into SIT.DM_GLB_COUNTRY_MLG (COUNTRY_KEY,LANGUAGE_KEY,COUNTRY_DES,ID_INE,LOAD_TIME)
FROM (select $1,$2,$3,$4,to_timestamp_ntz(current_timestamp)  from '@s3_stage/global/DM_GLB_COUNTRY_MLG_V7.csv') file_format = 'csv_format_dm' force= true on_error='continue';

create or replace TABLE SIT.DM_GLB_REGION_MLG (
	REGION_KEY NUMBER(16,0) NOT NULL,
	LANGUAGE_KEY NUMBER(16,0) NOT NULL,
	REGION_DES VARCHAR(75),
	ID_INE NUMBER(16,0),
	LOAD_TIME TIMESTAMP_NTZ(9),
	primary key (REGION_KEY, LANGUAGE_KEY)
);


copy into SIT.DM_GLB_REGION_MLG (REGION_KEY,LANGUAGE_KEY,REGION_DES,ID_INE,LOAD_TIME)
FROM (select $1,$2,$3,$4,to_timestamp_ntz(current_timestamp)  from '@s3_stage/global/DM_GLB_REGION_MLG_V6.csv') file_format = 'csv_format_dm' force= true on_error='continue';

create or replace TABLE SIT.DM_FLX_ZONE_MLG (
	ZONE_KEY NUMBER(16,0),
	LANGUAGE_KEY NUMBER(16,0),
	ZONE_DES VARCHAR(100),
	LOAD_TIME TIMESTAMP_LTZ(0)
);

copy into SIT.DM_FLX_ZONE_MLG (ZONE_KEY,LANGUAGE_KEY,ZONE_DES,LOAD_TIME)
FROM (select $1,$2,$3,to_timestamp_ntz(current_timestamp)  from '@s3_stage/flux_dt/DM_FLX_ZONE_MLG_V2.csv') file_format = 'csv_format_dm' force= true on_error='continue';



create or replace TABLE SIT.DM_FLX_ZONE (
	ZONE_KEY NUMBER(16,0),
	ZONE_ID VARCHAR(100),
	ZONE_DES VARCHAR(100),
	ZONE_TYPE_DES VARCHAR(100),
	LOAD_TIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

copy into SIT.DM_FLX_ZONE (ZONE_KEY,ZONE_ID,ZONE_DES,ZONE_TYPE_DES,LOAD_TIME)
FROM (select $1,$2,$3,$4,to_timestamp_ntz(current_timestamp)  from '@s3_stage/flux_dt/DM_FLX_ZONE_V4.csv') file_format = 'csv_format_dm' force= true on_error='continue';


create or replace TABLE SIT.DM_FLX_VISITOR_ORIGIN (
	VISITOR_ORIGIN_KEY NUMBER(16,0),
	VISITOR_ORIGIN_ID VARCHAR(20),
	VISITOR_ORIGIN_DES VARCHAR(100),
	LOAD_TIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO SIT.DM_FLX_VISITOR_ORIGIN (
    VISITOR_ORIGIN_KEY, 
    VISITOR_ORIGIN_ID, 
    VISITOR_ORIGIN_DES, 
    LOAD_TIME
)
FROM (
    SELECT 
        $1, 
        $2, 
        $3, 
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM '@s3_stage/flux_dt/DM_FLX_VISITOR_ORIGIN_V2.csv'
)
FILE_FORMAT = 'csv_format_dm' 
FORCE = TRUE 
ON_ERROR = 'CONTINUE';

create or replace TABLE SIT.DM_GLB_VISITOR_CATEGORY (
	VISITOR_CATEGORY_KEY NUMBER(16,0),
	VISITOR_CATEGORY_ID VARCHAR(100),
	ID_FRONTUR VARCHAR(200),
	VISITOR_CATEGORY_DES VARCHAR(100),
	LOAD_TIME TIMESTAMP_LTZ(9)
);


COPY INTO SIT.DM_GLB_VISITOR_CATEGORY (VISITOR_CATEGORY_KEY, VISITOR_CATEGORY_ID, ID_FRONTUR, VISITOR_CATEGORY_DES, LOAD_TIME)
FROM (
    SELECT 
        $1, 
        $2, 
        $3, 
        $4, 
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM '@s3_stage/global/DM_GLB_VISITOR_CATEGORY_V3.csv'
)
FILE_FORMAT = 'csv_format_dm' 
FORCE = TRUE 
ON_ERROR = 'CONTINUE';



create or replace TABLE SIT.DM_GLB_COUNTRY (
	COUNTRY_KEY NUMBER(16,0),
	COUNTRY_ID VARCHAR(12),
	COUNTRY_DES VARCHAR(75),
	COUNTRY_NORM_DES VARCHAR(75),
	COUNTRY_SHORT_DES VARCHAR(15),
	COUNTRY_SHORT2_DES VARCHAR(15),
	COUNTRY_EUROSTAT_ID VARCHAR(15),
	ID_INE NUMBER(16,0),
	ID_ETR VARCHAR(15),
	ID_FRONTUR VARCHAR(15),
	ID_EGATUR VARCHAR(15),
	LOAD_TIME TIMESTAMP_NTZ(9)
);

COPY INTO SIT.DM_GLB_COUNTRY (
    COUNTRY_KEY, 
    COUNTRY_ID, 
    COUNTRY_DES, 
    COUNTRY_NORM_DES, 
    COUNTRY_SHORT_DES, 
    COUNTRY_SHORT2_DES, 
    COUNTRY_EUROSTAT_ID, 
    ID_INE, 
    ID_ETR, 
    ID_FRONTUR, 
    ID_EGATUR, 
    LOAD_TIME
)
FROM (
    SELECT 
        $1, 
        $2, 
        $3, 
        $4, 
        $5, 
        $6, 
        $7, 
        $8, 
        $9, 
        $10, 
        $11, 
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM '@s3_stage/global/DM_GLB_COUNTRY_V9.csv'
)
FILE_FORMAT = 'csv_format_dm' 
FORCE = TRUE 
ON_ERROR = 'CONTINUE';

create or replace TABLE SIT.DM_GLB_COUNTRY_MLG (
	COUNTRY_KEY NUMBER(16,0) NOT NULL,
	LANGUAGE_KEY NUMBER(16,0) NOT NULL,
	COUNTRY_DES VARCHAR(75),
	ID_INE NUMBER(16,0),
	LOAD_TIME TIMESTAMP_NTZ(9),
	primary key (COUNTRY_KEY, LANGUAGE_KEY)
);

COPY INTO SIT.DM_GLB_COUNTRY_MLG (
    COUNTRY_KEY, 
    LANGUAGE_KEY, 
    COUNTRY_DES, 
    ID_INE, 
    LOAD_TIME
)
FROM (
    SELECT 
        $1, 
        $2, 
        $3, 
        $4, 
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM '@s3_stage/global/DM_GLB_COUNTRY_MLG_V7.csv'
)
FILE_FORMAT = 'csv_format_dm' 
FORCE = TRUE 
ON_ERROR = 'CONTINUE';

create or replace TABLE SIT.DM_FLX_ZIPCODE (
	ZIPCODE_KEY NUMBER(16,0),
	ZIPCODE_ID VARCHAR(100),
	CITY_DES VARCHAR(200),
	PROVINCE_ID VARCHAR(10),
	PROVINCE_DES VARCHAR(100),
	CITY_ID VARCHAR(10),
	LOAD_TIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO SIT.DM_FLX_ZIPCODE (
    ZIPCODE_KEY,
    ZIPCODE_ID, 
    CITY_DES, 
    PROVINCE_ID, 
    PROVINCE_DES, 
    CITY_ID, 
    LOAD_TIME
)
FROM (
    SELECT 
        $1, 
        $2, 
        $3, 
        $4, 
        $5, 
        $6,
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM '@s3_stage/flux_dt/DM_FLX_ZIPCODE_V2.csv'
)
FILE_FORMAT = 'csv_format_dm' 
FORCE = TRUE 
ON_ERROR = 'CONTINUE';


create or replace TABLE SIT.DM_FLX_HOUR (
	HOUR_KEY NUMBER(16,0),
	HOUR_ID VARCHAR(20),
	LOAD_TIME TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO SIT.DM_FLX_HOUR (
    HOUR_KEY,
    HOUR_ID, 
    LOAD_TIME
)
FROM (
    SELECT 
        $1,
        $2, 
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM '@s3_stage/flux_dt/DM_FLX_HOUR_V1.csv'
)
FILE_FORMAT = 'csv_format_dm' 
FORCE = TRUE 
ON_ERROR = 'CONTINUE';

create or replace TABLE SIT.DM_ATR_AGE (
	AGE_KEY NUMBER(16,0),
	AGE_ID VARCHAR(20),
	AGE_DES VARCHAR(100),
	LOAD_TIME TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO SIT.DM_ATR_AGE (
    AGE_KEY,
    AGE_ID, 
    AGE_DES, 
    LOAD_TIME
)
FROM (
    SELECT 
        $1,
        $2, 
        $3,
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM '@s3_stage/atribus_dt/DM_ATR_AGE_V2.csv'
)
FILE_FORMAT = 'csv_format_dm' 
FORCE = TRUE 
ON_ERROR = 'CONTINUE';


create or replace TABLE SIT.DM_IVE_KPI_MASTER (
	KPI_KEY NUMBER(16,0),
	KPI_ID VARCHAR(100),
	KPI_DES VARCHAR(100),
	LOAD_TIME TIMESTAMP_NTZ(9)
);

COPY INTO SIT.DM_IVE_KPI_MASTER (
    KPI_KEY, 
    KPI_ID, 
    KPI_DES, 
    LOAD_TIME
)
FROM (
    SELECT 
        $1, 
        $2, 
        $3, 
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM '@s3_stage/ive_dt/DM_IVE_KPI_MASTER_V5.csv'
)
FILE_FORMAT = 'csv_format_dm' 
FORCE = TRUE 
ON_ERROR = 'CONTINUE';

create or replace TABLE SIT.DM_ATR_GENDER (
	GENDER_KEY NUMBER(16,0),
	GENDER_ID VARCHAR(20),
	GENDER_CHAR_ID VARCHAR(20),
	GENDER_DES VARCHAR(100),
	LOAD_TIME TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO SIT.DM_ATR_GENDER (
    GENDER_KEY,
    GENDER_ID, 
    GENDER_CHAR_ID, 
    GENDER_DES, 
    LOAD_TIME
)
FROM (
    SELECT 
        $1, 
        $2, 
        $3, 
        $4,
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM '@s3_stage/atribus_dt/DM_ATR_GENDER_V3.csv'
)
FILE_FORMAT = 'csv_format_dm' 
FORCE = TRUE 
ON_ERROR = 'CONTINUE';
