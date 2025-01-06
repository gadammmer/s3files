EXECUTE IMMEDIATE '
    BEGIN

        create or replace TABLE SIT.DM_FWK_AIRPORT (
            AIRPORTIATACODE VARCHAR(5),
            ID NUMBER(38,0) NOT NULL,
            AIRPORTICAOCODE VARCHAR(5),
            TERMINALCODE VARCHAR(10),
            AIRPORTNAME VARCHAR(100),
            CITYCODE VARCHAR(5),
            CITYNAME VARCHAR(50),
            COUNTRYCODE VARCHAR(3),
            REGIONCODE VARCHAR(10),
            COUNTRYNAME VARCHAR(100),
            ISO_CODE VARCHAR(5),
            SUBCONTINENTCODE VARCHAR(10),
            REGIONNAME VARCHAR(100),
            SUBCONTINENTNAME VARCHAR(100),
            CONTINENTCODE VARCHAR(3),
            CONTINENTNAME VARCHAR(100),
            ACTIVESTATUS NUMBER(38,0),
            AIRPORTLATITUDE FLOAT,
            AIRPORTLONGITUDE FLOAT,
            LOAD_TIME TIMESTAMP_NTZ(9),
            primary key (ID)
        );

        copy into SIT.DM_FWK_AIRPORT (AIRPORTIATACODE,ID,AIRPORTICAOCODE,TERMINALCODE,AIRPORTNAME,CITYCODE,CITYNAME,COUNTRYCODE,REGIONCODE,COUNTRYNAME,ISO_CODE,SUBCONTINENTCODE,REGIONNAME,SUBCONTINENTNAME,CONTINENTCODE,CONTINENTNAME,ACTIVESTATUS,AIRPORTLATITUDE,AIRPORTLONGITUDE,LOAD_TIME) 
            FROM (select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,to_timestamp_ntz(current_timestamp)  from ''@s3_stage/fwk_dt/DM_FWK_AIRPORT_V6.csv'' ) file_format = ''csv_format_dm'' force= true;

        RETURN ''Create & insert DM_FWK_AIRPORT'';

    END;
';

EXECUTE IMMEDIATE '
    BEGIN

        create or replace TABLE SIT.DM_FWK_AIRPORT_MLG (
            AIRPORTIATACODE VARCHAR(5),
            ID NUMBER(38,0) NOT NULL,
            AIRPORTICAOCODE VARCHAR(5),
            TERMINALCODE VARCHAR(10),
            AIRPORTNAME VARCHAR(100),
            CITYCODE VARCHAR(5),
            CITYNAME VARCHAR(50),
            COUNTRYCODE VARCHAR(3),
            REGIONCODE VARCHAR(10),
            COUNTRYNAME VARCHAR(100),
            ISO_CODE VARCHAR(5),
            SUBCONTINENTCODE VARCHAR(10),
            REGIONNAME VARCHAR(100),
            SUBCONTINENTNAME VARCHAR(100),
            CONTINENTCODE VARCHAR(3),
            CONTINENTNAME VARCHAR(100),
            ACTIVESTATUS NUMBER(38,0),
            AIRPORTLATITUDE FLOAT,
            AIRPORTLONGITUDE FLOAT,
            LOAD_TIME TIMESTAMP_NTZ(9),
            LANGUAGE_KEY NUMBER(38,0),
            primary key (ID)
        );

        copy into SIT.DM_FWK_AIRPORT_MLG (AIRPORTIATACODE,ID,AIRPORTICAOCODE,TERMINALCODE,AIRPORTNAME,CITYCODE,CITYNAME,COUNTRYCODE,REGIONCODE,COUNTRYNAME
        ,ISO_CODE,SUBCONTINENTCODE,REGIONNAME,SUBCONTINENTNAME,CONTINENTCODE,CONTINENTNAME,ACTIVESTATUS,AIRPORTLATITUDE,AIRPORTLONGITUDE,LOAD_TIME,LANGUAGE_KEY) 
            FROM (select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,to_timestamp_ntz(current_timestamp),$21 from ''@s3_stage/fwk_dt/DM_FWK_AIRPORT_MLG_V5.csv'' ) file_format = ''csv_format_dm'' on_error = ''CONTINUE'' force= true;

        RETURN ''Create & insert DM_FWK_AIRPORT_MLG'';

    END;
';

EXECUTE IMMEDIATE '
    BEGIN

        create or replace TABLE SIT.DM_FWK_CARRIER (
            AIRLINEIATACODE VARCHAR(3),
            ID NUMBER(38,0) NOT NULL,
            AIRLINENAME VARCHAR(100),
            BIZMODEL VARCHAR(30),
            ACTIVESTATUS VARCHAR(100),
            GROUPNAME VARCHAR(50),
            HUBAIRPORTIATACODE VARCHAR(100),
            COUNTRYCODE VARCHAR(3),
            STARTDATE VARCHAR(100),
            ENDDATE VARCHAR(100),
            LOAD_TIME TIMESTAMP_NTZ(9),
            primary key (ID)
        );

        copy into SIT.DM_FWK_CARRIER (AIRLINEIATACODE,ID,AIRLINENAME,BIZMODEL,ACTIVESTATUS,GROUPNAME,HUBAIRPORTIATACODE,COUNTRYCODE,STARTDATE,ENDDATE,LOAD_TIME) 
            FROM (select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,to_timestamp_ntz(current_timestamp) from ''@s3_stage/fwk_dt/DM_FWK_CARRIER_V4.csv'' ) file_format = ''csv_format_dm'' on_error = ''CONTINUE'' force= true;

        RETURN ''Create & insert DM_FWK_CARRIER'';

    END;
';

EXECUTE IMMEDIATE '
    BEGIN

        create or replace TABLE SIT.DM_REF_CITY_MLG (
            CITYCODE VARCHAR(15),
            ID NUMBER(38,0) NOT NULL,
            CITYNAME VARCHAR(50),
            COUNTRYCODE VARCHAR(3),
            REGIONCODE VARCHAR(10),
            COUNTRYNAME VARCHAR(100),
            ISO_CODE VARCHAR(5),
            SUBCONTINENTCODE VARCHAR(10),
            REGIONNAME VARCHAR(100),
            SUBCONTINENTNAME VARCHAR(100),
            CONTINENTCODE VARCHAR(3),
            CONTINENTNAME VARCHAR(100),
            ACTIVESTATUS NUMBER(38,0),
            LATITUDE_NUM VARCHAR(100),
            LONGITUDE_NUM VARCHAR(100),
            LANGUAGE_KEY NUMBER(38,0),
            LOAD_TIME TIMESTAMP_NTZ(9),
            primary key (ID)
        );

        copy into SIT.DM_REF_CITY_MLG (CITYCODE,ID,CITYNAME,COUNTRYCODE,REGIONCODE,COUNTRYNAME,ISO_CODE,SUBCONTINENTCODE,REGIONNAME,SUBCONTINENTNAME
        ,CONTINENTCODE,CONTINENTNAME,ACTIVESTATUS,LATITUDE_NUM,LONGITUDE_NUM,LANGUAGE_KEY,LOAD_TIME) 
            FROM (select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,to_timestamp_ntz(current_timestamp)  
            from ''@s3_stage/dm_ref/DM_REF_CITY_MLG_V3.csv'' ) 
            file_format = ''csv_format_dm'' 
            on_error = ''CONTINUE'' 
            force= true;

        RETURN ''Create & insert DM_REF_CITY_MLG'';

    END;
';

EXECUTE IMMEDIATE '
    BEGIN
        create or replace TABLE SIT.DM_FWK_CABIN_CLASS (
            NAME VARCHAR(15),
            ID NUMBER(38,0) NOT NULL,
            primary key (ID)
        );

        INSERT INTO SIT.DM_FWK_CABIN_CLASS (NAME, ID) VALUES (''FIRST'', ''1'');
        INSERT INTO SIT.DM_FWK_CABIN_CLASS (NAME, ID) VALUES (''BUSINESS'', ''2'');
        INSERT INTO SIT.DM_FWK_CABIN_CLASS (NAME, ID) VALUES (''PREMIUM_ECONOMY'', ''3'');
        INSERT INTO SIT.DM_FWK_CABIN_CLASS (NAME, ID) VALUES (''ECONOMY'', ''4'');

        RETURN ''Create & insert DM_FWK_CABIN_CLASS'';

    END;
';


EXECUTE IMMEDIATE '
    BEGIN

        update SIT.DM_REF_REGION_MLG
        set REGIONNAME = ''Comunitat Valenciana''
        where ID = 21 and LANGUAGE_KEY = 1;

        update SIT.DM_FWK_AIRPORT_MLG
        set REGIONNAME = ''Comunitat Valenciana''
        where REGIONNAME = ''Comunidad Valenciana'';

        update SIT.DM_REF_CITY_MLG
        set REGIONNAME = ''Comunitat Valenciana''
        where REGIONNAME = ''Comunidad Valenciana'';

        RETURN ''Update DM_REF_REGION_MLG, DM_FWK_AIRPORT_MLG, DM_REF_CITY_MLG'';

    END;
';
