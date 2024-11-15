EXECUTE IMMEDIATE '
    BEGIN

		create or replace TABLE PROCESS.AX_FLX_VISITOR_DEPARTURES_ALL_TODAY (
			PERIOD_KEY NUMBER(16,0),
			ZONE_KEY NUMBER(16,0),
			VISITOR_ORIGIN_KEY NUMBER(16,0),
			VISITOR_CATEGORY_KEY NUMBER(16,0),
			COUNTRY_DES VARCHAR(200),
			KPI_VALUE_NUM FLOAT,
			LOAD_TIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
		);

        RETURN ''Create table PROCESS.AX_FLX_VISITOR_DEPARTURES_ALL_TODAY'';

    END;
';
/*
EXECUTE IMMEDIATE '
    BEGIN

		create or replace HYBRID TABLE PROCESS.AX_FLX_BY_AGE_ALL_TODAY (
			PERIOD_KEY NUMBER(16,0) NOT NULL,
			ZONE_ID VARCHAR(100) NOT NULL,
			VISITOR_ORIGIN_ID VARCHAR(20) NOT NULL,
			VISITOR_CATEGORY_ID VARCHAR(100) NOT NULL,
			AGE_ID VARCHAR(20) NOT NULL,
			KPI_ID VARCHAR(100) NOT NULL,
			KPI_VALUE_NUM FLOAT,
			LOAD_TIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
			primary key (PERIOD_KEY, ZONE_ID, VISITOR_ORIGIN_ID, VISITOR_CATEGORY_ID, AGE_ID, KPI_ID)
		);

        RETURN ''Create table PROCESS.AX_FLX_BY_AGE_ALL_TODAY'';

    END;
';

EXECUTE IMMEDIATE '
    BEGIN

		create or replace TABLE PROCESS.AX_FLX_BY_AGE (
			PERIOD_KEY NUMBER(16,0),
			ZONE_KEY NUMBER(16,0),
			VISITOR_ORIGIN_KEY NUMBER(16,0),
			VISITOR_CATEGORY_KEY NUMBER(16,0),
			AGE_KEY NUMBER(16,0),
			KPI_KEY NUMBER(16,0),
			KPI_VALUE_NUM FLOAT,
			LOAD_TIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
		);

        RETURN ''Create table PROCESS.AX_FLX_BY_AGE'';

    END;
';

EXECUTE IMMEDIATE '
    BEGIN

		create or replace TABLE PROCESS.AX_FLX_BY_GENDER (
			PERIOD_KEY NUMBER(16,0),
			ZONE_KEY NUMBER(16,0),
			VISITOR_ORIGIN_KEY NUMBER(16,0),
			VISITOR_CATEGORY_KEY NUMBER(16,0),
			GENDER_KEY NUMBER(16,0),
			KPI_KEY NUMBER(16,0),
			KPI_VALUE_NUM FLOAT,
			LOAD_TIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
		);

        RETURN ''Create table PROCESS.AX_FLX_BY_GENDER'';

    END;
';

*/
EXECUTE IMMEDIATE '
    BEGIN

			create or replace task PROCESS.LOAD_AX_FLX_ALL
				schedule=''USING CRON  30 11 * * * Europe/Madrid''
				USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
				as BEGIN

					TRUNCATE TABLE PROCESS.AX_FLX_VISITOR_DEPARTURES_ALL_TODAY;

					INSERT INTO PROCESS.AX_FLX_VISITOR_DEPARTURES_ALL_TODAY
					(
					select to_number(to_char(fecha,''YYYYMMDD'')) period_key,nvl(z.ZONE_KEY,-3) ZONE_KEY,nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,decode(a.pais,''Acumulado'',''Todos los Paises'',''EE.UU'',''Estados Unidos de América'',''Congo-Kinshasa'',''República Democrática del Congo'',
''Cáucaso'',''Países del Cáucaso'',''NC'',''No Informado'',''África del Sur'',''Sudáfrica'', a.pais) COUNTRY_DES,Volumen KPI_VALUE_NUM,current_timestamp() LOAD_TIME
					from STAGING.ST_FLX_SALIDA_NACIONALIDAD a
					,(select max(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_SALIDA_NACIONALIDAD 
  					where (file_name like ''%INVA-1080-6430%'' or
  					file_name like ''%INVA-1080-6642%'')
 					) M
					,SIT.DM_FLX_ZONE z
					,SIT.DM_GLB_VISITOR_CATEGORY C
					,SIT.DM_FLX_VISITOR_ORIGIN O
					where
 					a.load_date=m.load_date
 					and (a.file_name like ''%INVA-1080-6430%'' or
      					a.file_name like ''%INVA-1080-6642%'')
					and a.origen=o.VISITOR_ORIGIN_ID(+)
					and a.zonaobservacion=z.ZONE_ID(+)
					and a.categoriadelvisitante=c.VISITOR_CATEGORY_ID(+)
					);

			   /*

					TRUNCATE TABLE PROCESS.AX_FLX_BY_AGE_ALL_TODAY;

					INSERT INTO PROCESS.AX_FLX_BY_AGE_ALL_TODAY
					(
					select to_number(to_char(fecha,''YYYYMMDD'')) period_key,zonaobservacion ZONE_ID,origen VISITOR_ORIGIN_ID,a.categoriadelvisitante VISITOR_CATEGORY_ID,case when edad=''65 o más'' then ''65+'' when  edad=''<18'' then ''14-17'' else a.edad end Edad,''Diario'' KPI_ID
					,Volumen KPI_VALUE_NUM,current_timestamp() LOAD_TIME
					from STAGING.ST_FLX_DIARIO_EDAD a
					,(select max(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_EDAD) M
					where 
					 a.load_date=m.load_date
					 union all
					 select to_number(to_char(fecha,''YYYYMMDD'')) period_key,zonaobservacion ZONE_ID,origen VISITOR_ORIGIN_ID,a.categoriadelvisitante VISITOR_CATEGORY_ID,case when edad=''65 o más'' then ''65+'' when  edad=''<18'' then ''14-17'' else a.edad end Edad,''Llegada'' KPI_ID
					,Volumen KPI_VALUE_NUM,current_timestamp() LOAD_TIME
					from STAGING.ST_FLX_LLEGADA_EDAD a
					,(select max(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_LLEGADA_EDAD) M
					where 
					 a.load_date=m.load_date
					 union all
					 select to_number(to_char(fecha,''YYYYMMDD'')) period_key,zonaobservacion ZONE_ID,origen VISITOR_ORIGIN_ID,a.categoriadelvisitante VISITOR_CATEGORY_ID,case when edad=''65 o más'' then ''65+'' when  edad=''<18'' then ''14-17'' else a.edad end Edad,''Salida'' KPI_ID
					,Volumen KPI_VALUE_NUM,current_timestamp() LOAD_TIME
					from STAGING.ST_FLX_SALIDA_EDAD a
					,(select max(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_SALIDA_EDAD) M
					where 
					 a.load_date=m.load_date
					 union all
					 select to_number(to_char(fecha,''YYYYMMDD'')) period_key,zonaobservacion ZONE_ID,origen VISITOR_ORIGIN_ID,a.categoriadelvisitante VISITOR_CATEGORY_ID,case when edad=''65 o más'' then ''65+'' when  edad=''<18'' then ''14-17'' else a.edad end Edad,''Nocturno'' KPI_ID
					,Volumen KPI_VALUE_NUM,current_timestamp() LOAD_TIME
					from STAGING.ST_FLX_NOCTURNO_EDAD a
					,(select max(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCTURNO_EDAD) M
					where 
					 a.load_date=m.load_date
					);


					TRUNCATE TABLE PROCESS.AX_FLX_BY_AGE;

					INSERT INTO PROCESS.AX_FLX_BY_AGE
					(
					select a.period_key,nvl(z.ZONE_KEY,-3) ZONE_KEY,nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,nvl(AGE_KEY,-3) AGE_KEY,nvl(KPI_KEY,-3) KPI_KEY
					,a.KPI_VALUE_NUM,current_timestamp() LOAD_TIME
					from PROCESS.AX_FLX_BY_AGE_ALL_TODAY a
					,SIT.DM_FLX_ZONE z
					,SIT.DM_GLB_VISITOR_CATEGORY C
					,SIT.DM_FLX_VISITOR_ORIGIN O
					,SIT.DM_ATR_AGE AGE
					,SIT.DM_IVE_KPI_MASTER KPI
					where 
					 a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
					and a.ZONE_ID=z.ZONE_ID(+)
					and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
					and a.age_id  =AGE.age_id(+)
					and a.kpi_id= kpi.kpi_id(+)
					union all
					select a.* from SIT.FC_FLX_BY_AGE a,
					(select distinct PERIOD_KEY from  PROCESS.AX_FLX_BY_AGE_ALL_TODAY) b
					where a.period_key=b.period_key(+) 
					and b.period_key is null
					);


					TRUNCATE TABLE PROCESS.AX_FLX_BY_GENDER_ALL_TODAY;

					INSERT INTO PROCESS.AX_FLX_BY_GENDER_ALL_TODAY
					(
					select to_number(to_char(fecha,''YYYYMMDD'')) period_key,zonaobservacion ZONE_ID,origen VISITOR_ORIGIN_ID,a.categoriadelvisitante VISITOR_CATEGORY_ID,GENERO GENDER_ID,''Diario'' KPI_ID
					,Volumen KPI_VALUE_NUM,current_timestamp() LOAD_TIME
					from STAGING.ST_FLX_DIARIO_GENERO a
					,(select max(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_GENERO) M
					where 
					 a.load_date=m.load_date
					 union all
					 select to_number(to_char(fecha,''YYYYMMDD'')) period_key,zonaobservacion ZONE_ID,origen VISITOR_ORIGIN_ID,a.categoriadelvisitante VISITOR_CATEGORY_ID,GENERO GENDER_ID,''Llegada'' KPI_ID
					,Volumen KPI_VALUE_NUM,current_timestamp() LOAD_TIME
					from STAGING.ST_FLX_LLEGADA_GENERO a
					,(select max(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_LLEGADA_GENERO) M
					where 
					 a.load_date=m.load_date
					 union all
					 select to_number(to_char(fecha,''YYYYMMDD'')) period_key,zonaobservacion ZONE_ID,origen VISITOR_ORIGIN_ID,a.categoriadelvisitante VISITOR_CATEGORY_ID,GENERO GENDER_ID,''Salida'' KPI_ID
					,Volumen KPI_VALUE_NUM,current_timestamp() LOAD_TIME
					from STAGING.ST_FLX_SALIDA_GENERO a
					,(select max(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_SALIDA_GENERO) M
					where 
					 a.load_date=m.load_date
					 union all
					 select to_number(to_char(fecha,''YYYYMMDD'')) period_key,zonaobservacion ZONE_ID,origen VISITOR_ORIGIN_ID,a.categoriadelvisitante VISITOR_CATEGORY_ID,GENERO GENDER_ID,''Nocturno'' KPI_ID
					,Volumen KPI_VALUE_NUM,current_timestamp() LOAD_TIME
					from STAGING.ST_FLX_NOCTURNO_GENERO a
					,(select max(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCTURNO_GENERO) M
					where 
					 a.load_date=m.load_date
					);

					TRUNCATE TABLE PROCESS.AX_FLX_BY_GENDER;

					INSERT INTO PROCESS.AX_FLX_BY_GENDER
					(
					select a.period_key,nvl(z.ZONE_KEY,-3) ZONE_KEY,nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,nvl(GENDER_KEY,-3) GENDER_KEY,nvl(KPI_KEY,-3) KPI_KEY
					,a.KPI_VALUE_NUM,current_timestamp() LOAD_TIME
					from PROCESS.AX_FLX_BY_GENDER_ALL_TODAY a
					,SIT.DM_FLX_ZONE z
					,SIT.DM_GLB_VISITOR_CATEGORY C
					,SIT.DM_FLX_VISITOR_ORIGIN O
					,SIT.DM_ATR_GENDER GENDER
					,SIT.DM_IVE_KPI_MASTER KPI
					where 
					 a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
					and a.ZONE_ID=z.ZONE_ID(+)
					and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
					and a.GENDER_id  =GENDER.GENDER_CHAR_id(+)
					and a.kpi_id= kpi.kpi_id(+)
					union all
					select a.* from SIT.FC_FLX_BY_GENDER a,
					(select distinct PERIOD_KEY from  PROCESS.AX_FLX_BY_GENDER_ALL_TODAY) b
					where a.period_key=b.period_key(+) 
					and b.period_key is null
					);
				*/
			END;

        	RETURN ''Create task PROCESS.LOAD_AX_FLX_ALL & RESUME'';

    END;
';


EXECUTE IMMEDIATE '
    BEGIN

		create or replace TABLE PROCESS.RL_COUNTRY_TRANSLATOR (
			RAW_COUNTRY_DES VARCHAR(200),
			ORIGIN_DATA_ID VARCHAR(200),
			COUNTRY_DES VARCHAR(200),
			COUNTRY_ID VARCHAR(200),
			LOAD_TIME TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
		);


	        copy into PROCESS.RL_COUNTRY_TRANSLATOR(RAW_COUNTRY_DES, ORIGIN_DATA_ID, COUNTRY_DES, COUNTRY_ID, LOAD_TIME)
                FROM (
                        SELECT
                        $1,
                        $2,
                        $3,
                        $4,
                        to_timestamp_ntz(current_timestamp)

                        FROM ''@s3_stage/flux_dt/DATA_RL_COUNTRY_TRANSLATOR.csv''
                )
                file_format=''csv_format_dm''
                on_error=''continue'';


       	RETURN ''Create table PROCESS.RL_COUNTRY_TRANSLATOR && Insert historic'';

    END;
';


EXECUTE IMMEDIATE '
 BEGIN

    create or replace task PROCESS.LOAD_RL_COUNTRY_TRANSLATOR
        USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
        after PROCESS.LOAD_AX_FLX_ALL
    as
    insert into PROCESS.RL_COUNTRY_TRANSLATOR (
        select a.COUNTRY_DES RAW_COUNTRY_DES,
               ''ST_FLX_SALIDA_NACIONALIDAD'' ORIGIN_DATA_ID,
               nvl(max(b.country_des),''*********'') country_des,
               nvl(max(b.country_id),''***'') country_id,
               current_timestamp() LOAD_TIME
        from
            (select TRANSLATE(upper(a.COUNTRY_DES),
                ''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’'', ''AEIOUAEIOUAEIOUO'') tra_country_des_JOIN,
                country_DES
             from PROCESS.AX_FLX_VISITOR_DEPARTURES_ALL_TODAY a) a
        left join
            (select distinct TRANSLATE(upper(a.COUNTRY_DES),
                ''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’'', ''AEIOUAEIOUAEIOUO'') COUNTRY_DES_JOIN,
                a.country_des, country_id
             from SIT.DM_GLB_COUNTRY_MLG a, SIT.DM_GLB_COUNTRY b
             where a.COUNTRY_DES_JOIN = b.COUNTRY_DES_JOIN) b
        on a.tra_country_des_JOIN = b.COUNTRY_DES_JOIN
        where a.COUNTRY_DES = tra.RAW_COUNTRY_DES(+)
        and tra.RAW_COUNTRY_DES is null
        group by a.COUNTRY_DES
    );

    ALTER TASK PROCESS.LOAD_RL_COUNTRY_TRANSLATOR RESUME;

    ALTER TASK PROCESS.LOAD_AX_FLX_ALL RESUME;

    RETURN ''Create task PROCESS.LOAD_RL_COUNTRY_TRANSLATOR & RESUME'';
END;

';
