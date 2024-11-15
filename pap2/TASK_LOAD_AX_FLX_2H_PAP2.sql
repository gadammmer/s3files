ALTER TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_AX_FLX_AGE SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_AX_FLX_BY_CITY SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H SUSPEND;

DROP TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H;
DROP TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H;
DROP TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H;
DROP TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H;
DROP TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H;
DROP TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H;
DROP TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H;
DROP TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H;
DROP TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H;
DROP TASK IF EXISTS PROCESS.LOAD_AX_FLX_2H;

create or replace TABLE PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (
	DUPLICATE_DETAIL VARCHAR(200),
	LOAD_TIME DATE DEFAULT CURRENT_DATE(),
	DUPLICATE_COUNT NUMBER(16,0)
);

create or replace TABLE SIT.WK_FLX_ALARM_DUPLICATE_LOG (
	DUPLICATE_DETAIL VARCHAR(200),
	LOAD_TIME DATE DEFAULT CURRENT_DATE(),
	DUPLICATE_COUNT NUMBER(16,0)
);

EXECUTE IMMEDIATE '
    BEGIN

        create or replace task PROCESS.LOAD_AX_FLX_2H
            schedule=''USING CRON  30 11 * * * Europe/Madrid''
            USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
            as BEGIN
                INSERT INTO PROCESS.AX_FLX_2H_PRE_ALL_TODAY
                (
                WITH CONSOLIDADO AS (
                select 
                to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                trim(zonaobservacion) AS zonaobservacion,
                trim(origen) AS origen,
                trim(categoriadelvisitante) AS categoriadelvisitante,
                TRIM(HORA) AS HORA ,
                Volumen ,
                trim(continuo) continuo,
                trim(A.LOAD_DATE) LOAD_DATE
                from STAGING.ST_FLX_DIARIO_2H a,
                (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_2H) M	
                where a.load_date=m.load_date  
                AND (trim(CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
                AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''   
                ),

                CONTINUO AS (
                select 
                to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                trim(zonaobservacion) AS zonaobservacion,
                trim(origen) AS origen,
                trim(categoriadelvisitante) AS categoriadelvisitante,
                TRIM(HORA) AS HORA ,
                Volumen ,
                trim(continuo) continuo,
                trim(A.LOAD_DATE) LOAD_DATE
                from STAGING.ST_FLX_DIARIO_2H a,
                (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_2H) M	
                where a.load_date=m.load_date  
                AND (trim(CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
                AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''
                ),

                TOTAL AS (
                SELECT 
                period_key,
                zonaobservacion ZONE_ID,
                origen VISITOR_ORIGIN_ID,
                categoriadelvisitante VISITOR_CATEGORY_ID,
                HORA HOUR_ID,
                Volumen KPI_VALUE_NUM,
                continuo,
                LOAD_DATE        
                FROM CONSOLIDADO

                UNION ALL

                SELECT 
                C.period_key,
                C.zonaobservacion ZONE_ID,
                C.origen VISITOR_ORIGIN_ID,
                C.categoriadelvisitante VISITOR_CATEGORY_ID,
                C.HORA HOUR_ID,
                C.Volumen KPI_VALUE_NUM,
                C.continuo,
                C.LOAD_DATE        
                FROM CONTINUO C
                LEFT JOIN CONSOLIDADO NC
                ON C.period_key = NC.period_key
                AND C.zonaobservacion = NC.zonaobservacion
                AND C.HORA = NC.HORA
                WHERE NC.period_key IS NULL
                )

                SELECT 
                period_key,
                ZONE_ID,
                VISITOR_ORIGIN_ID,
                VISITOR_CATEGORY_ID,
                HOUR_ID,
                KPI_VALUE_NUM,
                continuo,
                LOAD_DATE AS FECHA_CARGA,          
                row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,HOUR_ID ,KPI_VALUE_NUM order by LOAD_DATE desc) AS DUPLICATE_FILTER , 
                row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,HOUR_ID order by LOAD_DATE desc) AS HYBRID_FILTER ,       
                current_timestamp() LOAD_TIME
                FROM TOTAL
                );

                DECLARE
                COMPROBACION_INICIAL INT;
                BEGIN
                -- Comprobar si existen duplicados en la tabla AX_FLX_BY_AGE_PRE_ALL_TODAY
                SELECT COUNT(1) 
                INTO COMPROBACION_INICIAL
                FROM PROCESS.AX_FLX_2H_PRE_ALL_TODAY 
                WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

                -- Verificar el valor de la comprobación inicial
                IF (COMPROBACION_INICIAL > 0) THEN
                -- Registrar el problema en la tabla de logs
                INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
                SELECT ''AX_FLX_2H_PRE_ALL_TODAY'' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
                FROM PROCESS.AX_FLX_2H_PRE_ALL_TODAY 
                WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

                -- Finalizar el proceso con un mensaje indicando el error
                RETURN ''Proceso cancelado: Se detectaron duplicados en AX_FLX_2H_PRE_ALL_TODAY. Total: '' || COMPROBACION_INICIAL;
                ELSE
                    INSERT INTO PROCESS.AX_FLX_2H_ALL_TODAY
                    (
                    SELECT 
                            PERIOD_KEY,
                            ZONE_ID,
                            VISITOR_ORIGIN_ID,
                            VISITOR_CATEGORY_ID,
                            HOUR_ID,
                            KPI_VALUE_NUM,
                            current_timestamp() LOAD_TIME
                    FROM PROCESS.AX_FLX_2H_PRE_ALL_TODAY     
                    WHERE DUPLICATE_FILTER = 1
                    );

                    INSERT INTO PROCESS.AX_FLX_2H
                    (
                    select 
                        a.period_key,
                        nvl(z.ZONE_KEY,-3) ZONE_KEY,
                        nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                        nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                        nvl(HORA.HOUR_KEY,-3) HOUR_KEY,    
                        a.KPI_VALUE_NUM,
                        current_timestamp() LOAD_TIME   
                    from PROCESS.AX_FLX_2H_ALL_TODAY a
                    ,SIT.DM_FLX_ZONE z
                    ,SIT.DM_GLB_VISITOR_CATEGORY C
                    ,SIT.DM_FLX_VISITOR_ORIGIN O
                    ,SIT.DM_FLX_HOUR HORA

                    where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
                    and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
                    and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
                    and trim(a.HOUR_ID)=trim(HORA.HOUR_ID(+))

                    union all

                    select 
                        a.PERIOD_KEY, 
                        a.ZONE_KEY,
                        a.VISITOR_ORIGIN_KEY,
                        a.VISITOR_CATEGORY_KEY,
                        a.HOUR_KEY,
                        a.KPI_VALUE_NUM,
                        a.LOAD_TIME
                    from 
                        (select a.*,z.zone_id from 
                        PROCESS.FC_FLX_2H a,
                        SIT.DM_FLX_ZONE z 
                        where a.zone_key=z.zone_key) a,
                        
                        (select DISTINCT PERIOD_KEY,zone_ID from 
                        PROCESS.AX_FLX_2H_ALL_TODAY 
                        ) b
                        
                    where a.period_key=b.period_key(+) 
                    and trim(a.zone_id)=trim(b.zone_id(+))
                    and b.period_key is null
                    );

                    END IF;
                    END;


    ------------------------------------------------------------------

    INSERT INTO PROCESS.AX_FLX_BY_CITY_2H_PRE_ALL_TODAY
        (
        WITH CONSOLIDADO AS (
            select 
                to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                trim(zonaobservacion) AS zonaobservacion,
                trim(origen) AS origen,
                trim(categoriadelvisitante) AS categoriadelvisitante,
                trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                TRIM(HORA) AS HORA ,
                Volumen ,
                trim(continuo) continuo,
                trim(A.LOAD_DATE) LOAD_DATE
        from STAGING.ST_FLX_DIARIO_2H_MUNICIPIO a,
        (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_2H_MUNICIPIO) M	
            where a.load_date=m.load_date  
            AND (trim(CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
            AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''   
        ),

        CONTINUO AS (
            select 
                to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                trim(zonaobservacion) AS zonaobservacion,
                trim(origen) AS origen,
                trim(categoriadelvisitante) AS categoriadelvisitante,
                trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                TRIM(HORA) AS HORA ,
                Volumen ,
                trim(continuo) continuo,
                trim(A.LOAD_DATE) LOAD_DATE
        from STAGING.ST_FLX_DIARIO_2H_MUNICIPIO a,
        (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_2H_MUNICIPIO) M	
            where a.load_date=m.load_date  
            AND (trim(CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
            AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''
        ),

        TOTAL AS (
            SELECT 
                period_key,
                zonaobservacion ZONE_ID,
                origen VISITOR_ORIGIN_ID,
                categoriadelvisitante VISITOR_CATEGORY_ID,
                NOMBREMUNICIPIO CITY_DES,
                HORA HOUR_ID,
                Volumen KPI_VALUE_NUM,
                continuo,
                LOAD_DATE        
            FROM CONSOLIDADO
            
            UNION ALL
            
            SELECT 
                C.period_key,
                C.zonaobservacion ZONE_ID,
                C.origen VISITOR_ORIGIN_ID,
                C.categoriadelvisitante VISITOR_CATEGORY_ID,
                C.NOMBREMUNICIPIO CITY_DES,
                C.HORA HOUR_ID,
                C.Volumen KPI_VALUE_NUM,
                C.continuo,
                C.LOAD_DATE        
            FROM CONTINUO C
            LEFT JOIN CONSOLIDADO NC
            ON C.period_key = NC.period_key
            AND C.zonaobservacion = NC.zonaobservacion
            AND C.HORA = NC.HORA
            WHERE NC.period_key IS NULL
        )

        SELECT 
            period_key,
            ZONE_ID,
            VISITOR_ORIGIN_ID,
            VISITOR_CATEGORY_ID,
            CITY_DES,
            HOUR_ID,
            KPI_VALUE_NUM,
            continuo,
            LOAD_DATE AS FECHA_CARGA,     
            row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,HOUR_ID ,CITY_DES,KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER ,  
            row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,HOUR_ID ,CITY_DES order by LOAD_DATE DESC) AS HYBRID_FILTER ,  
            current_timestamp() LOAD_TIME
        FROM TOTAL
        );

         DECLARE
        COMPROBACION_INICIAL INT;
        BEGIN
        -- Comprobar si existen duplicados en la tabla AX_FLX_BY_AGE_PRE_ALL_TODAY
        SELECT COUNT(1) 
        INTO COMPROBACION_INICIAL
        FROM PROCESS.AX_FLX_BY_CITY_2H_PRE_ALL_TODAY 
        WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;
        
        -- Verificar el valor de la comprobación inicial
        IF (COMPROBACION_INICIAL > 0) THEN
            -- Registrar el problema en la tabla de logs
            INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
            SELECT ''AX_FLX_BY_CITY_2H_PRE_ALL_TODAY'' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
            FROM PROCESS.AX_FLX_BY_CITY_2H_PRE_ALL_TODAY 
            WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;
        
            -- Finalizar el proceso con un mensaje indicando el error
            RETURN ''Proceso cancelado: Se detectaron duplicados en AX_FLX_BY_CITY_2H_PRE_ALL_TODAY. Total: '' || COMPROBACION_INICIAL;
        ELSE
            INSERT INTO PROCESS.AX_FLX_BY_CITY_2H_ALL_TODAY (
            SELECT 
                CAST(PERIOD_KEY AS NUMBER(16,0)), -- Ajuste para evitar valores demasiado largos
                ZONE_ID,
                VISITOR_ORIGIN_ID,
                VISITOR_CATEGORY_ID,
                CITY_DES,
                HOUR_ID,
                TRUNC(KPI_VALUE_NUM, 2), -- Limitar a 2 decimales
                CURRENT_TIMESTAMP() LOAD_TIME
            FROM PROCESS.AX_FLX_BY_CITY_2H_PRE_ALL_TODAY     
            WHERE DUPLICATE_FILTER = 1
        );
        
                
        INSERT INTO PROCESS.AX_FLX_BY_CITY_2H (
            SELECT 
                CAST(a.PERIOD_KEY AS NUMBER(16,0)), -- Ajuste para evitar valores demasiado largos
                NVL(z.ZONE_KEY, -3) ZONE_KEY,
                NVL(o.VISITOR_ORIGIN_KEY, -3) VISITOR_ORIGIN_KEY,
                NVL(c.VISITOR_CATEGORY_KEY, -3) VISITOR_CATEGORY_KEY,
                NVL(CITY.CITY_KEY, -3) CITY_KEY,
                NVL(HORA.HOUR_KEY, -3) HOUR_KEY,    
                tra.ZIPCODE_ID,
                TRUNC(a.KPI_VALUE_NUM, 2), -- Limitar a 2 decimales
                CURRENT_TIMESTAMP() LOAD_TIME   
            FROM PROCESS.AX_FLX_BY_CITY_2H_ALL_TODAY a
            LEFT JOIN SIT.DM_FLX_ZONE z ON TRIM(a.ZONE_ID) = TRIM(z.ZONE_ID)
            LEFT JOIN SIT.DM_GLB_VISITOR_CATEGORY c ON a.VISITOR_CATEGORY_ID = c.VISITOR_CATEGORY_ID
            LEFT JOIN SIT.DM_FLX_VISITOR_ORIGIN o ON a.VISITOR_ORIGIN_ID = o.VISITOR_ORIGIN_ID
            LEFT JOIN SIT.DM_FLX_ZIPCODE_NORM tra ON a.CITY_DES = tra.CITY_DES
            LEFT JOIN SIT.DM_GLB_CITY CITY ON tra.CITY_ID = CITY.CITY_ID
            LEFT JOIN SIT.DM_FLX_HOUR HORA ON TRIM(a.HOUR_ID) = TRIM(HORA.HOUR_ID)
            WHERE a.CITY_DES != ''Acumulado''
            
            UNION ALL
            
            SELECT 
                CAST(a.PERIOD_KEY AS NUMBER(16,0)),
                a.ZONE_KEY,
                a.VISITOR_ORIGIN_KEY,
                a.VISITOR_CATEGORY_KEY,
                a.CITY_KEY,
                a.HOUR_KEY,
                a.ZIPCODE_ID,
                TRUNC(a.KPI_VALUE_NUM, 2),
                a.LOAD_TIME
            FROM (
                SELECT a.*, z.ZONE_ID 
                FROM SIT.FC_FLX_BY_CITY_2H a
                LEFT JOIN SIT.DM_FLX_ZONE z ON a.ZONE_KEY = z.ZONE_KEY
            ) a
            LEFT JOIN (
                SELECT DISTINCT PERIOD_KEY, ZONE_ID 
                FROM PROCESS.AX_FLX_BY_CITY_2H_ALL_TODAY
            ) b ON a.PERIOD_KEY = b.PERIOD_KEY AND TRIM(a.ZONE_ID) = TRIM(b.ZONE_ID)
            WHERE b.PERIOD_KEY IS NULL
        );

        END IF;
    END;


            INSERT INTO PROCESS.AX_FLX_BY_COUNTRY_2H_PRE_ALL_TODAY

            (
            WITH CONSOLIDADO AS (
                select 
                    to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    trim(pais) AS pais,
                    trim(HORA) AS HORA ,
                    Volumen ,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
            from STAGING.ST_FLX_DIARIO_2H_NACIONALIDAD a,
            (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_2H_NACIONALIDAD) M	
                where a.load_date=m.load_date  
                AND (trim(CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
                AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''   
            ),

            CONTINUO AS (
                select 
                    to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    trim(pais) AS pais,
                    trim(HORA) AS HORA ,
                    Volumen ,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
            from STAGING.ST_FLX_DIARIO_2H_NACIONALIDAD a,
            (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_2H_NACIONALIDAD) M	
                where a.load_date=m.load_date  
                AND (trim(CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
                AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''
            ),

            TOTAL AS (
                SELECT 
                    period_key,
                    zonaobservacion ZONE_ID,
                    origen VISITOR_ORIGIN_ID,
                    categoriadelvisitante VISITOR_CATEGORY_ID,
                    PAIS COUNTRY_DES,
                    HORA HOUR_ID,
                    Volumen KPI_VALUE_NUM,
                    continuo,
                    LOAD_DATE          
                FROM CONSOLIDADO
                
                UNION ALL
                
                SELECT 
                    C.period_key,
                    C.zonaobservacion ZONE_ID,
                    C.origen VISITOR_ORIGIN_ID,
                    C.categoriadelvisitante VISITOR_CATEGORY_ID,
                    C.PAIS COUNTRY_DES,
                    C.HORA HOUR_ID,
                    C.Volumen KPI_VALUE_NUM,
                    C.continuo,
                    C.LOAD_DATE            
                FROM CONTINUO C
                LEFT JOIN CONSOLIDADO NC
                ON C.period_key = NC.period_key
                AND C.zonaobservacion = NC.zonaobservacion
                AND C.HORA = NC.HORA
                WHERE NC.period_key IS NULL
            )

            SELECT 
                period_key,
                ZONE_ID,
                VISITOR_ORIGIN_ID,
                VISITOR_CATEGORY_ID,
                COUNTRY_DES,
                HOUR_ID,
                KPI_VALUE_NUM,
                continuo,
                LOAD_DATE AS FECHA_CARGA,     
                row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,COUNTRY_DES,HOUR_ID ,KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER ,  
                row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,COUNTRY_DES,HOUR_ID order by LOAD_DATE DESC) AS HYBRID_FILTER ,  
                current_timestamp() LOAD_TIME
            FROM TOTAL
            );

            DECLARE
            COMPROBACION_INICIAL INT;
            BEGIN
            -- Comprobar si existen duplicados en la tabla AX_FLX_BY_AGE_PRE_ALL_TODAY
            SELECT COUNT(1) 
            INTO COMPROBACION_INICIAL
            FROM PROCESS.AX_FLX_BY_COUNTRY_2H_PRE_ALL_TODAY 
            WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

            -- Verificar el valor de la comprobación inicial
            IF (COMPROBACION_INICIAL > 0) THEN
            -- Registrar el problema en la tabla de logs
            INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
            SELECT ''AX_FLX_BY_COUNTRY_2H_PRE_ALL_TODAY'' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
            FROM PROCESS.AX_FLX_BY_COUNTRY_2H_PRE_ALL_TODAY 
            WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

            -- Finalizar el proceso con un mensaje indicando el error
            RETURN ''Proceso cancelado: Se detectaron duplicados en AX_FLX_BY_COUNTRY_2H_PRE_ALL_TODAY. Total: '' || COMPROBACION_INICIAL;
            ELSE

                    INSERT INTO PROCESS.AX_FLX_BY_COUNTRY_2H_ALL_TODAY
                    (
                    SELECT 
                        PERIOD_KEY,
                        ZONE_ID,
                        VISITOR_ORIGIN_ID,
                        VISITOR_CATEGORY_ID,
                        COUNTRY_DES,
                        HOUR_ID,
                        KPI_VALUE_NUM,
                        current_timestamp() LOAD_TIME
                    FROM PROCESS.AX_FLX_BY_COUNTRY_2H_PRE_ALL_TODAY     
                    WHERE DUPLICATE_FILTER = 1
                    );

                    insert into PROCESS.RL_COUNTRY_TRANSLATOR (
                    select 
                        a.COUNTRY_DES RAW_COUNTRY_DES,
                        ''AX_FLX_BY_COUNTRY_2H_ALL_TODAY'' ORIGIN_DATA_ID,
                        nvl(max(b.country_des),''*********'') country_des,
                        nvl(max(b.country_id),''***'') country_id,
                        current_timestamp() LOAD_TIME 
                    from (select 
                            TRANSLATE(upper(a.COUNTRY_DES),''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’'', ''AEIOUAEIOUAEIOUO'') tra_country_des_JOIN,
                            country_DES 
                        from PROCESS.AX_FLX_BY_COUNTRY_2H_ALL_TODAY  a) a,
                        
                        (select DISTINCT 
                            TRANSLATE(upper(a.COUNTRY_DES),''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’'', ''AEIOUAEIOUAEIOUO'') COUNTRY_DES_JOIN,
                            a.country_des,
                            country_id 
                        from  SIT.DM_GLB_COUNTRY_MLG a, SIT.DM_GLB_COUNTRY b 
                        where a.country_key=b.country_key ) b, 
                    
                        PROCESS.RL_COUNTRY_TRANSLATOR tra
                    where a.tra_country_des_JOIN=b.COUNTRY_DES_JOIN(+)
                    and a.COUNTRY_DES=tra.RAW_COUNTRY_DES(+)
                    and tra.RAW_COUNTRY_DES is null
                    group by a.COUNTRY_DES
                    );

                    INSERT INTO PROCESS.AX_FLX_BY_COUNTRY_2H
                    (
                    select 
                        a.period_key,
                        nvl(z.ZONE_KEY,-3) ZONE_KEY,
                        nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                        nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                        nvl(country.country_key,-3) country_key,
                        nvl(HOUR_KEY,-3) HOUR_KEY,
                        a.KPI_VALUE_NUM,
                        current_timestamp() LOAD_TIME
                    from PROCESS.AX_FLX_BY_COUNTRY_2H_ALL_TODAY a
                    ,SIT.DM_FLX_ZONE z
                    ,SIT.DM_GLB_VISITOR_CATEGORY C
                    ,SIT.DM_FLX_VISITOR_ORIGIN O
                    ,PROCESS.RL_COUNTRY_TRANSLATOR TRA
                    ,SIT.DM_GLB_COUNTRY country
                    ,SIT.DM_FLX_HOUR HORA
                    where 
                    a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
                    and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
                    and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
                    and a.country_des=tra.raw_country_des(+)
                    and tra.country_id=country.country_id(+)
                    and trim(a.HOUR_ID)=trim(HORA.HOUR_ID(+))
                    
                    union all
                    
                    select 
                        a.PERIOD_KEY, 
                        a.ZONE_KEY,
                        a.VISITOR_ORIGIN_KEY,
                        a.VISITOR_CATEGORY_KEY,
                        a.COUNTRY_KEY,
                        a.HOUR_KEY,
                        a.KPI_VALUE_NUM,
                        a.LOAD_TIME
                    from (
                        select a.*,z.zone_id 
                        from SIT.FC_FLX_BY_COUNTRY_2H a,SIT.DM_FLX_ZONE z 
                        where a.zone_key=z.zone_key) a,
                        
                        (select DISTINCT 
                            PERIOD_KEY,
                            zone_ID 
                        from  PROCESS.AX_FLX_BY_COUNTRY_2H_ALL_TODAY) b
                    where a.period_key=b.period_key(+) 
                    and trim(a.zone_id)=trim(b.zone_id(+))
                    and b.period_key is null
                    );

                    END IF;
                    END;

            END;
        RETURN ''Task LOAD_AX_FLX_2H creada correctamente'';
    END;
';
