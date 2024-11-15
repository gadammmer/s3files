CREATE or REPLACE TABLE PROCESS.AX_FLX_REC_BY_CITY_PRE_ALL_TODAY
(
  PERIOD_KEY NUMBER (16,0) ,
  ZONE_ID  VARCHAR(100) ,
  VISITOR_ORIGIN_ID  VARCHAR(20) ,
  VISITOR_CATEGORY_ID  VARCHAR(100) ,
  CITY_DES VARCHAR(200),
  RECURRENCE_NUM NUMBER(10,0) ,
  KPI_VALUE_NUM FLOAT,
  CONTINUO  VARCHAR(10) ,
  FECHA_CARGA DATE,
  DUPLICATE_FILTER  NUMBER (16,0) ,    
  HYBRID_FILTER  NUMBER (16,0) ,  
  LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
);

INSERT INTO PROCESS.AX_FLX_REC_BY_CITY_PRE_ALL_TODAY
(
WITH CONSOLIDADO AS (
    select 
		to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
		recurrencia ,
		Volumen ,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
from STAGING.ST_FLX_RECURRENCIA_MUNICIPIO a,
(select DISTINCT LOAD_DATE from STAGING.ST_FLX_RECURRENCIA_MUNICIPIO) M	
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
),

CONTINUO AS (
    select 
		to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
		recurrencia ,
		Volumen ,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
from STAGING.ST_FLX_RECURRENCIA_MUNICIPIO a,
(select DISTINCT LOAD_DATE from STAGING.ST_FLX_RECURRENCIA_MUNICIPIO) M	
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
),

TOTAL AS (
    SELECT 
        period_key,
        zonaobservacion ZONE_ID,
        origen VISITOR_ORIGIN_ID,
        categoriadelvisitante VISITOR_CATEGORY_ID,
        NOMBREMUNICIPIO CITY_DES,
        recurrencia RECURRENCE_NUM,
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
        C.recurrencia RECURRENCE_NUM,
        C.Volumen KPI_VALUE_NUM,
        C.continuo,
        C.LOAD_DATE          
    FROM CONTINUO C
    LEFT JOIN CONSOLIDADO NC
    ON C.period_key = NC.period_key
    AND C.zonaobservacion = NC.zonaobservacion
    WHERE NC.period_key IS NULL
)

SELECT 
     period_key,
     ZONE_ID,
     VISITOR_ORIGIN_ID,
     VISITOR_CATEGORY_ID,
     CITY_DES,
     RECURRENCE_NUM,
     KPI_VALUE_NUM,
     CONTINUO,
     LOAD_DATE AS FECHA_CARGA,     
     row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,CITY_DES, RECURRENCE_NUM , KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER ,  
     row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,CITY_DES, RECURRENCE_NUM  order by LOAD_DATE DESC) AS HYBRID_FILTER ,  
     current_timestamp() LOAD_TIME
FROM TOTAL    
);
/*
DECLARE
COMPROBACION_INICIAL INT;
BEGIN
-- Comprobar si existen duplicados en la tabla AX_FLX_BY_AGE_PRE_ALL_TODAY
SELECT COUNT(1) 
INTO COMPROBACION_INICIAL
FROM PROCESS.AX_FLX_REC_BY_CITY_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

-- Verificar el valor de la comprobación inicial
IF (COMPROBACION_INICIAL > 0) THEN
-- Registrar el problema en la tabla de logs
INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
SELECT 'AX_FLX_REC_BY_CITY_PRE_ALL_TODAY' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
FROM PROCESS.AX_FLX_REC_BY_CITY_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

-- Finalizar el proceso con un mensaje indicando el error
RETURN 'Proceso cancelado: Se detectaron duplicados en AX_FLX_REC_BY_CITY_PRE_ALL_TODAY. Total: ' || COMPROBACION_INICIAL;
ELSE
*/
            CREATE or REPLACE TABLE PROCESS.AX_FLX_REC_BY_CITY_ALL_TODAY
        (
          PERIOD_KEY NUMBER (16,0) ,
          ZONE_ID  VARCHAR(100) ,
          VISITOR_ORIGIN_ID  VARCHAR(20) ,
          VISITOR_CATEGORY_ID  VARCHAR(100) ,
          CITY_DES VARCHAR(200),
          RECURRENCE_NUM NUMBER(10,0) ,
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
          );
        
        INSERT INTO PROCESS.AX_FLX_REC_BY_CITY_ALL_TODAY
        (
        select 
             period_key,
             ZONE_ID,
             VISITOR_ORIGIN_ID,
             VISITOR_CATEGORY_ID,
             CITY_DES,
             RECURRENCE_NUM,
             KPI_VALUE_NUM,
             current_timestamp() LOAD_TIME
        FROM PROCESS.AX_FLX_REC_BY_CITY_PRE_ALL_TODAY
        WHERE DUPLICATE_FILTER = 1
        );
        
        CREATE or REPLACE TABLE PROCESS.AX_FLX_REC_BY_CITY
        (
          PERIOD_KEY       NUMBER (16,0) ,
          ZONE_KEY  NUMBER (16,0) ,
          VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
          VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
          CITY_KEY NUMBER (16,0),
          RECURRENCE_NUM NUMBER (10,0),
          ZIPCODE_ID FLOAT,
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
        );
        
        INSERT INTO PROCESS.AX_FLX_REC_BY_CITY
        (
        select 
            a.period_key,
            nvl(z.ZONE_KEY,-3) ZONE_KEY,
            nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
            nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
            nvl(CITY.CITY_key,-3) CITY_key,
            RECURRENCE_NUM,tra.ZIPCODE_ID
            ,a.KPI_VALUE_NUM,
            current_timestamp() LOAD_TIME
        from PROCESS.AX_FLX_REC_BY_CITY_ALL_TODAY a
        ,SIT.DM_FLX_ZONE z
        ,SIT.DM_GLB_VISITOR_CATEGORY C
        ,SIT.DM_FLX_VISITOR_ORIGIN O
        ,SIT.DM_GLB_CITY CITY
        ,SIT.DM_FLX_ZIPCODE_NORM tra
        
        where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
        and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
        and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
        and a.city_des=tra.city_des(+)
        and tra.city_id=CITY.city_id(+)
        and a.city_des !='Acumulado'
        
        union all
        
        select 
            a.PERIOD_KEY, 
            a.ZONE_KEY,
            a.VISITOR_ORIGIN_KEY,
            a.VISITOR_CATEGORY_KEY,
            a.city_key, 
            a.recurrence_num, 
            a.ZIPCODE_ID,
            a.KPI_VALUE_NUM,
            a.LOAD_TIME
        
        from (select a.*,z.zone_id 
                from SIT.FC_FLX_REC_BY_CITY a,
                SIT.DM_FLX_ZONE z 
                where a.zone_key=z.zone_key) a
                
        ,(select distinct PERIOD_KEY,zone_ID 
            from PROCESS.AX_FLX_REC_BY_CITY_ALL_TODAY) b
        where a.period_key=b.period_key(+) 
        and trim(a.zone_id)=trim(b.zone_id(+))
        and b.period_key is null
        );
        /*
    END IF;
    END;

    DECLARE
    COMPROBACION_FINAL INT;
    BEGIN
    -- Inicializar la variable COMPROBACION_INICIAL
    SELECT COUNT(1) 
    INTO COMPROBACION_FINAL
    FROM (
    select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY                                   
    from PROCESS.AX_FLX_REC_BY_CITY 
    group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
    having count(1) > 1);

    IF (COMPROBACION_FINAL > 0) THEN

    INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
    SELECT 'Hay duplicados en AX_FLX_REC_BY_CITY' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
    select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY 
    from PROCESS.AX_FLX_REC_BY_CITY
    group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
    having count(1) > 1);

    -- Mensaje del return
    RETURN 'Proceso cancelado: Hay duplicados en AX_FLX_REC_BY_CITY: ' || COMPROBACION_FINAL;	
    ELSE
    */
        CREATE or REPLACE TABLE SIT.FC_FLX_REC_BY_CITY
        (
          PERIOD_KEY       NUMBER (16,0) ,
          ZONE_KEY  NUMBER (16,0) ,
          VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
          VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
          CITY_KEY NUMBER (16,0),
          RECURRENCE_NUM NUMBER (10,0),
          ZIPCODE_ID VARCHAR(10),
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
        );
        
        INSERT INTO SIT.FC_FLX_REC_BY_CITY 
        (
        select 
            a.period_key,
            a.zone_key,
            a.visitor_origin_key,
            a.visitor_category_key,
            a.CITY_key, 
            a.RECURRENCE_NUM,
            nvl(A.ZIPCODE_ID,'00000') ZIPCODE_ID,
            a.KPI_VALUE_NUM,
            LOAD_TIME
        from PROCESS.AX_FLX_REC_BY_CITY a
        where city_key>0
        );
        
        RETURN 'Proceso finalizado correctamente.';
/*
        END IF;
		
END;
*/

CREATE or REPLACE TABLE PROCESS.AX_FLX_REC_BY_COUNTRY_PRE_ALL_TODAY
(
  PERIOD_KEY NUMBER (16,0) ,
  ZONE_ID  VARCHAR(100) ,
  VISITOR_ORIGIN_ID  VARCHAR(20 CHAR) ,
  VISITOR_CATEGORY_ID  VARCHAR(100) ,
  COUNTRY_DES VARCHAR(75 CHAR),
  RECURRENCE_NUM NUMBER(10,0),
  KPI_VALUE_NUM FLOAT,
  CONTINUO  VARCHAR(10) ,
  FECHA_CARGA DATE,
  DUPLICATE_FILTER  NUMBER (16,0) ,    
  HYBRID_FILTER  NUMBER (16,0) ,  
  LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
);

INSERT INTO PROCESS.AX_FLX_REC_BY_COUNTRY_PRE_ALL_TODAY

(
WITH CONSOLIDADO AS (
    select 
		to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
		trim(pais) as pais ,
		recurrencia ,
		Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
from STAGING.ST_FLX_RECURRENCIA_NACIONALIDAD a,
(select DISTINCT LOAD_DATE from STAGING.ST_FLX_RECURRENCIA_NACIONALIDAD) M	
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
),

CONTINUO AS (
    select 
		to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
		trim(pais) as pais ,
		recurrencia ,
		Volumen ,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
from STAGING.ST_FLX_RECURRENCIA_NACIONALIDAD a,
(select DISTINCT LOAD_DATE from STAGING.ST_FLX_RECURRENCIA_NACIONALIDAD) M	
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
),

TOTAL AS (
    SELECT 
        period_key,
        zonaobservacion ZONE_ID,
        origen VISITOR_ORIGIN_ID,
        categoriadelvisitante VISITOR_CATEGORY_ID,
        PAIS COUNTRY_DES,
        recurrencia RECURRENCE_NUM,
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
        C.recurrencia RECURRENCE_NUM,
        C.Volumen KPI_VALUE_NUM,
        C.continuo,
        C.LOAD_DATE  
    FROM CONTINUO C
    LEFT JOIN CONSOLIDADO NC
    ON C.period_key = NC.period_key
    AND C.zonaobservacion = NC.zonaobservacion
    WHERE NC.period_key IS NULL
)

SELECT 
     period_key,
     ZONE_ID,
     VISITOR_ORIGIN_ID,
     VISITOR_CATEGORY_ID,
     COUNTRY_DES,
     RECURRENCE_NUM,
     KPI_VALUE_NUM, 
     CONTINUO,
     LOAD_DATE AS FECHA_CARGA,     
     row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,COUNTRY_DES,RECURRENCE_NUM, KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER ,  
     row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,COUNTRY_DES,RECURRENCE_NUM  order by LOAD_DATE DESC) AS HYBRID_FILTER ,  
     current_timestamp() LOAD_TIME
FROM TOTAL    
);
/*
DECLARE
COMPROBACION_INICIAL INT;
BEGIN
-- Comprobar si existen duplicados en la tabla AX_FLX_BY_AGE_PRE_ALL_TODAY
SELECT COUNT(1) 
INTO COMPROBACION_INICIAL
FROM PROCESS.AX_FLX_REC_BY_COUNTRY_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

-- Verificar el valor de la comprobación inicial
IF (COMPROBACION_INICIAL > 0) THEN
-- Registrar el problema en la tabla de logs
INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
SELECT 'AX_FLX_REC_BY_COUNTRY_PRE_ALL_TODAY' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
FROM PROCESS.AX_FLX_REC_BY_COUNTRY_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

-- Finalizar el proceso con un mensaje indicando el error
RETURN 'Proceso cancelado: Se detectaron duplicados en AX_FLX_REC_BY_COUNTRY_PRE_ALL_TODAY. Total: ' || COMPROBACION_INICIAL;
ELSE
*/
            CREATE or REPLACE TABLE PROCESS.AX_FLX_REC_BY_COUNTRY_ALL_TODAY
        (
          PERIOD_KEY NUMBER (16,0) ,
          ZONE_ID  VARCHAR(100) ,
          VISITOR_ORIGIN_ID  VARCHAR(20 CHAR) ,
          VISITOR_CATEGORY_ID  VARCHAR(100) ,
          COUNTRY_DES VARCHAR(75 CHAR),
          RECURRENCE_NUM NUMBER(10,0),
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
          );
        
        INSERT INTO PROCESS.AX_FLX_REC_BY_COUNTRY_ALL_TODAY
        (
        SELECT 
             period_key,
             ZONE_ID,
             VISITOR_ORIGIN_ID,
             VISITOR_CATEGORY_ID,
             COUNTRY_DES,
             RECURRENCE_NUM,
             KPI_VALUE_NUM,
             current_timestamp() LOAD_TIME
        FROM PROCESS.AX_FLX_REC_BY_COUNTRY_PRE_ALL_TODAY
        WHERE DUPLICATE_FILTER = 1
        );


        insert into PROCESS.RL_COUNTRY_TRANSLATOR (
        select 
            a.COUNTRY_DES RAW_COUNTRY_DES,
            'AX_FLX_REC_BY_COUNTRY_ALL_TODAY' ORIGIN_DATA_ID,
            nvl(max(b.country_des),'*********') country_des,
            nvl(max(b.country_id),'***') country_id,
            current_timestamp() LOAD_TIME 
        from (select 
                  TRANSLATE(upper(a.COUNTRY_DES),'ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’', 'AEIOUAEIOUAEIOUO') tra_country_des_JOIN,
                  country_DES 
              from PROCESS.AX_FLX_REC_BY_COUNTRY_ALL_TODAY  a) a,
            
            (select distinct 
                TRANSLATE(upper(a.COUNTRY_DES),'ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’', 'AEIOUAEIOUAEIOUO') COUNTRY_DES_JOIN,
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
        
        
        CREATE or REPLACE TABLE PROCESS.AX_FLX_REC_BY_COUNTRY
        (
          PERIOD_KEY       NUMBER (16,0) ,
          ZONE_KEY  NUMBER (16,0) ,
          VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
          VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
          COUNTRY_KEY NUMBER (16,0),
          RECURRENCE_NUM NUMBER(10,0),
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
        );
        
        INSERT INTO PROCESS.AX_FLX_REC_BY_COUNTRY
        (
        select 
            a.period_key,
            nvl(z.ZONE_KEY,-3) ZONE_KEY,
            nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
            nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
            nvl(country.country_key,-3) country_key,
            a.RECURRENCE_NUM
            ,a.KPI_VALUE_NUM,current_timestamp() LOAD_TIME
        from PROCESS.AX_FLX_REC_BY_COUNTRY_ALL_TODAY a
            ,SIT.DM_FLX_ZONE z
            ,SIT.DM_GLB_VISITOR_CATEGORY C
            ,SIT.DM_FLX_VISITOR_ORIGIN O
            ,PROCESS.RL_COUNTRY_TRANSLATOR TRA
            ,SIT.DM_GLB_COUNTRY country
            
        where  a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
        and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
        and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
        and a.country_des=tra.raw_country_des(+)
        and tra.country_id=country.country_id(+)
        
        union all
        
        select 
            a.PERIOD_KEY, 
            a.ZONE_KEY,
            a.VISITOR_ORIGIN_KEY,
            a.VISITOR_CATEGORY_KEY,
            a.COUNTRY_KEY,
            a.recurrence_num,
            a.KPI_VALUE_NUM,
            a.LOAD_TIME
        from (select 
                a.*,
                z.zone_id
            from SIT.FC_FLX_REC_BY_COUNTRY a,
            SIT.DM_FLX_ZONE z
            where a.zone_key=z.zone_key) a
        ,(select distinct 
            PERIOD_KEY,
            zone_ID 
        from  PROCESS.AX_FLX_REC_BY_COUNTRY_ALL_TODAY) b
        where a.period_key=b.period_key(+) 
        and trim(a.zone_id)=trim(b.zone_id(+))
        and b.period_key is null
        );
/*
    END IF;
    END;

DECLARE
COMPROBACION_FINAL INT;
BEGIN
-- Inicializar la variable COMPROBACION_INICIAL
SELECT COUNT(1) 
INTO COMPROBACION_FINAL
FROM (
select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY                                     
from PROCESS.AX_FLX_REC_BY_COUNTRY 
group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY 
having count(1) > 1);

IF (COMPROBACION_FINAL > 0) THEN

INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
SELECT 'Hay duplicados en AX_FLX_REC_BY_COUNTRY' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
from PROCESS.AX_FLX_REC_BY_COUNTRY
group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY 
having count(1) > 1);

-- Mensaje del return
RETURN 'Proceso cancelado: Hay duplicados en AX_FLX_REC_BY_COUNTRY: ' || COMPROBACION_FINAL;	
ELSE
  */
            CREATE or REPLACE TABLE SIT.FC_FLX_REC_BY_COUNTRY
        (
          PERIOD_KEY       NUMBER (16,0) ,
          ZONE_KEY  NUMBER (16,0) ,
          VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
          VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
          COUNTRY_KEY NUMBER (16,0),
          RECURRENCE_NUM NUMBER(10,0),
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
        );
        
        INSERT INTO SIT.FC_FLX_REC_BY_COUNTRY 
        (
        select 
            a.period_key,
            a.zone_key,
            a.visitor_origin_key,
            a.visitor_category_key,
            a.COUNTRY_key,
            RECURRENCE_NUM,
            a.KPI_VALUE_NUM,
            LOAD_TIME
        from PROCESS.AX_FLX_REC_BY_COUNTRY a
        where country_key>0
        );
        
        RETURN 'Proceso finalizado correctamente.';
/*
        END IF;
		
END;
*/
CREATE or REPLACE TABLE PROCESS.AX_FLX_REC_PRE_ALL_TODAY
(
  PERIOD_KEY NUMBER (16,0) ,
  ZONE_ID  VARCHAR(100) ,
  VISITOR_ORIGIN_ID  VARCHAR(20) ,
  VISITOR_CATEGORY_ID  VARCHAR(100) ,
  RECURRENCE_NUM NUMBER(10,0) ,
  KPI_VALUE_NUM FLOAT,
  CONTINUO  VARCHAR(10) ,
  FECHA_CARGA DATE,
  DUPLICATE_FILTER  NUMBER (16,0) ,    
  HYBRID_FILTER  NUMBER (16,0) ,  
  LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
);

INSERT INTO PROCESS.AX_FLX_REC_PRE_ALL_TODAY
(
WITH CONSOLIDADO AS (
    select 
		to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
		recurrencia ,
		Volumen  ,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
from DEV_TOURISM_DB.STAGING.ST_FLX_RECURRENCIA a,
(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_RECURRENCIA ) M		
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
),

CONTINUO AS (
    select 
		to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
		recurrencia ,
		Volumen ,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
from DEV_TOURISM_DB.STAGING.ST_FLX_RECURRENCIA a,
(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_RECURRENCIA ) M		
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
),

TOTAL AS (
    SELECT 
        period_key,
        zonaobservacion ZONE_ID,
        origen VISITOR_ORIGIN_ID,
        categoriadelvisitante VISITOR_CATEGORY_ID,
        recurrencia RECURRENCE_NUM,
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
        C.recurrencia RECURRENCE_NUM,
        C.Volumen KPI_VALUE_NUM,
        C.continuo,
        C.LOAD_DATE           
    FROM CONTINUO C
    LEFT JOIN CONSOLIDADO NC
    ON C.period_key = NC.period_key
    AND C.zonaobservacion = NC.zonaobservacion
    WHERE NC.period_key IS NULL
)

SELECT 
     period_key,
     ZONE_ID,
     VISITOR_ORIGIN_ID,
     VISITOR_CATEGORY_ID,
     RECURRENCE_NUM,
     KPI_VALUE_NUM,  
     CONTINUO,
     LOAD_DATE AS FECHA_CARGA,         
     row_number() over (partition by period_key, ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,RECURRENCE_NUM ,KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER ,  
     row_number() over (partition by period_key, ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,RECURRENCE_NUM  order by LOAD_DATE DESC) AS HYBRID_FILTER ,  
     current_timestamp() LOAD_TIME
FROM TOTAL    
);
/*
DECLARE
COMPROBACION_INICIAL INT;
BEGIN
-- Comprobar si existen duplicados en la tabla AX_FLX_BY_AGE_PRE_ALL_TODAY
SELECT COUNT(1) 
INTO COMPROBACION_INICIAL
FROM PROCESS.AX_FLX_REC_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

-- Verificar el valor de la comprobación inicial
IF (COMPROBACION_INICIAL > 0) THEN
-- Registrar el problema en la tabla de logs
INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
SELECT 'AX_FLX_REC_PRE_ALL_TODAY' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
FROM PROCESS.AX_FLX_REC_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

-- Finalizar el proceso con un mensaje indicando el error
RETURN 'Proceso cancelado: Se detectaron duplicados en AX_FLX_REC_PRE_ALL_TODAY. Total: ' || COMPROBACION_INICIAL;
ELSE
*/
            CREATE or REPLACE TABLE PROCESS.AX_FLX_REC_ALL_TODAY
        (
          PERIOD_KEY NUMBER (16,0) ,
          ZONE_ID  VARCHAR(100) ,
          VISITOR_ORIGIN_ID  VARCHAR(20) ,
          VISITOR_CATEGORY_ID  VARCHAR(100) ,
          RECURRENCE_NUM NUMBER(10,0) ,
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
          );
        
    
        INSERT INTO PROCESS.AX_FLX_REC_ALL_TODAY
        (
        SELECT 
             period_key,
             ZONE_ID,
             VISITOR_ORIGIN_ID,
             VISITOR_CATEGORY_ID,
             RECURRENCE_NUM,
             KPI_VALUE_NUM,
             current_timestamp() LOAD_TIME
        FROM PROCESS.AX_FLX_REC_PRE_ALL_TODAY
        WHERE DUPLICATE_FILTER = 1
        );
        
        CREATE or REPLACE TABLE PROCESS.AX_FLX_REC
        (
          PERIOD_KEY NUMBER (16,0) ,
          ZONE_KEY  NUMBER (16,0) ,
          VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
          VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
          RECURRENCE_NUM NUMBER (10,0),
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
        );
        
        INSERT INTO PROCESS.AX_FLX_REC
        (
        select 
            a.period_key,
            nvl(z.ZONE_KEY,-3) ZONE_KEY,
            nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
            nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
            a.RECURRENCE_NUM,
            a.KPI_VALUE_NUM,
            current_timestamp() LOAD_TIME
            
        from PROCESS.AX_FLX_REC_ALL_TODAY a
        ,SIT.DM_FLX_ZONE z
        ,SIT.DM_GLB_VISITOR_CATEGORY C
        ,SIT.DM_FLX_VISITOR_ORIGIN O
        
        where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
        and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
        and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
        
        union all
        
        select 
            a.PERIOD_KEY, 
            a.ZONE_KEY,
            a.VISITOR_ORIGIN_KEY,
            a.VISITOR_CATEGORY_KEY, 
            a.recurrence_num, 
            a.KPI_VALUE_NUM,
            a.LOAD_TIME
        
        from (select a.*,z.zone_id 
                from SIT.FC_FLX_REC a,
                SIT.DM_FLX_ZONE z 
                where a.zone_key=z.zone_key) a
                
        ,(select distinct PERIOD_KEY,zone_ID 
            from PROCESS.AX_FLX_REC_ALL_TODAY) b
        where a.period_key=b.period_key(+) 
        and trim(a.zone_id)=trim(b.zone_id(+))
        and b.period_key is null
        );
/*
    END IF;
    END;

    DECLARE
    COMPROBACION_FINAL INT;
    BEGIN
    -- Inicializar la variable COMPROBACION_INICIAL
    SELECT COUNT(1) 
    INTO COMPROBACION_FINAL
    FROM (
    select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY                                 
    from PROCESS.AX_FLX_REC 
    group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
    having count(1) > 1);

    IF (COMPROBACION_FINAL > 0) THEN

    INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
    SELECT 'Hay duplicados en AX_FLX_REC' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
    select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
    from PROCESS.AX_FLX_REC
    group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
    having count(1) > 1);

    -- Mensaje del return
    RETURN 'Proceso cancelado: Hay duplicados en AX_FLX_REC: ' || COMPROBACION_FINAL;	
    ELSE
    */
                CREATE or REPLACE TABLE SIT.FC_FLX_REC
        (
          PERIOD_KEY NUMBER (16,0) ,
          ZONE_KEY  NUMBER (16,0) ,
          VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
          VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
          RECURRENCE_NUM NUMBER (10,0),
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
        );
        
        INSERT INTO SIT.FC_FLX_REC
        (
        select 
            a.period_key,
            a.zone_key,
            a.visitor_origin_key,
            a.visitor_category_key,
            a.RECURRENCE_NUM,
            a.KPI_VALUE_NUM,
            LOAD_TIME
        from PROCESS.AX_FLX_REC a
        );
        
        RETURN 'Proceso finalizado correctamente.';
/*
        END IF;
        		
END;

*/


