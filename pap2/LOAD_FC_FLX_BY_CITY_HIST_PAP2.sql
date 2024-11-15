CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_CITY_MONTH_PRE_ALL_TODAY
(
  YEAR_KEY NUMBER (10,0) ,
  MONTH_KEY NUMBER (10,0) ,
  ZONE_ID  VARCHAR(100) ,
  VISITOR_ORIGIN_ID  VARCHAR(20 CHAR) ,
  VISITOR_CATEGORY_ID  VARCHAR(100) ,
  CITY_DES VARCHAR(200 CHAR),
  KPI_ID  VARCHAR(100) ,
  KPI_VALUE_NUM FLOAT,
  CONTINUO  VARCHAR(10) ,
  FECHA_CARGA DATE,
  DUPLICATE_FILTER  NUMBER (16,0) ,    
  HYBRID_FILTER  NUMBER (16,0) ,  
  LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
  );

INSERT INTO PROCESS.AX_FLX_BY_CITY_MONTH_PRE_ALL_TODAY
(
WITH CONSOLIDADO AS (
    select 
        TRIM(MES) AS MES,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) AS NOMBREMUNICIPIO,
        'Diario' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE    
    from STAGING.ST_FLX_DIARIO_MES_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_DIARIO_MES_MUNICIPIO) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
 
     union all
     
      select 
        TRIM(MES) AS MES,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) AS NOMBREMUNICIPIO,
        'Nocturno' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_NOCTURNO_MES_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_NOCTURNO_MES_MUNICIPIO) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
),

CONTINUO AS (
    select 
        TRIM(MES) AS MES,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) AS NOMBREMUNICIPIO,
        'Diario' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    
    from STAGING.ST_FLX_DIARIO_MES_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_DIARIO_MES_MUNICIPIO) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
    
     union all
     
      select 
        TRIM(MES) AS MES,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) AS NOMBREMUNICIPIO,
        'Nocturno' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_NOCTURNO_MES_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_NOCTURNO_MES_MUNICIPIO) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
),

TOTAL AS (
    SELECT 
        CAST(SUBSTRING(MES, 1, 4) AS INTEGER) YEAR_KEY,
        CAST(SUBSTRING(MES, 5, 2) AS INTEGER) MONTH_KEY,
        zonaobservacion ZONE_ID,
        origen VISITOR_ORIGIN_ID,
        categoriadelvisitante VISITOR_CATEGORY_ID,
        NOMBREMUNICIPIO  CITY_DES,
        KPI_ID,
        Volumen KPI_VALUE_NUM,
        continuo,
        LOAD_DATE             
    FROM CONSOLIDADO
    
    UNION ALL
    
    SELECT 
        CAST(SUBSTRING(C.MES, 1, 4) AS INTEGER) YEAR_KEY,
        CAST(SUBSTRING(C.MES, 5, 2) AS INTEGER) MONTH_KEY,
        C.zonaobservacion ZONE_ID,
        C.origen VISITOR_ORIGIN_ID,
        C.categoriadelvisitante VISITOR_CATEGORY_ID,
        C.NOMBREMUNICIPIO  CITY_DES,
        C.KPI_ID,
        C.Volumen KPI_VALUE_NUM,
        C.continuo,
        C.LOAD_DATE            
    FROM CONTINUO C
    LEFT JOIN CONSOLIDADO NC
    ON C.MES = NC.MES
    AND C.zonaobservacion = NC.zonaobservacion
    AND C.KPI_ID = NC.KPI_ID
    WHERE NC.MES IS NULL
)

SELECT 
     YEAR_KEY,
     MONTH_KEY,
     ZONE_ID,
     VISITOR_ORIGIN_ID,
     VISITOR_CATEGORY_ID,
     CITY_DES,
     KPI_ID,
     KPI_VALUE_NUM,
     continuo,
     LOAD_DATE AS FECHA_CARGA,     
     row_number() over (partition by YEAR_KEY, MONTH_KEY,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,CITY_DES ,KPI_ID ,KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER ,  
     row_number() over (partition by YEAR_KEY, MONTH_KEY,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,CITY_DES ,KPI_ID  order by LOAD_DATE DESC) AS HYBRID_FILTER ,  
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
FROM PROCESS.AX_FLX_BY_CITY_MONTH_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

-- Verificar el valor de la comprobación inicial
IF (COMPROBACION_INICIAL > 0) THEN
-- Registrar el problema en la tabla de logs
INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
SELECT 'AX_FLX_BY_CITY_MONTH_PRE_ALL_TODAY' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
FROM PROCESS.AX_FLX_BY_CITY_MONTH_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

-- Finalizar el proceso con un mensaje indicando el error
RETURN 'Proceso cancelado: Se detectaron duplicados en AX_FLX_BY_CITY_MONTH_PRE_ALL_TODAY. Total: ' || COMPROBACION_INICIAL;
ELSE
*/
    CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_CITY_MONTH_ALL_TODAY
    (
      YEAR_KEY NUMBER (10,0) ,
      MONTH_KEY NUMBER (10,0) ,
      ZONE_ID  VARCHAR(100) ,
      VISITOR_ORIGIN_ID  VARCHAR(20 CHAR) ,
      VISITOR_CATEGORY_ID  VARCHAR(100) ,
      CITY_DES VARCHAR(200 CHAR),
      KPI_ID  VARCHAR(100) ,
      KPI_VALUE_NUM FLOAT,
      LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
      );
    
    INSERT INTO PROCESS.AX_FLX_BY_CITY_MONTH_ALL_TODAY
    (
    SELECT 
         YEAR_KEY,
         MONTH_KEY,
         ZONE_ID,
         VISITOR_ORIGIN_ID,
         VISITOR_CATEGORY_ID,
         CITY_DES,
         KPI_ID,
         KPI_VALUE_NUM,
         current_timestamp() LOAD_TIME
    FROM PROCESS.AX_FLX_BY_CITY_MONTH_PRE_ALL_TODAY     
    WHERE DUPLICATE_FILTER = 1
    );

    CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_CITY_MONTH
    (
      YEAR_KEY NUMBER (10,0) ,
      MONTH_KEY NUMBER (10,0) ,
      ZONE_KEY  NUMBER (16,0),
      VISITOR_ORIGIN_KEY  NUMBER (16,0),
      VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
      CITY_KEY NUMBER (16,0),
      ZIPCODE_ID VARCHAR(10),
      KPI_KEY NUMBER (16,0),
      KPI_VALUE_NUM FLOAT,
      LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
    );
    
    INSERT INTO PROCESS.AX_FLX_BY_CITY_MONTH
    (
    select 
        a.YEAR_KEY,
        a.MONTH_KEY,
        nvl(z.ZONE_KEY,-3) ZONE_KEY,
        nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
        nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
        nvl(CITY.CITY_key,-3) CITY_key,    
        tra.ZIPCODE_ID,
        nvl(KPI_KEY,-3) KPI_KEY,
        a.KPI_VALUE_NUM,
        current_timestamp() LOAD_TIME   
    from PROCESS.AX_FLX_BY_CITY_MONTH_ALL_TODAY a
    ,SIT.DM_FLX_ZONE z
    ,SIT.DM_GLB_VISITOR_CATEGORY C
    ,SIT.DM_FLX_VISITOR_ORIGIN O
    ,SIT.DM_GLB_CITY CITY
    ,SIT.DM_FLX_ZIPCODE_NORM tra
    ,SIT.DM_IVE_KPI_MASTER KPI
    
    where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
    and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
    and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
    and a.city_des=tra.city_des(+)
    and tra.city_id=CITY.city_id(+)
    and a.kpi_id= kpi.kpi_id(+)
    and a.city_des !='Acumulado'
    
    union all
    
    select 
        a.YEAR_KEY,
        a.MONTH_KEY,
        a.ZONE_KEY,
        a.VISITOR_ORIGIN_KEY,
        a.VISITOR_CATEGORY_KEY,
        a.CITY_KEY,
        a.ZIPCODE_ID,
        a.KPI_KEY,
        a.KPI_VALUE_NUM,
        a.LOAD_TIME
    from 
        (select a.*,z.zone_id from 
        SIT.FC_FLX_BY_CITY_MONTH a,
                SIT.DM_FLX_ZONE z,
                SIT.DM_IVE_KPI_MASTER KPI
            where a.zone_key=z.zone_key
            and a.kpi_key=kpi.kpi_key) a,
        
        (select distinct
            year_key,
            MONTH_KEY,
            zone_ID,
            kpi_id 
            from   PROCESS.AX_FLX_BY_CITY_MONTH_ALL_TODAY 
        ) b
        
    where a.MONTH_KEY=b.MONTH_KEY(+)
    and a.year_key = b.year_key(+)
    and trim(a.zone_id)=trim(b.zone_id(+))
    and b.MONTH_KEY is null
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
select count(1), CITY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY                                    
from PROCESS.AX_FLX_BY_CITY_MONTH 
group by CITY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
having count(1) > 1);

IF (COMPROBACION_FINAL > 0) THEN

INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
SELECT 'Hay duplicados en AX_FLX_BY_CITY_MONTH' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
select count(1), CITY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
from PROCESS.AX_FLX_BY_CITY_MONTH
group by CITY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
having count(1) > 1);

-- Mensaje del return
RETURN 'Proceso cancelado: Hay duplicados en AX_FLX_BY_CITY_MONTH: ' || COMPROBACION_FINAL;	
ELSE
  */  CREATE or REPLACE TABLE SIT.FC_FLX_BY_CITY_MONTH
        (
            YEAR_KEY NUMBER (10,0) ,
            MONTH_KEY NUMBER (10,0) ,
            ZONE_KEY  NUMBER (16,0) ,
            VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
            VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
            CITY_KEY NUMBER (16,0),
            ZIPCODE_ID VARCHAR(10),
            KPI_KEY NUMBER (16,0),  
            KPI_VALUE_NUM FLOAT,
            LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
        );
        
        INSERT INTO SIT.FC_FLX_BY_CITY_MONTH 
        (
        select 
            a.YEAR_KEY,
            a.MONTH_KEY,
            a.zone_key,
            a.visitor_origin_key,
            a.visitor_category_key,
            a.CITY_key, 
            nvl(A.ZIPCODE_ID,'00000') ZIPCODE_ID,
            a.KPI_KEY,
            a.KPI_VALUE_NUM,
            LOAD_TIME
        from PROCESS.AX_FLX_BY_CITY_MONTH a
        where city_key>0
        );
/*
        END IF;
END;
*/

CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_CITY_PRE_ALL_TODAY
(
  PERIOD_KEY NUMBER (16,0) ,
  ZONE_ID  VARCHAR(100) ,
  VISITOR_ORIGIN_ID  VARCHAR(20 CHAR) ,
  VISITOR_CATEGORY_ID  VARCHAR(100) ,
  CITY_DES VARCHAR(200 CHAR),
  KPI_ID  VARCHAR(100) ,
  KPI_VALUE_NUM FLOAT,
  CONTINUO  VARCHAR(10) ,
  FECHA_CARGA DATE,
  DUPLICATE_FILTER  NUMBER (16,0) ,    
  HYBRID_FILTER  NUMBER (16,0) ,  
  LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
);

INSERT INTO PROCESS.AX_FLX_BY_CITY_PRE_ALL_TODAY
(

WITH CONSOLIDADO AS (
    select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
        'Diario' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE    
    from STAGING.ST_FLX_DIARIO_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_DIARIO_MUNICIPIO) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
    
     union all
     
     select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
        'Llegada' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE    
    from STAGING.ST_FLX_LLEGADA_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_LLEGADA_MUNICIPIO) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
    
     union all
     
     select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
        'Salida' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE    
    from STAGING.ST_FLX_SALIDA_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_SALIDA_MUNICIPIO) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
    
     union all
     
      select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
        'Nocturno' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE    
    from STAGING.ST_FLX_NOCTURNO_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_NOCTURNO_MUNICIPIO) M
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
        'Diario' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE    
    from STAGING.ST_FLX_DIARIO_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_DIARIO_MUNICIPIO) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
    
     union all
     
     select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
        'Llegada' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE    
    from STAGING.ST_FLX_LLEGADA_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_LLEGADA_MUNICIPIO) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
    
     union all
     
     select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
        'Salida' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE    
    from STAGING.ST_FLX_SALIDA_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_SALIDA_MUNICIPIO) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
    
     union all
     
      select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
        'Nocturno' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE    
    from STAGING.ST_FLX_NOCTURNO_MUNICIPIO a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_NOCTURNO_MUNICIPIO) M
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
        KPI_ID,
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
        C.KPI_ID,
        C.Volumen KPI_VALUE_NUM,
        C.continuo,
        C.LOAD_DATE
    FROM CONTINUO C
    LEFT JOIN CONSOLIDADO NC
    ON C.PERIOD_KEY = NC.PERIOD_KEY
    AND C.KPI_ID = NC.KPI_ID
    AND C.zonaobservacion = NC.zonaobservacion
    WHERE NC.PERIOD_KEY IS NULL
)


SELECT 
     period_key,
     ZONE_ID,
     VISITOR_ORIGIN_ID,
     VISITOR_CATEGORY_ID,
     CITY_DES,
     KPI_ID,
     KPI_VALUE_NUM,
     continuo,
     LOAD_DATE AS FECHA_CARGA,     
     row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,CITY_DES ,KPI_ID ,KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER ,
     row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,CITY_DES ,KPI_ID order by LOAD_DATE DESC) AS HYBRID_FILTER ,  
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
FROM PROCESS.AX_FLX_BY_CITY_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

-- Verificar el valor de la comprobación inicial
IF (COMPROBACION_INICIAL > 0) THEN
-- Registrar el problema en la tabla de logs
INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
SELECT 'AX_FLX_BY_CITY_PRE_ALL_TODAY' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
FROM PROCESS.AX_FLX_BY_CITY_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

-- Finalizar el proceso con un mensaje indicando el error
RETURN 'Proceso cancelado: Se detectaron duplicados en AX_FLX_BY_CITY_PRE_ALL_TODAY. Total: ' || COMPROBACION_INICIAL;
ELSE
*/
            CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_CITY_ALL_TODAY
            (
              PERIOD_KEY NUMBER (16,0) ,
              ZONE_ID  VARCHAR(100) ,
              VISITOR_ORIGIN_ID  VARCHAR(20 CHAR) ,
              VISITOR_CATEGORY_ID  VARCHAR(100) ,
              CITY_DES VARCHAR(200 CHAR),
              KPI_ID  VARCHAR(100) ,
              KPI_VALUE_NUM FLOAT,
              LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
              );
        
            INSERT INTO PROCESS.AX_FLX_BY_CITY_ALL_TODAY
            (
            SELECT 
                 period_key,
                 ZONE_ID,
                 VISITOR_ORIGIN_ID,
                 VISITOR_CATEGORY_ID,
                 CITY_DES,
                 KPI_ID,
                 KPI_VALUE_NUM,
                 current_timestamp() LOAD_TIME
            FROM PROCESS.AX_FLX_BY_CITY_PRE_ALL_TODAY     
            WHERE DUPLICATE_FILTER = 1
            
            );

             
            CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_CITY
            (
              PERIOD_KEY NUMBER (16,0) ,
              ZONE_KEY  NUMBER (16,0) ,
              VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
              VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
              CITY_KEY NUMBER (16,0),
              KPI_KEY NUMBER (16,0),
              ZIPCODE_ID VARCHAR(10),
              KPI_VALUE_NUM FLOAT,
              LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
            );

            INSERT INTO PROCESS.AX_FLX_BY_CITY
            (
            select 
                a.period_key,
                nvl(z.ZONE_KEY,-3) ZONE_KEY,
                nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                nvl(CITY.CITY_key,-3) CITY_key,
                nvl(KPI_KEY,-3) KPI_KEY,
                LPAD(tra.ZIPCODE_ID, 5, '0') ZIPCODE_ID
                ,a.KPI_VALUE_NUM,
                current_timestamp() LOAD_TIME
            from PROCESS.AX_FLX_BY_CITY_ALL_TODAY a
            ,SIT.DM_FLX_ZONE z
            ,SIT.DM_GLB_VISITOR_CATEGORY C
            ,SIT.DM_FLX_VISITOR_ORIGIN O
            ,SIT.DM_GLB_CITY CITY
            ,SIT.DM_FLX_ZIPCODE_NORM tra
            ,SIT.DM_IVE_KPI_MASTER KPI
            
            where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
            and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
            and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
            and a.city_des=tra.city_des(+)
            and tra.city_id=CITY.city_id(+)
            and a.kpi_id= kpi.kpi_id(+)
            and a.city_des !='Acumulado'
            
            union all
            
            select 
                a.PERIOD_KEY, 
                a.ZONE_KEY,
                a.VISITOR_ORIGIN_KEY,
                a.VISITOR_CATEGORY_KEY,
                a.CITY_KEY,
                a.KPI_KEY,
                LPAD(a.ZIPCODE_ID, 5, '0') ZIPCODE_ID,
                a.KPI_VALUE_NUM,
                a.LOAD_TIME
            from (select a.*,z.zone_id, kpi.kpi_ID
                from SIT.FC_FLX_BY_CITY a,
                SIT.DM_FLX_ZONE z ,
                SIT.DM_IVE_KPI_MASTER KPI
                where a.zone_key=z.zone_key
                and a.kpi_key=kpi.kpi_key) a,
            (select distinct 
                PERIOD_KEY,
                zone_ID,
                kpi_ID
                from  PROCESS.AX_FLX_BY_CITY_ALL_TODAY) b
            where a.period_key=b.period_key(+) 
            and trim(a.zone_id)=trim(b.zone_id(+))
            and a.KPI_id=b.kpi_id(+)
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
    select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, KPI_KEY                                    
    from PROCESS.AX_FLX_BY_CITY 
    group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
    having count(1) > 1);

    IF (COMPROBACION_FINAL > 0) THEN

    INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
    SELECT 'Hay duplicados en AX_FLX_BY_CITY' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
    select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
    from PROCESS.AX_FLX_BY_CITY
    group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
    having count(1) > 1);

    -- Mensaje del return
    RETURN 'Proceso cancelado: Hay duplicados en AX_FLX_BY_CITY: ' || COMPROBACION_FINAL;	
    ELSE
*/
        CREATE or REPLACE TABLE SIT.FC_FLX_BY_CITY
        (
            PERIOD_KEY       NUMBER (16,0) ,
            ZONE_KEY  NUMBER (16,0) ,
            VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
            VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
            CITY_KEY NUMBER (16,0),
            KPI_KEY NUMBER (16,0),
            ZIPCODE_ID VARCHAR(10),
            KPI_VALUE_NUM FLOAT,
            LOAD_TIME TIMESTAMP_LTZ default current_timestamp()  
        );
        
        INSERT INTO SIT.FC_FLX_BY_CITY 
        (
        select 
            a.period_key,
            a.zone_key,
            a.visitor_origin_key,
            a.visitor_category_key,
            a.CITY_key, 
            a.kpi_key,
            nvl(A.ZIPCODE_ID,'00000') ZIPCODE_ID,
            a.KPI_VALUE_NUM,
            LOAD_TIME
        from PROCESS.AX_FLX_BY_CITY a
        where city_key>0
        );

  /*
    END IF;
    END;
*/

CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_COUNTRY_MONTH_PRE_ALL_TODAY
(
  YEAR_KEY NUMBER (10,0) ,
  MONTH_KEY NUMBER (10,0) ,
  ZONE_ID  VARCHAR(100) ,
  VISITOR_ORIGIN_ID  VARCHAR(20 CHAR) ,
  VISITOR_CATEGORY_ID  VARCHAR(100) ,
  COUNTRY_DES VARCHAR(75 CHAR),
  KPI_ID  VARCHAR(100) ,
  KPI_VALUE_NUM FLOAT,
  CONTINUO  VARCHAR(10) ,
  FECHA_CARGA DATE,
  DUPLICATE_FILTER  NUMBER (16,0) ,    
  HYBRID_FILTER  NUMBER (16,0) ,  
  LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
);

INSERT INTO PROCESS.AX_FLX_BY_COUNTRY_MONTH_PRE_ALL_TODAY
(

WITH CONSOLIDADO AS (
    select 
        TRIM(MES) AS MES,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(pais) AS pais,
        'Diario' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_DIARIO_MES_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_DIARIO_MES_NACIONALIDAD) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
 
     union all
     
      select 
        TRIM(MES) AS MES,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(pais) AS pais,
        'Nocturno' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_NOCTURNO_MES_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_NOCTURNO_MES_NACIONALIDAD) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
),

CONTINUO AS (
    select 
        TRIM(MES) AS MES,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(pais) AS pais,
        'Diario' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_DIARIO_MES_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_DIARIO_MES_NACIONALIDAD) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
    
     union all
     
      select 
        TRIM(MES) AS MES,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(pais) AS pais,
        'Nocturno' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_NOCTURNO_MES_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_NOCTURNO_MES_NACIONALIDAD) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
),

TOTAL AS (
    SELECT 
        CAST(SUBSTRING(MES, 1, 4) AS INTEGER) YEAR_KEY,
        CAST(SUBSTRING(MES, 5, 2) AS INTEGER) MONTH_KEY,
        zonaobservacion ZONE_ID,
        origen VISITOR_ORIGIN_ID,
        categoriadelvisitante VISITOR_CATEGORY_ID,
        pais  country_des,
        KPI_ID,
        Volumen KPI_VALUE_NUM,
        continuo,
        LOAD_DATE        
    FROM CONSOLIDADO
    
    UNION ALL
    
    SELECT 
        CAST(SUBSTRING(C.MES, 1, 4) AS INTEGER) YEAR_KEY,
        CAST(SUBSTRING(C.MES, 5, 2) AS INTEGER) MONTH_KEY,
        C.zonaobservacion ZONE_ID,
        C.origen VISITOR_ORIGIN_ID,
        C.categoriadelvisitante VISITOR_CATEGORY_ID,
        C.pais  country_des,
        C.KPI_ID,
        C.Volumen KPI_VALUE_NUM,
        C.continuo,
        C.LOAD_DATE  
    FROM CONTINUO C
    LEFT JOIN CONSOLIDADO NC
    ON C.MES = NC.MES
    AND C.zonaobservacion = NC.zonaobservacion
    AND C.KPI_ID = NC.KPI_ID
    WHERE NC.MES IS NULL
)

SELECT 
     YEAR_KEY,
     MONTH_KEY,
     ZONE_ID,
     VISITOR_ORIGIN_ID,
     VISITOR_CATEGORY_ID,
     country_des,
     KPI_ID,
     KPI_VALUE_NUM, 
     continuo,
     LOAD_DATE AS FECHA_CARGA,
     row_number() over (partition by YEAR_KEY, MONTH_KEY,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,country_des ,KPI_ID ,KPI_VALUE_NUM order by LOAD_DATE desc) AS DUPLICATE_FILTER , 
     row_number() over (partition by YEAR_KEY, MONTH_KEY,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,country_des ,KPI_ID order by LOAD_DATE desc) AS HYBRID_FILTER, 
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
FROM PROCESS.AX_FLX_BY_COUNTRY_MONTH_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

-- Verificar el valor de la comprobación inicial
IF (COMPROBACION_INICIAL > 0) THEN
-- Registrar el problema en la tabla de logs
INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
SELECT 'AX_FLX_BY_COUNTRY_MONTH_PRE_ALL_TODAY' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
FROM PROCESS.AX_FLX_BY_COUNTRY_MONTH_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

-- Finalizar el proceso con un mensaje indicando el error
RETURN 'Proceso cancelado: Se detectaron duplicados en AX_FLX_BY_COUNTRY_MONTH_PRE_ALL_TODAY. Total: ' || COMPROBACION_INICIAL;
ELSE
*/
        CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_COUNTRY_MONTH_ALL_TODAY
        (
          YEAR_KEY NUMBER (10,0) ,
          MONTH_KEY NUMBER (10,0) ,
          ZONE_ID  VARCHAR(100) ,
          VISITOR_ORIGIN_ID  VARCHAR(20 CHAR) ,
          VISITOR_CATEGORY_ID  VARCHAR(100) ,
          COUNTRY_DES VARCHAR(75 CHAR),
          KPI_ID  VARCHAR(100) ,
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
          );
    
        
        INSERT INTO PROCESS.AX_FLX_BY_COUNTRY_MONTH_ALL_TODAY
        (
        SELECT 
             YEAR_KEY,
             MONTH_KEY,
             ZONE_ID,
             VISITOR_ORIGIN_ID,
             VISITOR_CATEGORY_ID,
             country_des,
             KPI_ID,
             KPI_VALUE_NUM,
             current_timestamp() LOAD_TIME
        FROM PROCESS.AX_FLX_BY_COUNTRY_MONTH_PRE_ALL_TODAY     
        WHERE DUPLICATE_FILTER = 1
        );

        insert into PROCESS.RL_COUNTRY_TRANSLATOR (
        
        select 
            a.COUNTRY_DES RAW_COUNTRY_DES,
            'AX_FLX_BY_COUNTRY_MONTH_ALL_TODAY' ORIGIN_DATA_ID,
            nvl(max(b.country_des),'*********') country_des,
            nvl(max(b.country_id),'***') country_id,
            current_timestamp() LOAD_TIME 
        from (select 
            TRANSLATE(upper(a.COUNTRY_DES),'ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’', 'AEIOUAEIOUAEIOUO') tra_country_des_JOIN,
            country_DES 
            from  PROCESS.AX_FLX_BY_COUNTRY_MONTH_ALL_TODAY  a) a,
        
        (select distinct 
            TRANSLATE(upper(a.COUNTRY_DES),'ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’', 'AEIOUAEIOUAEIOUO') COUNTRY_DES_JOIN,
            a.country_des,country_id
        from SIT.DM_GLB_COUNTRY_MLG a,
             SIT.DM_GLB_COUNTRY b 
            where a.country_key=b.country_key ) b, 
        
        PROCESS.RL_COUNTRY_TRANSLATOR tra
        where a.tra_country_des_JOIN=b.COUNTRY_DES_JOIN(+)
        and a.COUNTRY_DES=tra.RAW_COUNTRY_DES(+)
        and tra.RAW_COUNTRY_DES is null
        group by a.COUNTRY_DES
        
        );

 
        CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_COUNTRY_MONTH
        (
          YEAR_KEY NUMBER (10,0) ,
          MONTH_KEY NUMBER (10,0) ,
          ZONE_KEY  NUMBER (16,0) ,
          VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
          VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
          COUNTRY_KEY NUMBER (16,0),
          KPI_KEY NUMBER (16,0),
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
        );
            
        INSERT INTO PROCESS.AX_FLX_BY_COUNTRY_MONTH
        (
        select 
            a.YEAR_KEY,
            a.MONTH_key,    
            nvl(z.ZONE_KEY,-3) ZONE_KEY,
            nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
            nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
            nvl(country.country_key,-3) country_key,
            nvl(KPI_KEY,-3) KPI_KEY,
            a.KPI_VALUE_NUM,
            current_timestamp() LOAD_TIME
        from PROCESS.AX_FLX_BY_COUNTRY_MONTH_ALL_TODAY a
            ,SIT.DM_FLX_ZONE z
            ,SIT.DM_GLB_VISITOR_CATEGORY C
            ,SIT.DM_FLX_VISITOR_ORIGIN O
            ,PROCESS.RL_COUNTRY_TRANSLATOR TRA
            ,SIT.DM_GLB_COUNTRY country
            ,SIT.DM_IVE_KPI_MASTER KPI
            
        where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
        and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
        and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
        and a.country_des=tra.raw_country_des(+)
        and tra.country_id=country.country_id(+)
        and a.kpi_id= kpi.kpi_id(+)
        
        union all
        
        select 
            a.YEAR_KEY,
            a.MONTH_key,    
            a.ZONE_KEY,
            a.VISITOR_ORIGIN_KEY,
            a.VISITOR_CATEGORY_KEY,
            a.COUNTRY_KEY,
            a.KPI_KEY,
            a.KPI_VALUE_NUM,
            a.LOAD_TIME
        from (
            select 
                a.*,
                z.zone_id,
                kpi.kpi_id 
                from SIT.FC_FLX_BY_COUNTRY_MONTH a,
                SIT.DM_FLX_ZONE z,
                SIT.DM_IVE_KPI_MASTER KPI 
                where a.zone_key=z.zone_key
                and a.kpi_key=kpi.kpi_key
            ) a,
            (select distinct
                year_key,
                MONTH_KEY,
                zone_ID,
                kpi_id 
                from PROCESS.AX_FLX_BY_COUNTRY_MONTH_ALL_TODAY
            ) b
        where a.MONTH_KEY=b.MONTH_KEY(+)
        and a.year_key = b.year_key(+)
        and trim(a.zone_id)=trim(b.zone_id(+))
        and a.KPI_id=b.kpi_id(+)
        and b.MONTH_KEY is null
        
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
select count(1), COUNTRY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY                                    
from PROCESS.AX_FLX_BY_COUNTRY_MONTH 
group by COUNTRY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
having count(1) > 1);

IF (COMPROBACION_FINAL > 0) THEN

INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
SELECT 'Hay duplicados en AX_FLX_BY_COUNTRY_MONTH' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
select count(1), COUNTRY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
from PROCESS.AX_FLX_BY_COUNTRY_MONTH
group by COUNTRY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
having count(1) > 1);

-- Mensaje del return
RETURN 'Proceso cancelado: Hay duplicados en AX_FLX_BY_COUNTRY_MONTH: ' || COMPROBACION_FINAL;	
ELSE
*/
            CREATE or REPLACE TABLE SIT.FC_FLX_BY_COUNTRY_MONTH
        (
          YEAR_KEY NUMBER (10,0) ,
          MONTH_KEY NUMBER (10,0) ,
          ZONE_KEY  NUMBER (16,0) ,
          VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
          VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
          COUNTRY_KEY NUMBER (16,0),
          KPI_KEY NUMBER (16,0),
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
        );
        
        INSERT INTO SIT.FC_FLX_BY_COUNTRY_MONTH 
        (
            select 
            a.YEAR_KEY,
            a.MONTH_key,   
            a.zone_key,
            a.visitor_origin_key,
            a.visitor_category_key,
            a.COUNTRY_key, 
            a.kpi_key,
            a.KPI_VALUE_NUM,
            LOAD_TIME
        from PROCESS.AX_FLX_BY_COUNTRY_MONTH a
        where country_key>0
        );
/*
        END IF;
END;
  */      

CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_COUNTRY_PRE_ALL_TODAY
(
  PERIOD_KEY  NUMBER (16,0) ,
  ZONE_ID  VARCHAR(100) ,
  VISITOR_ORIGIN_ID  VARCHAR(20 CHAR) ,
  VISITOR_CATEGORY_ID  VARCHAR(100) ,
  COUNTRY_DES VARCHAR(75 CHAR),
  KPI_ID  VARCHAR(100) ,
  KPI_VALUE_NUM FLOAT,
  CONTINUO  VARCHAR(10) ,
  FECHA_CARGA DATE,
  DUPLICATE_FILTER  NUMBER (16,0) ,    
  HYBRID_FILTER  NUMBER (16,0) ,  
  LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
);

INSERT INTO PROCESS.AX_FLX_BY_COUNTRY_PRE_ALL_TODAY
(
WITH CONSOLIDADO AS (
    select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(pais) AS pais,
        'Diario' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_DIARIO_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_DIARIO_NACIONALIDAD) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
    
     union all
     
     select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(pais) AS pais,
        'Llegada' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_LLEGADA_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_LLEGADA_NACIONALIDAD) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
    
     union all
     
     select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(pais) AS pais,
        'Salida' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_SALIDA_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_SALIDA_NACIONALIDAD) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
    
     union all
     
      select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(pais) AS pais,
        'Nocturno' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_NOCTURNO_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_NOCTURNO_NACIONALIDAD) M
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
        trim(pais) AS pais,
        'Diario' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_DIARIO_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_DIARIO_NACIONALIDAD) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
    
     union all
     
     select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(pais) AS pais,
        'Llegada' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_LLEGADA_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_LLEGADA_NACIONALIDAD) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
    
     union all
     
     select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(pais) AS pais,
        'Salida' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_SALIDA_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_SALIDA_NACIONALIDAD) M
	where a.load_date=m.load_date  
    AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
    
     union all
     
      select 
        to_number(to_char(fecha,'YYYYMMDD')) period_key,
        trim(zonaobservacion) AS zonaobservacion,
        trim(origen) AS origen,
        trim(categoriadelvisitante) AS categoriadelvisitante,
        trim(pais) AS pais,
        'Nocturno' KPI_ID,
        Volumen,
        trim(continuo) continuo,
        trim(A.LOAD_DATE) LOAD_DATE
    from STAGING.ST_FLX_NOCTURNO_NACIONALIDAD a
    ,(select DISTINCT LOAD_DATE from STAGING.ST_FLX_NOCTURNO_NACIONALIDAD) M
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
        pais  country_des,
        KPI_ID,
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
        C.pais  country_des,
        C.KPI_ID,
        C.Volumen KPI_VALUE_NUM,
        C.continuo,
        C.LOAD_DATE
    FROM CONTINUO C
    LEFT JOIN CONSOLIDADO NC
    ON C.PERIOD_KEY = NC.PERIOD_KEY
    AND C.zonaobservacion = NC.zonaobservacion
    AND C.KPI_ID = NC.KPI_ID
    WHERE NC.PERIOD_KEY IS NULL
)

SELECT 
     period_key,
     ZONE_ID,
     VISITOR_ORIGIN_ID,
     VISITOR_CATEGORY_ID,
     country_des,
     KPI_ID,
     KPI_VALUE_NUM,
     continuo,
     LOAD_DATE AS FECHA_CARGA,
     row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,country_des ,KPI_ID ,KPI_VALUE_NUM order by LOAD_DATE desc) AS DUPLICATE_FILTER,
     row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,country_des ,KPI_ID order by LOAD_DATE desc) AS HYBRID_FILTER, 
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
FROM PROCESS.AX_FLX_BY_COUNTRY_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

-- Verificar el valor de la comprobación inicial
IF (COMPROBACION_INICIAL > 0) THEN
-- Registrar el problema en la tabla de logs
INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
SELECT 'AX_FLX_BY_COUNTRY_PRE_ALL_TODAY' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
FROM PROCESS.AX_FLX_BY_COUNTRY_PRE_ALL_TODAY 
WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

-- Finalizar el proceso con un mensaje indicando el error
RETURN 'Proceso cancelado: Se detectaron duplicados en AX_FLX_BY_COUNTRY_PRE_ALL_TODAY. Total: ' || COMPROBACION_INICIAL;
ELSE
*/
                CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_COUNTRY_ALL_TODAY
            (
              PERIOD_KEY NUMBER (16,0) ,
              ZONE_ID  VARCHAR(100) ,
              VISITOR_ORIGIN_ID  VARCHAR(20 CHAR) ,
              VISITOR_CATEGORY_ID  VARCHAR(100) ,
              COUNTRY_DES VARCHAR(75 CHAR),
              KPI_ID  VARCHAR(100) ,
              KPI_VALUE_NUM FLOAT,
              LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
              );
            
            INSERT INTO PROCESS.AX_FLX_BY_COUNTRY_ALL_TODAY
            (
            SELECT 
                 period_key,
                 ZONE_ID,
                 VISITOR_ORIGIN_ID,
                 VISITOR_CATEGORY_ID,
                 country_des,
                 KPI_ID,
                 KPI_VALUE_NUM,
                 current_timestamp() LOAD_TIME
            FROM PROCESS.AX_FLX_BY_COUNTRY_PRE_ALL_TODAY     
            WHERE DUPLICATE_FILTER = 1
            );
            
            
            -- ------RL_COUNTRY_TRANSLATOR---------------------
            
            
            insert into PROCESS.RL_COUNTRY_TRANSLATOR (
            
            select 
                a.COUNTRY_DES RAW_COUNTRY_DES,
                'AX_FLX_BY_COUNTRY_ALL_TODAY' ORIGIN_DATA_ID,
                nvl(max(b.country_des),'*********') country_des,
                nvl(max(b.country_id),'***') country_id,
                current_timestamp() LOAD_TIME 
            from (select
                TRANSLATE(upper(a.COUNTRY_DES),'ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’', 'AEIOUAEIOUAEIOUO') tra_country_des_JOIN,
                country_DES 
            from  PROCESS.AX_FLX_BY_COUNTRY_ALL_TODAY  a) a,
            
            (select distinct 
                TRANSLATE(upper(a.COUNTRY_DES),'ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’', 'AEIOUAEIOUAEIOUO') COUNTRY_DES_JOIN,
                a.country_des,country_id 
            from  SIT.DM_GLB_COUNTRY_MLG a,
            SIT.DM_GLB_COUNTRY b 
            where a.country_key=b.country_key ) b, 
            
            PROCESS.RL_COUNTRY_TRANSLATOR tra
            where a.tra_country_des_JOIN=b.COUNTRY_DES_JOIN(+)
            and a.COUNTRY_DES=tra.RAW_COUNTRY_DES(+)
            and tra.RAW_COUNTRY_DES is null
            group by a.COUNTRY_DES
            );
            
            
            -- ------AX_FLX_BY_COUNTRY-------------------------------------------
            
            CREATE or REPLACE TABLE PROCESS.AX_FLX_BY_COUNTRY
            (
              PERIOD_KEY NUMBER (16,0) ,
              ZONE_KEY  NUMBER (16,0) ,
              VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
              VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
              COUNTRY_KEY NUMBER (16,0),
              KPI_KEY NUMBER (16,0),
              KPI_VALUE_NUM FLOAT,
              LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
            );
                        
            INSERT INTO PROCESS.AX_FLX_BY_COUNTRY
            (
            select 
                a.period_key,
                nvl(z.ZONE_KEY,-3) ZONE_KEY,
                nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                nvl(country.country_key,-3) country_key,
                nvl(KPI_KEY,-3) KPI_KEY
            ,a.KPI_VALUE_NUM,current_timestamp() LOAD_TIME
            
            from PROCESS.AX_FLX_BY_COUNTRY_ALL_TODAY a
                ,SIT.DM_FLX_ZONE z
                ,SIT.DM_GLB_VISITOR_CATEGORY C
                ,SIT.DM_FLX_VISITOR_ORIGIN O
                ,PROCESS.RL_COUNTRY_TRANSLATOR TRA
                ,SIT.DM_GLB_COUNTRY country
                ,SIT.DM_IVE_KPI_MASTER KPI
            
            where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
            and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
            and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
            and a.country_des=tra.raw_country_des(+)
            and tra.country_id=country.country_id(+)
            and a.kpi_id= kpi.kpi_id(+)
            
            union all
            
            select 
                a.PERIOD_KEY, 
                a.ZONE_KEY,
                a.VISITOR_ORIGIN_KEY,
                a.VISITOR_CATEGORY_KEY,
                a.COUNTRY_KEY,
                a.KPI_KEY,
                a.KPI_VALUE_NUM,
                a.LOAD_TIME
            from (select 
                a.*,
                z.zone_id,
                kpi.kpi_id 
            from SIT.FC_FLX_BY_COUNTRY a,
                SIT.DM_FLX_ZONE z,
                SIT.DM_IVE_KPI_MASTER KPI
                where a.zone_key=z.zone_key
                and a.kpi_key=kpi.kpi_key) a,
            (select distinct 
                PERIOD_KEY,
                zone_ID,
                kpi_id 
            from  PROCESS.AX_FLX_BY_COUNTRY_ALL_TODAY) b
            where a.period_key=b.period_key(+) 
            and trim(a.zone_id)=trim(b.zone_id(+))
            and a.KPI_id=b.kpi_id(+)
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
select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY                                    
from PROCESS.AX_FLX_BY_COUNTRY 
group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
having count(1) > 1);

IF (COMPROBACION_FINAL > 0) THEN

INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
SELECT 'Hay duplicados en AX_FLX_BY_COUNTRY' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
from PROCESS.AX_FLX_BY_COUNTRY
group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
having count(1) > 1);

-- Mensaje del return
RETURN 'Proceso cancelado: Hay duplicados en AX_FLX_BY_COUNTRY: ' || COMPROBACION_FINAL;	
ELSE
*/
      CREATE or REPLACE TABLE SIT.FC_FLX_BY_COUNTRY
        (
          PERIOD_KEY       NUMBER (16,0) ,
          ZONE_KEY  NUMBER (16,0) ,
          VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
          VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
          COUNTRY_KEY NUMBER (16,0),
          KPI_KEY NUMBER (16,0),
          KPI_VALUE_NUM FLOAT,
          LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
        );

        INSERT INTO SIT.FC_FLX_BY_COUNTRY 
        (
        select 
            a.period_key,
            a.zone_key,
            a.visitor_origin_key,
            a.visitor_category_key,
            a.COUNTRY_key, 
            a.kpi_key,
            a.KPI_VALUE_NUM,
            LOAD_TIME
        from PROCESS.AX_FLX_BY_COUNTRY a
        where country_key>0
        );

/*
END IF;
END;
*/
