TRUNCATE TABLE SIT.FC_FLX_BY_COUNTRY;
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

