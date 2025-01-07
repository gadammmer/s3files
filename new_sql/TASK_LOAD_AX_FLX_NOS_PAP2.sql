EXECUTE IMMEDIATE '
BEGIN
        create or replace task PROCESS.LOAD_AX_FLX_NOS
          USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
          after PROCESS.LOAD_AX_FLX_LOS
          as BEGIN

            TRUNCATE TABLE PROCESS.AX_FLX_NOS_BY_CITY_PRE_ALL_TODAY;
            INSERT INTO PROCESS.AX_FLX_NOS_BY_CITY_PRE_ALL_TODAY
            (
            WITH CONSOLIDADO AS (
                select 
                to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                trim(DURACIONESTANCIA)  DURACIONESTANCIA,
                Volumen ,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
            from STAGING.ST_FLX_NOCHEESTANCIA_MUNICIPIO a,
            (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCHEESTANCIA_MUNICIPIO) M	
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
                trim(DURACIONESTANCIA)  DURACIONESTANCIA ,
                Volumen ,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
            from STAGING.ST_FLX_NOCHEESTANCIA_MUNICIPIO a,
            (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCHEESTANCIA_MUNICIPIO) M	
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
                    DURACIONESTANCIA NIGHTS_STAY_ID,
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
                    C.DURACIONESTANCIA NIGHTS_STAY_ID,
                    C.Volumen KPI_VALUE_NUM,
                    C.continuo,
                    C.LOAD_DATE          
                FROM CONTINUO C
                LEFT JOIN CONSOLIDADO NC
                ON C.period_key = NC.period_key
                AND C.zonaobservacion = NC.zonaobservacion
                AND C.DURACIONESTANCIA = NC.DURACIONESTANCIA
                WHERE NC.period_key IS NULL
            )

            SELECT 
                period_key,
                ZONE_ID,
                VISITOR_ORIGIN_ID,
                VISITOR_CATEGORY_ID,
                CITY_DES,
                NIGHTS_STAY_ID,
                KPI_VALUE_NUM,  
                CONTINUO,
                LOAD_DATE AS FECHA_CARGA,     
                row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,NIGHTS_STAY_ID ,CITY_DES,KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER ,  
                row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID, NIGHTS_STAY_ID ,CITY_DES order by LOAD_DATE DESC) AS HYBRID_FILTER ,  
                current_timestamp() LOAD_TIME
            FROM TOTAL    
            );

          DECLARE
          COMPROBACION_INICIAL INT;
          BEGIN
          -- Comprobar si existen duplicados en la tabla AX_FLX_BY_AGE_PRE_ALL_TODAY
          SELECT COUNT(1) 
          INTO COMPROBACION_INICIAL
          FROM PROCESS.AX_FLX_NOS_BY_CITY_PRE_ALL_TODAY 
          WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

          -- Verificar el valor de la comprobación inicial
          IF (COMPROBACION_INICIAL > 0) THEN
          -- Registrar el problema en la tabla de logs
          INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
          SELECT ''AX_FLX_NOS_BY_CITY_PRE_ALL_TODAY'' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
          FROM PROCESS.AX_FLX_NOS_BY_CITY_PRE_ALL_TODAY 
          WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

          -- Finalizar el proceso con un mensaje indicando el error
          RETURN ''Proceso cancelado: Se detectaron duplicados en AX_FLX_NOS_BY_CITY_PRE_ALL_TODAY. Total: '' || COMPROBACION_INICIAL;
          ELSE
                TRUNCATE TABLE PROCESS.AX_FLX_NOS_BY_CITY_ALL_TODAY;
                INSERT INTO PROCESS.AX_FLX_NOS_BY_CITY_ALL_TODAY
                (
                SELECT 
                    PERIOD_KEY,
                    ZONE_ID,
                    VISITOR_ORIGIN_ID,
                    VISITOR_CATEGORY_ID,
                    CITY_DES,
                    NIGHTS_STAY_ID,
                    KPI_VALUE_NUM,
                    current_timestamp() LOAD_TIME
                FROM PROCESS.AX_FLX_NOS_BY_CITY_PRE_ALL_TODAY     
                WHERE DUPLICATE_FILTER = 1
                );
              
                TRUNCATE TABLE PROCESS.AX_FLX_NOS_BY_CITY;
                INSERT INTO PROCESS.AX_FLX_NOS_BY_CITY
                (
                select 
                    a.period_key,
                    nvl(z.ZONE_KEY,-3) ZONE_KEY,
                    nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                    nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                    nvl(CITY.CITY_key,-3) CITY_key,
                    nvl(NIGHTS_STAY_KEY,-3) NIGHTS_STAY_KEY,
                    tra.ZIPCODE_ID,
                    a.KPI_VALUE_NUM,
                    current_timestamp() LOAD_TIME
                from PROCESS.AX_FLX_NOS_BY_CITY_ALL_TODAY a
                ,SIT.DM_FLX_ZONE z
                ,SIT.DM_GLB_VISITOR_CATEGORY C
                ,SIT.DM_FLX_VISITOR_ORIGIN O
                ,SIT.DM_GLB_CITY CITY
                ,SIT.DM_FLX_ZIPCODE_NORM tra
                ,SIT.DM_FLX_NIGHTS_STAY KPI
                where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
                and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
                and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
                and a.city_des=tra.city_des(+)
                and tra.city_id=CITY.city_id(+)
                and a.NIGHTS_STAY_ID= kpi.NIGHTS_STAY_ID(+)
                and a.city_des !=''Acumulado''
                
                union all
                
                select 
                    a.PERIOD_KEY, 
                    a.ZONE_KEY,
                    a.VISITOR_ORIGIN_KEY,
                    a.VISITOR_CATEGORY_KEY,
                    a.CITY_KEY,a.NIGHTS_STAY_KEY,
                    a.ZIPCODE_ID,
                    a.KPI_VALUE_NUM,
                    a.LOAD_TIME
                from 
                (select a.*,z.zone_id from SIT.FC_FLX_NOS_BY_CITY a,SIT.DM_FLX_ZONE z where a.zone_key=z.zone_key) a,
                (select MAX(LOAD_DATE) PERIOD_KEY,zone_ID from  PROCESS.AX_FLX_NOS_BY_CITY_ALL_TODAY) b
                where a.period_key=b.period_key(+) 
                and trim(a.zone_id)=trim(b.zone_id(+))
                and b.period_key is null
                );

            END IF;
            END;

            TRUNCATE TABLE PROCESS.AX_FLX_NOS_BY_COUNTRY_PRE_ALL_TODAY;
            INSERT INTO PROCESS.AX_FLX_NOS_BY_COUNTRY_PRE_ALL_TODAY
            (
            WITH CONSOLIDADO AS (
                select 
                to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                trim(pais) as pais ,
                trim(DURACIONESTANCIA) AS DURACIONESTANCIA,
                Volumen ,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
            from STAGING.ST_FLX_NOCHEESTANCIA_NACIONALIDAD a,
            (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCHEESTANCIA_NACIONALIDAD) M	
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
                trim(pais) as pais ,
                trim(DURACIONESTANCIA) AS DURACIONESTANCIA,
                Volumen ,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
            from STAGING.ST_FLX_NOCHEESTANCIA_NACIONALIDAD a,
            (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCHEESTANCIA_NACIONALIDAD) M	
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
                    DURACIONESTANCIA NIGHTS_STAY_ID,
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
                    C.DURACIONESTANCIA NIGHTS_STAY_ID,
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
                NIGHTS_STAY_ID,
                KPI_VALUE_NUM,  
                CONTINUO,
                LOAD_DATE AS FECHA_CARGA,     
                row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,COUNTRY_DES,NIGHTS_STAY_ID ,KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER ,  
                row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,COUNTRY_DES,NIGHTS_STAY_ID  order by LOAD_DATE DESC) AS HYBRID_FILTER ,  
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
            FROM PROCESS.AX_FLX_NOS_BY_COUNTRY_PRE_ALL_TODAY 
            WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

            -- Verificar el valor de la comprobación inicial
            IF (COMPROBACION_INICIAL > 0) THEN
            -- Registrar el problema en la tabla de logs
            INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
            SELECT ''AX_FLX_NOS_BY_COUNTRY_PRE_ALL_TODAY'' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
            FROM PROCESS.AX_FLX_NOS_BY_COUNTRY_PRE_ALL_TODAY 
            WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

            -- Finalizar el proceso con un mensaje indicando el error
            RETURN ''Proceso cancelado: Se detectaron duplicados en AX_FLX_NOS_BY_COUNTRY_PRE_ALL_TODAY. Total: '' || COMPROBACION_INICIAL;
            ELSE
                
        */
                TRUNCATE TABLE PROCESS.AX_FLX_NOS_BY_COUNTRY_ALL_TODAY;
                INSERT INTO PROCESS.AX_FLX_NOS_BY_COUNTRY_ALL_TODAY
                (
                SELECT 
                    period_key,
                    ZONE_ID,
                    VISITOR_ORIGIN_ID,
                    VISITOR_CATEGORY_ID,
                    COUNTRY_DES,
                    NIGHTS_STAY_ID,
                    KPI_VALUE_NUM,
                    current_timestamp() LOAD_TIME
                FROM PROCESS.AX_FLX_NOS_BY_COUNTRY_PRE_ALL_TODAY
                WHERE DUPLICATE_FILTER = 1
                );
                
                        
                /*
                insert into PROCESS.RL_COUNTRY_TRANSLATOR (
                select 
                    a.COUNTRY_DES RAW_COUNTRY_DES,
                    ''AX_FLX_NOS_BY_COUNTRY_ALL_TODAY'' ORIGIN_DATA_ID,
                    nvl(max(b.country_des),''*********'') country_des,
                    nvl(max(b.country_id),''***'') country_id,
                    current_timestamp() LOAD_TIME 
                from (select 
                          TRANSLATE(upper(a.COUNTRY_DES),''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’'', ''AEIOUAEIOUAEIOUO'') tra_country_des_JOIN,
                          country_DES 
                      from PROCESS.AX_FLX_NOS_BY_COUNTRY_ALL_TODAY  a) a,
                    
                    (select MAX(LOAD_DATE) 
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
*/
                TRUNCATE TABLE PROCESS.AX_FLX_NOS_BY_COUNTRY;
                INSERT INTO PROCESS.AX_FLX_NOS_BY_COUNTRY
                (
                    select 
                    a.period_key,
                    nvl(z.ZONE_KEY,-3) ZONE_KEY,
                    nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                    nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                    nvl(country.country_key,-3) country_key,
                    nvl(ho.NIGHTS_STAY_KEY,-3) NIGHTS_STAY_KEY,
                    a.KPI_VALUE_NUM,
                    current_timestamp() LOAD_TIME
                
                from PROCESS.AX_FLX_NOS_BY_COUNTRY_ALL_TODAY a
                    ,SIT.DM_FLX_ZONE z
                    ,SIT.DM_GLB_VISITOR_CATEGORY C
                    ,SIT.DM_FLX_VISITOR_ORIGIN O
                    ,PROCESS.RL_COUNTRY_TRANSLATOR TRA
                    ,SIT.DM_GLB_COUNTRY country
                    ,SIT.DM_FLX_NIGHTS_STAY ho
                    
                where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
                and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
                and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
                and a.country_des=tra.raw_country_des(+)
                and tra.country_id=country.country_id(+)
                and a.NIGHTS_STAY_ID=ho.NIGHTS_STAY_ID(+)
                
                union all
                
                select 
                    a.PERIOD_KEY, 
                    a.ZONE_KEY,
                    a.VISITOR_ORIGIN_KEY,
                    a.VISITOR_CATEGORY_KEY,
                    a.COUNTRY_KEY,
                    a.NIGHTS_STAY_KEY,
                    a.KPI_VALUE_NUM,
                    a.LOAD_TIME
                from (select 
                        a.*,
                        z.zone_id
                    from SIT.FC_FLX_NOS_BY_COUNTRY a,
                SIT.DM_FLX_ZONE z 
                where a.zone_key=z.zone_key) a
                ,(select MAX(LOAD_DATE) PERIOD_KEY,zone_ID from  PROCESS.AX_FLX_NOS_BY_COUNTRY_ALL_TODAY) b
                where a.period_key=b.period_key(+) 
                and trim(a.zone_id)=trim(b.zone_id(+))
                and b.period_key is null
                );
                
            --  END IF;
             -- END;

              TRUNCATE TABLE PROCESS.AX_FLX_NOS_PRE_ALL_TODAY;
              INSERT INTO PROCESS.AX_FLX_NOS_PRE_ALL_TODAY
              (
              WITH CONSOLIDADO AS (
                  select 
                  to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                      trim(zonaobservacion) AS zonaobservacion,
                      trim(origen) AS origen,
                      trim(categoriadelvisitante) AS categoriadelvisitante,
                  DURACIONESTANCIA ,
                  Volumen  ,
                      trim(continuo) continuo,
                      trim(A.LOAD_DATE) LOAD_DATE
              from STAGING.ST_FLX_NOCHEESTANCIA a,
              (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCHEESTANCIA) M	
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
                  DURACIONESTANCIA ,
                  Volumen ,
                      trim(continuo) continuo,
                      trim(A.LOAD_DATE) LOAD_DATE
              from STAGING.ST_FLX_NOCHEESTANCIA a,
              (select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCHEESTANCIA) M	
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
                      DURACIONESTANCIA NIGHTS_STAY_ID,
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
                      C.DURACIONESTANCIA NIGHTS_STAY_ID,
                      C.Volumen KPI_VALUE_NUM,
                      C.continuo,
                      C.LOAD_DATE           
                  FROM CONTINUO C
                  LEFT JOIN CONSOLIDADO NC
                  ON C.period_key = NC.period_key
                  AND C.zonaobservacion = NC.zonaobservacion
                  AND C.DURACIONESTANCIA = NC.DURACIONESTANCIA
                  WHERE NC.period_key IS NULL
              )

              SELECT 
                  period_key,
                  ZONE_ID,
                  VISITOR_ORIGIN_ID,
                  VISITOR_CATEGORY_ID,
                  NIGHTS_STAY_ID,
                  KPI_VALUE_NUM,   
                  CONTINUO,
                  LOAD_DATE AS FECHA_CARGA,     
                  row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,NIGHTS_STAY_ID ,KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER ,   
                  row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID, NIGHTS_STAY_ID order by LOAD_DATE DESC) AS HYBRID_FILTER ,  
                  current_timestamp() LOAD_TIME
              FROM TOTAL    
              );

              DECLARE
              COMPROBACION_INICIAL INT;
              BEGIN
              -- Comprobar si existen duplicados en la tabla AX_FLX_BY_AGE_PRE_ALL_TODAY
              SELECT COUNT(1) 
              INTO COMPROBACION_INICIAL
              FROM PROCESS.AX_FLX_NOS_PRE_ALL_TODAY 
              WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

              -- Verificar el valor de la comprobación inicial
              IF (COMPROBACION_INICIAL > 0) THEN
              -- Registrar el problema en la tabla de logs
              INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
              SELECT ''AX_FLX_NOS_PRE_ALL_TODAY'' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
              FROM PROCESS.AX_FLX_NOS_PRE_ALL_TODAY 
              WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

              -- Finalizar el proceso con un mensaje indicando el error
              RETURN ''Proceso cancelado: Se detectaron duplicados en AX_FLX_NOS_PRE_ALL_TODAY. Total: '' || COMPROBACION_INICIAL;
              ELSE
                  TRUNCATE TABLE PROCESS.AX_FLX_NOS_ALL_TODAY;
                  INSERT INTO PROCESS.AX_FLX_NOS_ALL_TODAY
                  (
                  SELECT 
                      period_key,
                      ZONE_ID,
                      VISITOR_ORIGIN_ID,
                      VISITOR_CATEGORY_ID,
                      NIGHTS_STAY_ID,
                      KPI_VALUE_NUM, 
                      current_timestamp() LOAD_TIME
                  FROM PROCESS.AX_FLX_NOS_PRE_ALL_TODAY
                  WHERE DUPLICATE_FILTER = 1
                  );

                  
                CREATE or REPLACE TABLE PROCESS.AX_FLX_NOS
                (
                  PERIOD_KEY       NUMBER (16,0) ,
                  ZONE_KEY  NUMBER (16,0) ,
                  VISITOR_ORIGIN_KEY  NUMBER (16,0) ,
                  VISITOR_CATEGORY_KEY  NUMBER (16,0) ,
                  NIGHTS_STAY_KEY NUMBER (16,0),
                  KPI_VALUE_NUM FLOAT,
                  LOAD_TIME TIMESTAMP_LTZ default current_timestamp()
                );

                TRUNCATE TABLE PROCESS.AX_FLX_NOS;
                INSERT INTO PROCESS.AX_FLX_NOS
                (
                select 
                    a.period_key,
                    nvl(z.ZONE_KEY,-3) ZONE_KEY,
                    nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                    nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                    nvl(NIGHTS_STAY_KEY,-3) NIGHTS_STAY_KEY,
                    a.KPI_VALUE_NUM,
                    current_timestamp() LOAD_TIME
                from PROCESS.AX_FLX_NOS_ALL_TODAY a
                ,SIT.DM_FLX_ZONE z
                ,SIT.DM_GLB_VISITOR_CATEGORY C
                ,SIT.DM_FLX_VISITOR_ORIGIN O
                ,SIT.DM_FLX_NIGHTS_STAY KPI
                where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
                and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
                and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
                and a.NIGHTS_STAY_ID= kpi.NIGHTS_STAY_ID(+)
                
                union all
                
                select 
                    a.PERIOD_KEY, 
                    a.ZONE_KEY,
                    a.VISITOR_ORIGIN_KEY,
                    a.VISITOR_CATEGORY_KEY,
                    a.NIGHTS_STAY_KEY,
                    a.KPI_VALUE_NUM,
                    a.LOAD_TIME
                from 
                    (select
                        a.*,
                        z.zone_id 
                    from SIT.FC_FLX_NOS a,
                    SIT.DM_FLX_ZONE z 
                    where a.zone_key=z.zone_key) a,
                (select MAX(LOAD_DATE) 
                    PERIOD_KEY,
                    zone_ID 
                from  PROCESS.AX_FLX_NOS_ALL_TODAY) b
                where a.period_key=b.period_key(+) 
                and trim(a.zone_id)=trim(b.zone_id(+))
                and b.period_key is null
                );

            END IF;
            END;
      END;

      RETURN ''Task LOAD_AX_FLX_BY_NOS creada correctamente'';

END;
';

        
    


