
EXECUTE IMMEDIATE '
    BEGIN        
        create or replace task PROCESS.LOAD_AX_FLX_PLACE_V1
          USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
          after PROCESS.LOAD_AX_FLX_NOS
          as BEGIN

              
            INSERT INTO PROCESS.AX_FLX_PLACE_BY_CITY_WEEK_PRE_ALL_TODAY
            (
            WITH CONSOLIDADO AS (
                select 
                    TRIM(SEMANA) AS SEMANA,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                    trim(ProvinciaZonaDiariaDiaVisita) AS PLACE_PROVINCE_DES,        
                    trim(a.MUNICIPIOZONADIARIADIAVISITA)  PLACE_CITY_DES ,        
                    'LugarActividad_DiaVisita' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE,
                    trim(FILE_NAME) FILE_NAME
                from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_MUNICIPIO_SEMANA a
                ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_MUNICIPIO_SEMANA) M
                where
                a.load_date=m.load_date  
                AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
                AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
                
                union all
                
                select 
                    TRIM(SEMANA) AS SEMANA,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                    trim(PROVINCIAZONADIARIAVISPERA) AS PLACE_PROVINCE_DES,        
                    trim(a.MUNICIPIOZONADIARIAvispera)  PLACE_CITY_DES,
                    'LugarActividad_Vispera' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE,
                    trim(FILE_NAME) FILE_NAME
                from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_MUNICIPIO_SEMANA a
                ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_MUNICIPIO_SEMANA) M
                where a.load_date=m.load_date 
                AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
                AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
                
                union all
                
                select 
                    TRIM(SEMANA) AS SEMANA,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                    trim(provinciazonapernoctaciondiavisita) AS PLACE_PROVINCE_DES, 
                    trim(a.municipiozonapernoctaciondiavisita)  PLACE_CITY_DES,
                    'LugarNoche_DiaVisita' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE,
                    trim(FILE_NAME) FILE_NAME
                from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_MUNICIPIO_SEMANA a
                ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_MUNICIPIO_SEMANA) M
                where a.load_date=m.load_date
                AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
                AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
                union all
                
                select 
                    TRIM(SEMANA) AS SEMANA,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                    trim(PROVINCIAZONAPERNOCTACIONVISPERA) AS PLACE_PROVINCE_DES, 
                    trim(a.MUNICIPIOZONAPERNOCTACIONVISPERA)  PLACE_CITY_DES,
                    'LugarNoche_Vispera' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE,
                    trim(FILE_NAME) FILE_NAME
                from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_MUNICIPIO_SEMANA a
                ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_MUNICIPIO_SEMANA) M
                where a.load_date=m.load_date
                AND (trim(CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
                AND FILE_NAME NOT LIKE '%INVA-1080-9999%'   
            ),

            CONTINUO AS (
                select 
                    TRIM(SEMANA) AS SEMANA,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                    trim(ProvinciaZonaDiariaDiaVisita) AS PLACE_PROVINCE_DES,        
                    trim(a.MUNICIPIOZONADIARIADIAVISITA)  PLACE_CITY_DES ,        
                    'LugarActividad_DiaVisita' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE,
                    trim(FILE_NAME) FILE_NAME
                from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_MUNICIPIO_SEMANA a
                ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_MUNICIPIO_SEMANA) M
                where
                a.load_date=m.load_date  
                AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
                AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
                
                union all
                
                select 
                    TRIM(SEMANA) AS SEMANA,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                    trim(PROVINCIAZONADIARIAVISPERA) AS PLACE_PROVINCE_DES,        
                    trim(a.MUNICIPIOZONADIARIAvispera)  PLACE_CITY_DES,
                    'LugarActividad_Vispera' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE,
                    trim(FILE_NAME) FILE_NAME
                from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_MUNICIPIO_SEMANA a
                ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_MUNICIPIO_SEMANA) M
                where
                a.load_date=m.load_date  
                AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
                AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
                
                union all
                
                select 
                    TRIM(SEMANA) AS SEMANA,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                    trim(provinciazonapernoctaciondiavisita) AS PLACE_PROVINCE_DES, 
                    trim(a.municipiozonapernoctaciondiavisita)  PLACE_CITY_DES,
                    'LugarNoche_DiaVisita' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE,
                    trim(FILE_NAME) FILE_NAME
                from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_MUNICIPIO_SEMANA a
                ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_MUNICIPIO_SEMANA) M
                where
                a.load_date=m.load_date  
                AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
                AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
                
                union all
                
                select 
                    TRIM(SEMANA) AS SEMANA,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                    trim(PROVINCIAZONAPERNOCTACIONVISPERA) AS PLACE_PROVINCE_DES, 
                    trim(a.MUNICIPIOZONAPERNOCTACIONVISPERA)  PLACE_CITY_DES,
                    'LugarNoche_Vispera' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE,
                    trim(FILE_NAME) FILE_NAME
                from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_MUNICIPIO_SEMANA a
                ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_MUNICIPIO_SEMANA) M
                where
                a.load_date=m.load_date  
                AND (trim(CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
                AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
            ),

            TOTAL AS (
                SELECT 
                    SUBSTRING(SEMANA, 1, 4) || (CASE WHEN LENGTH(SUBSTRING(SEMANA, 6, LENGTH(SEMANA) - 5)) = 1 THEN '0' || SUBSTRING(SEMANA, 6, LENGTH(SEMANA) - 5) 
                        ELSE SUBSTRING(SEMANA, 6, LENGTH(SEMANA) - 5) END)  AS WEEK_YEAR_KEY,
                    zonaobservacion ZONE_ID,
                    origen VISITOR_ORIGIN_ID,
                    categoriadelvisitante VISITOR_CATEGORY_ID,
                    NombreMunicipio  CITY_DES,
                    DECODE(PLACE_PROVINCE_DES, 
                        'Alicante', 'Alicante/Alacant',
                        'Castellón', 'Castellón/Castelló',
                        'Illes Balears', 'Islas Baleares',
                        'Valencia', 'Valencia/València', 
                    PLACE_PROVINCE_DES) AS PLACE_PROVINCE_DES,
                    PLACE_CITY_DES,
                    KPI_ID,
                    Volumen KPI_VALUE_NUM ,
                    continuo,
                    LOAD_DATE,
                    REPLACE(FILE_NAME, 'San_Bartolome_de_Tirajana_Municipio_', 'San_Bartolome_de_Tirajana_Municipio' ) AS FILE_NAME
                FROM CONSOLIDADO
                
                UNION ALL
                
                SELECT 
                    SUBSTRING(C.SEMANA, 1, 4) || (CASE WHEN LENGTH(SUBSTRING(C.SEMANA, 6, LENGTH(C.SEMANA) - 5)) = 1 THEN '0' || SUBSTRING(C.SEMANA, 6, LENGTH(C.SEMANA) - 5) 
                        ELSE SUBSTRING(C.SEMANA, 6, LENGTH(C.SEMANA) - 5) END) AS WEEK_YEAR_KEY,	
                    C.zonaobservacion ZONE_ID,
                    C.origen VISITOR_ORIGIN_ID,
                    C.categoriadelvisitante VISITOR_CATEGORY_ID,
                    C.NombreMunicipio  CITY_DES,
                    DECODE(C.PLACE_PROVINCE_DES, 
                        'Alicante', 'Alicante/Alacant',
                        'Castellón', 'Castellón/Castelló',
                        'Illes Balears', 'Islas Baleares',
                        'Valencia', 'Valencia/València', 
                    C.PLACE_PROVINCE_DES) AS PLACE_PROVINCE_DES,
                    C.PLACE_CITY_DES,
                    C.KPI_ID,
                    C.Volumen KPI_VALUE_NUM ,
                    C.continuo,
                    C.LOAD_DATE,
                    REPLACE(C.FILE_NAME, 'San_Bartolome_de_Tirajana_Municipio_', 'San_Bartolome_de_Tirajana_Municipio' ) AS FILE_NAME
                FROM CONTINUO C
                LEFT JOIN CONSOLIDADO NC
                ON C.SEMANA = NC.SEMANA
                AND C.zonaobservacion = NC.zonaobservacion
                AND C.KPI_ID = NC.KPI_ID
                WHERE NC.SEMANA IS NULL
            )

            SELECT 
                WEEK_YEAR_KEY,
                ZONE_ID,
                VISITOR_ORIGIN_ID,
                VISITOR_CATEGORY_ID,
                CITY_DES,
                PLACE_PROVINCE_DES,
                PLACE_CITY_DES,
                KPI_ID,
                KPI_VALUE_NUM,
                CONTINUO,
                LOAD_DATE AS FECHA_CARGA, 
                FILE_NAME,
                row_number() over (partition by WEEK_YEAR_KEY, ZONE_ID ,VISITOR_ORIGIN_ID, VISITOR_CATEGORY_ID, CITY_DES, PLACE_PROVINCE_DES, PLACE_CITY_DES, KPI_ID, FILE_NAME order by LOAD_DATE desc) AS DUPLICATE_FILTER, 
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
            FROM PROCESS.AX_FLX_PLACE_BY_CITY_WEEK_PRE_ALL_TODAY 
            WHERE  DUPLICATE_FILTER = 1;

            -- Verificar el valor de la comprobación inicial
            IF (COMPROBACION_INICIAL > 0) THEN
            -- Registrar el problema en la tabla de logs
            INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
            SELECT ''AX_FLX_PLACE_BY_CITY_WEEK_PRE_ALL_TODAY'' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
            FROM PROCESS.AX_FLX_PLACE_BY_CITY_WEEK_PRE_ALL_TODAY 
            WHERE DUPLICATE_FILTER = 1;

            -- Finalizar el proceso con un mensaje indicando el error
            RETURN ''Proceso cancelado: Se detectaron duplicados en AX_FLX_PLACE_BY_CITY_WEEK_PRE_ALL_TODAY. Total: '' || COMPROBACION_INICIAL;
            ELSE
            */
                INSERT INTO PROCESS.AX_FLX_PLACE_BY_CITY_WEEK_ALL_TODAY
            (
            select 
                WEEK_YEAR_KEY,
                ZONE_ID,
                VISITOR_ORIGIN_ID,
                VISITOR_CATEGORY_ID,
                CITY_DES,
                PLACE_PROVINCE_DES,
                PLACE_CITY_DES,
                KPI_ID,
                SUM(KPI_VALUE_NUM) AS KPI_VALUE_NUM,  
                current_timestamp() LOAD_TIME
            from PROCESS.AX_FLX_PLACE_BY_CITY_WEEK_pre_ALL_TODAY
            WHERE DUPLICATE_FILTER = 1
            GROUP BY 
                WEEK_YEAR_KEY,
                ZONE_ID,
                VISITOR_ORIGIN_ID,
                VISITOR_CATEGORY_ID,
                CITY_DES,
                PLACE_PROVINCE_DES,
                PLACE_CITY_DES,
                KPI_ID
            );
                /*
                      
                insert into PROCESS.RL_CITY_TRANSLATOR (
                
                WITH RAW AS (
                SELECT     
                    TRANSLATE(upper(place_city_des),''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔŔŃČŇĎ.;:-_/¿?¡!+*#& ,’()'', ''AEIOUAEIOUAEIOUOAÑEOI'') tra_city_des_JOIN, 
                    place_city_des
                    --,PLACE_PROVINCE_DES
                    from PROCESS.AX_FLX_PLACE_BY_CITY_WEEK_ALL_TODAY
                ),
                DIMENSION AS (
                select MAX(LOAD_DATE) 
                    TRANSLATE(upper(city_des),''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔŔŃČŇĎ.;:-_/¿?¡!+*#& ,’()'', ''AEIOUAEIOUAEIOUOAÑEOI'') city_DES_JOIN,
                    city_des,
                    city_id 
                from DEV_TOURISM_DB.SIT.DM_GLB_CITY
                )
                select
                    a.place_city_des RAW_city_DES,
                    --A.PLACE_PROVINCE_DES,
                    ''AX_FLX_PLACE_BY_CITY_WEEK_ALL_TODAY'' ORIGIN_DATA_ID,
                    nvl(max(b.city_des),''*********'') city_des,
                    nvl(max(b.city_id),''***'') city_id,
                    current_timestamp() LOAD_TIME 
                from RAW a
                LEFT JOIN DIMENSION b
                ON a.tra_city_des_JOIN=b.city_DES_JOIN
                
                LEFT JOIN PROCESS.RL_CITY_TRANSLATOR tra
                ON a.place_city_des=tra.RAW_city_DES
                
                WHERE tra.RAW_city_DES is null
                group by a.place_city_des--, A.PLACE_PROVINCE_DES
                );

                */

                INSERT INTO PROCESS.AX_FLX_PLACE_BY_CITY_WEEK
                    (
                    
                    WITH CITY_PROV AS (
                        SELECT CITY_DES, PROVINCE_DES, CITY_KEY
                        FROM SIT.DM_GLB_CITY CITY
                        INNER JOIN SIT.DM_GLB_PROVINCE PRO
                        ON CITY.PROVINCE_KEY = PRO.PROVINCE_KEY 
                    )
                    
                    select 
                        A.WEEK_YEAR_KEY,
                        nvl(z.ZONE_KEY,-3) ZONE_KEY,
                        nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                        nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                        nvl(CITY.city_key,-3) CITY_KEY,
                        trazip.ZIPCODE_ID,
                        nvl(CITY_PROV.city_key,-3) PLACE_CITY_KEY,
                        nvl(KPI_KEY,-3) KPI_KEY,
                        a.KPI_VALUE_NUM,
                        current_timestamp() LOAD_TIME
                        
                    FROM PROCESS.AX_FLX_PLACE_BY_CITY_WEEK_ALL_TODAY a
                    LEFT JOIN SIT.DM_FLX_ZONE z
                    ON trim(a.ZONE_ID)=trim(z.ZONE_ID)
                    
                    LEFT JOIN SIT.DM_GLB_VISITOR_CATEGORY C
                    ON a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID
                    
                    LEFT JOIN SIT.DM_FLX_VISITOR_ORIGIN O
                    ON a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID
                    
                    LEFT JOIN PROCESS.RL_CITY_TRANSLATOR TRA
                    ON a.PLACE_CITY_DES=TRA.RAW_CITY_DES
                    
                    LEFT JOIN CITY_PROV
                    ON TRA.CITY_DES=CITY_PROV.city_des
                    AND a.PLACE_PROVINCE_DES = CITY_PROV.PROVINCE_DES
                    
                    LEFT JOIN SIT.DM_FLX_ZIPCODE_NORM trazip
                    ON a.CITY_DES=trazip.city_des
                    
                    LEFT JOIN DEV_TOURISM_DB.SIT.DM_GLB_CITY CITY
                    ON trazip.city_id=CITY.city_id
                    
                    LEFT JOIN SIT.DM_IVE_KPI_MASTER KPI
                    ON a.kpi_id= kpi.kpi_id
                    
                    union all
                    
                    select 
                        A.WEEK_YEAR_KEY,
                        a.ZONE_KEY,
                        a.VISITOR_ORIGIN_KEY,
                        a.VISITOR_CATEGORY_KEY,
                        a.CITY_KEY,
                        a.ZIPCODE_ID,
                        a.PLACE_CITY_KEY,
                        a.KPI_KEY,
                        a.KPI_VALUE_NUM,
                        a.LOAD_TIME
                    from (select a.*,
                            z.zone_id,
                            kpi.kpi_id 
                            from SIT.FC_FLX_PLACE_BY_CITY_WEEK a,
                            SIT.DM_FLX_ZONE z,
                            SIT.DM_IVE_KPI_MASTER KPI 
                            where a.zone_key=z.zone_key and a.kpi_key=kpi.kpi_key) a,
                    (select distinct WEEK_YEAR_KEY, zone_ID,kpi_id from  PROCESS.AX_FLX_PLACE_BY_CITY_WEEK_ALL_TODAY) b
                    where a.WEEK_YEAR_KEY=b.WEEK_YEAR_KEY(+) 
                    and trim(a.zone_id)=trim(b.zone_id(+))
                    and a.KPI_id=b.kpi_id(+)
                    and b.WEEK_YEAR_KEY is null
                    );


              --END IF;
              --END;

               
              INSERT INTO PROCESS.AX_FLX_PLACE_BY_CITY_PRE_ALL_TODAY
              (
              WITH CONSOLIDADO AS (
                  select 
                      to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                      trim(zonaobservacion) AS zonaobservacion,
                      trim(origen) AS origen,
                      trim(categoriadelvisitante) AS categoriadelvisitante,
                      trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                  trim(ProvinciaZonaDiariaDiaVisita) AS PLACE_PROVINCE_DES,        
                  trim(a.MUNICIPIOZONADIARIADIAVISITA)  PLACE_CITY_DES ,        
                  ''LugarActividad_DiaVisita'' KPI_ID,
                      Volumen,
                      trim(continuo) continuo,
                      trim(A.LOAD_DATE) LOAD_DATE
                  from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_MUNICIPIO a
                  ,(select MAX(LOAD_DATE) LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_MUNICIPIO) M
                  where
                  a.load_date=m.load_date
                  AND (trim(A.CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
                  AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''
                  
                  union all
                  
                  select 
                      to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                      trim(zonaobservacion) AS zonaobservacion,
                      trim(origen) AS origen,
                      trim(categoriadelvisitante) AS categoriadelvisitante,
                      trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                  trim(ProvinciaZonaDiariaVispera) AS PLACE_PROVINCE_DES,        
                  trim(a.MUNICIPIOZONADIARIAVISPERA)  PLACE_CITY_DES,
                  ''LugarActividad_Vispera'' KPI_ID,
                      Volumen,
                      trim(continuo) continuo,
                      trim(A.LOAD_DATE) LOAD_DATE
                  from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_MUNICIPIO a
                  ,(select MAX(LOAD_DATE) LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_MUNICIPIO) M
                  where
                  a.load_date=m.load_date
                  AND (trim(A.CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
                  AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''
                  
                  union all
                  
                  select 
                      to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                      trim(zonaobservacion) AS zonaobservacion,
                      trim(origen) AS origen,
                      trim(categoriadelvisitante) AS categoriadelvisitante,
                      trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                  trim(ProvinciaZonaPernoctacionDiaVisita) AS PLACE_PROVINCE_DES, 
                  trim(a.MUNICIPIOZONAPERNOCTACIONDIAVISITA)  PLACE_CITY_DES,
                  ''LugarNoche_DiaVisita'' KPI_ID,
                      Volumen,
                      trim(continuo) continuo,
                      trim(A.LOAD_DATE) LOAD_DATE
                  from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_MUNICIPIO a
                  ,(select MAX(LOAD_DATE) LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_MUNICIPIO) M
                  where
                  a.load_date=m.load_date
                  AND (trim(A.CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
                  AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''
                  
                  union all
                  
                    select 
                      to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                      trim(zonaobservacion) AS zonaobservacion,
                      trim(origen) AS origen,
                      trim(categoriadelvisitante) AS categoriadelvisitante,
                      trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                  trim(ProvinciaZonaPernoctacionVispera) AS PLACE_PROVINCE_DES, 
                  trim(a.MUNICIPIOZONAPERNOCTACIONVISPERA)  PLACE_CITY_DES,
                  ''LugarNoche_Vispera'' KPI_ID,
                      Volumen,
                      trim(continuo) continuo,
                      trim(A.LOAD_DATE) LOAD_DATE
                  from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_MUNICIPIO a
                  ,(select MAX(LOAD_DATE) LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_MUNICIPIO) M
                  where
                  a.load_date=m.load_date
                  AND (trim(A.CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
                  AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''
              ),

              CONTINUO AS (
                  select 
                      to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                      trim(zonaobservacion) AS zonaobservacion,
                      trim(origen) AS origen,
                      trim(categoriadelvisitante) AS categoriadelvisitante,
                      trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                  trim(ProvinciaZonaDiariaDiaVisita) AS PLACE_PROVINCE_DES,        
                  trim(a.MUNICIPIOZONADIARIADIAVISITA)  PLACE_CITY_DES ,        
                  ''LugarActividad_DiaVisita'' KPI_ID,
                      Volumen,
                      trim(continuo) continuo,
                      trim(A.LOAD_DATE) LOAD_DATE
                  from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_MUNICIPIO a
                  ,(select MAX(LOAD_DATE) LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_MUNICIPIO) M
                  where
                  a.load_date=m.load_date  
                  AND (trim(A.CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
                  AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''    
                  
                  union all
                  
                  select 
                      to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                      trim(zonaobservacion) AS zonaobservacion,
                      trim(origen) AS origen,
                      trim(categoriadelvisitante) AS categoriadelvisitante,
                      trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                  trim(ProvinciaZonaDiariaVispera) AS PLACE_PROVINCE_DES,        
                  trim(a.MUNICIPIOZONADIARIAVISPERA)  PLACE_CITY_DES,
                  ''LugarActividad_Vispera'' KPI_ID,
                      Volumen,
                      trim(continuo) continuo,
                      trim(A.LOAD_DATE) LOAD_DATE
                  from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_MUNICIPIO a
                  ,(select MAX(LOAD_DATE) LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_MUNICIPIO) M
                  where
                  a.load_date=m.load_date  
                  AND (trim(A.CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
                  AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''    
                  
                  union all
                  
                  select 
                      to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                      trim(zonaobservacion) AS zonaobservacion,
                      trim(origen) AS origen,
                      trim(categoriadelvisitante) AS categoriadelvisitante,
                      trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                  trim(ProvinciaZonaPernoctacionDiaVisita) AS PLACE_PROVINCE_DES, 
                  trim(a.MUNICIPIOZONAPERNOCTACIONDIAVISITA)  PLACE_CITY_DES,
                  ''LugarNoche_DiaVisita'' KPI_ID,
                      Volumen,
                      trim(continuo) continuo,
                      trim(A.LOAD_DATE) LOAD_DATE
                  from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_MUNICIPIO a
                  ,(select MAX(LOAD_DATE) LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_MUNICIPIO) M
                  where
                  a.load_date=m.load_date  
                  AND (trim(A.CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
                  AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''    
                  
                  union all
                  
                    select 
                      to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                      trim(zonaobservacion) AS zonaobservacion,
                      trim(origen) AS origen,
                      trim(categoriadelvisitante) AS categoriadelvisitante,
                      trim(NOMBREMUNICIPIO) NOMBREMUNICIPIO,
                  trim(ProvinciaZonaPernoctacionVispera) AS PLACE_PROVINCE_DES, 
                  trim(a.MUNICIPIOZONAPERNOCTACIONVISPERA) PLACE_CITY_DES,
                  ''LugarNoche_Vispera'' KPI_ID,
                      Volumen,
                      trim(continuo) continuo,
                      trim(A.LOAD_DATE) LOAD_DATE
                  from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_MUNICIPIO a
                  ,(select MAX(LOAD_DATE) LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_MUNICIPIO) M
                  where
                  a.load_date=m.load_date  
                  AND (trim(A.CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
                  AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''    
              ),

              TOTAL AS (
                  SELECT 
                      period_key,
                      zonaobservacion ZONE_ID,
                      origen VISITOR_ORIGIN_ID,
                      categoriadelvisitante VISITOR_CATEGORY_ID,
                      NombreMunicipio  CITY_DES,
                  DECODE(PLACE_PROVINCE_DES, 
                    ''Alicante'', ''Alicante/Alacant'',
                    ''Castellón'', ''Castellón/Castelló'',
                    ''Illes Balears'', ''Islas Baleares'',
                    ''Valencia'', ''Valencia/València'', 
                    PLACE_PROVINCE_DES) AS PLACE_PROVINCE_DES,
                  PLACE_CITY_DES,
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
                      C.NombreMunicipio  CITY_DES,
                  DECODE(C.PLACE_PROVINCE_DES, 
                    ''Alicante'', ''Alicante/Alacant'',
                    ''Castellón'', ''Castellón/Castelló'',
                    ''Illes Balears'', ''Islas Baleares'',
                    ''Valencia'', ''Valencia/València'', 
                    C.PLACE_PROVINCE_DES) AS PLACE_PROVINCE_DES,
                  C.PLACE_CITY_DES,
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
                  CITY_DES,
                PLACE_PROVINCE_DES,
                PLACE_CITY_DES,
                  KPI_ID,
                  KPI_VALUE_NUM,     
                  CONTINUO,
                  LOAD_DATE AS FECHA_CARGA,     
                    row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,PLACE_PROVINCE_DES, PLACE_CITY_DES, CITY_DES ,KPI_ID ,KPI_VALUE_NUM order by period_key) AS DUPLICATE_FILTER ,    
                  row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,PLACE_PROVINCE_DES, PLACE_CITY_DES, CITY_DES ,KPI_ID  order by LOAD_DATE DESC) AS HYBRID_FILTER ,    
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
              FROM PROCESS.AX_FLX_PLACE_BY_CITY_PRE_ALL_TODAY 
              WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

              -- Verificar el valor de la comprobación inicial
              IF (COMPROBACION_INICIAL > 0) THEN
              -- Registrar el problema en la tabla de logs
              INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
              SELECT ''AX_FLX_PLACE_BY_CITY_PRE_ALL_TODAY'' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
              FROM PROCESS.AX_FLX_PLACE_BY_CITY_PRE_ALL_TODAY 
              WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

              -- Finalizar el proceso con un mensaje indicando el error
              RETURN ''Proceso cancelado: Se detectaron duplicados en AX_FLX_PLACE_BY_CITY_PRE_ALL_TODAY. Total: '' || COMPROBACION_INICIAL;
              ELSE

                   */
                  INSERT INTO PROCESS.AX_FLX_PLACE_BY_CITY_ALL_TODAY
                  (
                  SELECT 
                      period_key,
                      ZONE_ID,
                      VISITOR_ORIGIN_ID,
                      VISITOR_CATEGORY_ID,
                      CITY_DES,
                    PLACE_PROVINCE_DES,
                    PLACE_CITY_DES,
                      KPI_ID,
                      KPI_VALUE_NUM,
                      current_timestamp() LOAD_TIME
                  FROM PROCESS.AX_FLX_PLACE_BY_CITY_PRE_ALL_TODAY     
                  WHERE DUPLICATE_FILTER = 1
                  );
                  
                  /*
                  insert into PROCESS.RL_CITY_TRANSLATOR (
                  
                  WITH RAW AS (
                  SELECT     
                      TRANSLATE(upper(place_city_des),''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔŔŃČŇĎ.;:-_/¿?¡!+*#& ,’()'', ''AEIOUAEIOUAEIOUOAÑEOI'') tra_city_des_JOIN, 
                      place_city_des
                      ,PLACE_PROVINCE_DES
                      from PROCESS.AX_FLX_PLACE_BY_CITY_ALL_TODAY
                  ),
                  DIMENSION AS (
                  select MAX(LOAD_DATE) 
                      TRANSLATE(upper(city_des),''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔŔŃČŇĎ.;:-_/¿?¡!+*#& ,’()'', ''AEIOUAEIOUAEIOUOAÑEOI'') city_DES_JOIN,
                      city_des,
                      city_id 
                  from SIT.DM_GLB_CITY
                  )
                  select
                      a.place_city_des RAW_city_DES,
                      --A.PLACE_PROVINCE_DES,
                      ''AX_FLX_PLACE_BY_CITY_ALL_TODAY'' ORIGIN_DATA_ID,
                      nvl(max(b.city_des),''*********'') city_des,
                      nvl(max(b.city_id),''***'') city_id,
                      current_timestamp() LOAD_TIME 
                  from RAW a
                  LEFT JOIN DIMENSION b
                  ON a.tra_city_des_JOIN=b.city_DES_JOIN
                  
                  LEFT JOIN PROCESS.RL_CITY_TRANSLATOR tra
                  ON a.place_city_des=tra.RAW_city_DES
                  
                  WHERE tra.RAW_city_DES is null
                  group by a.place_city_des--, A.PLACE_PROVINCE_DES
                  );
                    */            
                  INSERT INTO PROCESS.AX_FLX_PLACE_BY_CITY
                  (
                  
                  WITH CITY_PROV AS (
                      SELECT CITY_DES, PROVINCE_DES, CITY_KEY
                      FROM SIT.DM_GLB_CITY CITY
                      INNER JOIN SIT.DM_GLB_PROVINCE PRO
                      ON CITY.PROVINCE_KEY = PRO.PROVINCE_KEY 
                  )
                  
                  select 
                      a.period_key,
                      nvl(z.ZONE_KEY,-3) ZONE_KEY,
                      nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                      nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                      nvl(CITY.city_key,-3) CITY_KEY,
                      trazip.ZIPCODE_ID,
                      nvl(CITY_PROV.city_key,-3) PLACE_CITY_KEY,
                      nvl(KPI_KEY,-3) KPI_KEY,
                      a.KPI_VALUE_NUM,
                      current_timestamp() LOAD_TIME
                      
                  FROM PROCESS.AX_FLX_PLACE_BY_CITY_ALL_TODAY a
                  LEFT JOIN SIT.DM_FLX_ZONE z
                  ON trim(a.ZONE_ID)=trim(z.ZONE_ID)
                  
                  LEFT JOIN SIT.DM_GLB_VISITOR_CATEGORY C
                  ON a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID
                  
                  LEFT JOIN SIT.DM_FLX_VISITOR_ORIGIN O
                  ON a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID
                  
                  LEFT JOIN PROCESS.RL_CITY_TRANSLATOR TRA
                  ON a.PLACE_CITY_DES=TRA.RAW_CITY_DES
                  
                  LEFT JOIN CITY_PROV
                  ON TRA.CITY_DES=CITY_PROV.city_des
                  AND a.PLACE_PROVINCE_DES = CITY_PROV.PROVINCE_DES
                  
                  LEFT JOIN SIT.DM_FLX_ZIPCODE_NORM trazip
                  ON a.CITY_DES=trazip.city_des
                  
                  LEFT JOIN DEV_TOURISM_DB.SIT.DM_GLB_CITY CITY
                  ON trazip.city_id=CITY.city_id
                  
                  LEFT JOIN SIT.DM_IVE_KPI_MASTER KPI
                  ON a.kpi_id= kpi.kpi_id
                  
                  WHERE a.city_des !=''Acumulado''
                  
                  union all
                  
                  select 
                      a.PERIOD_KEY, 
                      a.ZONE_KEY,
                      a.VISITOR_ORIGIN_KEY,
                      a.VISITOR_CATEGORY_KEY,
                      a.CITY_KEY,
                      a.ZIPCODE_ID,
                      a.PLACE_CITY_KEY,
                      a.KPI_KEY,
                      a.KPI_VALUE_NUM,
                      a.LOAD_TIME
                  from (select a.*,
                          z.zone_id,
                          kpi.kpi_id 
                          from SIT.FC_FLX_PLACE_BY_CITY a,
                          SIT.DM_FLX_ZONE z,
                          SIT.DM_IVE_KPI_MASTER KPI 
                          where a.zone_key=z.zone_key and a.kpi_key=kpi.kpi_key) a,
                  (select MAX(LOAD_DATE) PERIOD_KEY,zone_ID,kpi_id from  PROCESS.AX_FLX_PLACE_BY_CITY_ALL_TODAY) b
                  where a.period_key=b.period_key(+) 
                  and trim(a.zone_id)=trim(b.zone_id(+))
                  and a.KPI_id=b.kpi_id(+)
                  and b.period_key is null
                  );
              
              --END IF;
              --END;

              
              INSERT INTO PROCESS.AX_FLX_PLACE_BY_COUNTRY_WEEK_PRE_ALL_TODAY
                (
                WITH CONSOLIDADO AS (
                    select 
                        TRIM(SEMANA) AS SEMANA,
                        trim(zonaobservacion) AS zonaobservacion,
                        trim(origen) AS origen,
                        trim(categoriadelvisitante) AS categoriadelvisitante,
                        trim(pais) AS pais,
                        trim(ProvinciaZonaDiariaDiaVisita) AS PLACE_PROVINCE_DES,        
                        trim(MUNICIPIOZONADIARIADIAVISITA) AS PLACE_CITY_DES ,        
                        'LugarActividad_DiaVisita' KPI_ID,
                        Volumen,
                        trim(continuo) continuo,
                        trim(A.LOAD_DATE) LOAD_DATE,
                        trim(FILE_NAME) FILE_NAME
                    from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_NACIONALIDAD_SEMANA a
                    ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_NACIONALIDAD_SEMANA) M
                    where
                    a.load_date=m.load_date
                    AND (trim(A.CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
                    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
                    
                    union all
                    
                    select 
                        TRIM(SEMANA) AS SEMANA,
                        trim(zonaobservacion) AS zonaobservacion,
                        trim(origen) AS origen,
                        trim(categoriadelvisitante) AS categoriadelvisitante,
                        trim(pais) AS pais,
                        trim(ProvinciaZonaDiariaVispera) AS PLACE_PROVINCE_DES,        
                        trim(a.MUNICIPIOZONADIARIAVISPERA)  PLACE_CITY_DES,
                        'LugarActividad_Vispera' KPI_ID,
                        Volumen,
                        trim(continuo) continuo,
                        trim(A.LOAD_DATE) LOAD_DATE,
                        trim(FILE_NAME) FILE_NAME
                    from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_NACIONALIDAD_SEMANA a
                    ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_NACIONALIDAD_SEMANA) M
                    where a.load_date=m.load_date 
                    AND (trim(A.CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
                    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
                    
                    union all
                    
                    select 
                        TRIM(SEMANA) AS SEMANA,
                        trim(zonaobservacion) AS zonaobservacion,
                        trim(origen) AS origen,
                        trim(categoriadelvisitante) AS categoriadelvisitante,
                        trim(pais) AS pais,
                        trim(ProvinciaZonaPernoctacionDiaVisita) AS PLACE_PROVINCE_DES, 
                        trim(a.MUNICIPIOZONAPERNOCTACIONDIAVISITA)  PLACE_CITY_DES,
                        'LugarNoche_DiaVisita' KPI_ID,
                        Volumen,
                        trim(continuo) continuo,
                        trim(A.LOAD_DATE) LOAD_DATE,
                        trim(FILE_NAME) FILE_NAME
                    from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_NACIONALIDAD_SEMANA a
                    ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_NACIONALIDAD_SEMANA) M
                    where a.load_date=m.load_date
                    AND (trim(A.CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
                    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
                    
                    union all
                    
                    select 
                        TRIM(SEMANA) AS SEMANA,
                        trim(zonaobservacion) AS zonaobservacion,
                        trim(origen) AS origen,
                        trim(categoriadelvisitante) AS categoriadelvisitante,
                        trim(pais) AS pais,
                        trim(ProvinciaZonaPernoctacionVispera) AS PLACE_PROVINCE_DES, 
                        trim(a.MUNICIPIOZONAPERNOCTACIONVISPERA)  PLACE_CITY_DES,
                        'LugarNoche_Vispera' KPI_ID,
                        Volumen,
                        trim(continuo) continuo,
                        trim(A.LOAD_DATE) LOAD_DATE,
                        trim(FILE_NAME) FILE_NAME
                    from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_NACIONALIDAD_SEMANA a
                    ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_NACIONALIDAD_SEMANA) M
                    where  a.load_date=m.load_date
                    AND (trim(A.CONTINUO) = 'nc' AND FILE_NAME NOT LIKE '%INVA-1080-6642%')
                    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'
                ),

                CONTINUO AS (
                    select 
                        TRIM(SEMANA) AS SEMANA,
                        trim(zonaobservacion) AS zonaobservacion,
                        trim(origen) AS origen,
                        trim(categoriadelvisitante) AS categoriadelvisitante,
                        trim(pais) AS pais,
                        trim(ProvinciaZonaDiariaDiaVisita) AS PLACE_PROVINCE_DES,        
                        trim(a.MUNICIPIOZONADIARIADIAVISITA)  PLACE_CITY_DES ,        
                        'LugarActividad_DiaVisita' KPI_ID,
                        Volumen,
                        trim(continuo) continuo,
                        trim(A.LOAD_DATE) LOAD_DATE,
                        trim(FILE_NAME) FILE_NAME
                    from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_NACIONALIDAD_SEMANA a
                    ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_DIAVISITA_NACIONALIDAD_SEMANA) M
                    where
                    a.load_date=m.load_date  
                    AND (trim(A.CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
                    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'    
                    
                    union all
                    
                    select 
                        TRIM(SEMANA) AS SEMANA,
                        trim(zonaobservacion) AS zonaobservacion,
                        trim(origen) AS origen,
                        trim(categoriadelvisitante) AS categoriadelvisitante,
                        trim(pais) AS pais,
                        trim(ProvinciaZonaDiariaVispera) AS PLACE_PROVINCE_DES,        
                        trim(a.MUNICIPIOZONADIARIAVISPERA)  PLACE_CITY_DES,
                        'LugarActividad_Vispera' KPI_ID,
                        Volumen,
                        trim(continuo) continuo,
                        trim(A.LOAD_DATE) LOAD_DATE,
                        trim(FILE_NAME) FILE_NAME
                    from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_NACIONALIDAD_SEMANA a
                    ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARACTIVIDAD_VISPERA_NACIONALIDAD_SEMANA) M
                    where a.load_date=m.load_date
                    AND (trim(A.CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
                    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'    
                    
                    union all
                    
                    select 
                        TRIM(SEMANA) AS SEMANA,
                        trim(zonaobservacion) AS zonaobservacion,
                        trim(origen) AS origen,
                        trim(categoriadelvisitante) AS categoriadelvisitante,
                        trim(pais) AS pais,
                        trim(ProvinciaZonaPernoctacionDiaVisita) AS PLACE_PROVINCE_DES, 
                        trim(a.MUNICIPIOZONAPERNOCTACIONDIAVISITA)  PLACE_CITY_DES,
                        'LugarNoche_DiaVisita' KPI_ID,
                        Volumen,
                        trim(continuo) continuo,
                        trim(A.LOAD_DATE) LOAD_DATE,
                        trim(FILE_NAME) FILE_NAME
                    from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_NACIONALIDAD_SEMANA a
                    ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_DIAVISITA_NACIONALIDAD_SEMANA) M
                    where a.load_date=m.load_date
                    AND (trim(A.CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
                    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'     
                    
                    union all
                    
                    select 
                        TRIM(SEMANA) AS SEMANA,
                        trim(zonaobservacion) AS zonaobservacion,
                        trim(origen) AS origen,
                        trim(categoriadelvisitante) AS categoriadelvisitante,
                        trim(pais) AS pais,
                        trim(ProvinciaZonaPernoctacionVispera) AS PLACE_PROVINCE_DES, 
                        trim(a.MUNICIPIOZONAPERNOCTACIONVISPERA)  PLACE_CITY_DES,
                        'LugarNoche_Vispera' KPI_ID,
                        Volumen,
                        trim(continuo) continuo,
                        trim(A.LOAD_DATE) LOAD_DATE,
                        trim(FILE_NAME) FILE_NAME
                    from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_NACIONALIDAD_SEMANA a
                    ,(select DISTINCT LOAD_DATE from DEV_TOURISM_DB.STAGING.ST_FLX_LUGARNOCHE_VISPERA_NACIONALIDAD_SEMANA) M
                    where a.load_date=m.load_date
                    AND (trim(A.CONTINUO) = 'c' OR FILE_NAME LIKE '%INVA-1080-6642%')
                    AND FILE_NAME NOT LIKE '%INVA-1080-9999%'       
                ),

                TOTAL AS (
                    SELECT 
                        SUBSTRING(SEMANA, 1, 4) || (CASE WHEN LENGTH(SUBSTRING(SEMANA, 6, LENGTH(SEMANA) - 5)) = 1 THEN '0' || SUBSTRING(SEMANA, 6, LENGTH(SEMANA) - 5) 
                            ELSE SUBSTRING(SEMANA, 6, LENGTH(SEMANA) - 5) END)  AS WEEK_YEAR_KEY,
                        zonaobservacion ZONE_ID,
                        origen VISITOR_ORIGIN_ID,
                        categoriadelvisitante VISITOR_CATEGORY_ID,
                        pais  country_des,
                        DECODE(PLACE_PROVINCE_DES, 
                            'Alicante', 'Alicante/Alacant',
                            'Castellón', 'Castellón/Castelló',
                            'Illes Balears', 'Islas Baleares',
                            'Valencia', 'Valencia/València', 
                        PLACE_PROVINCE_DES) AS PLACE_PROVINCE_DES,
                        PLACE_CITY_DES,
                        KPI_ID,
                        Volumen KPI_VALUE_NUM,
                        continuo,
                        LOAD_DATE ,
                        REPLACE(FILE_NAME, 'San_Bartolome_de_Tirajana_Municipio_', 'San_Bartolome_de_Tirajana_Municipio' ) AS FILE_NAME
                    FROM CONSOLIDADO
                    
                    UNION ALL
                    
                    SELECT 
                        SUBSTRING(C.SEMANA, 1, 4) || (CASE WHEN LENGTH(SUBSTRING(C.SEMANA, 6, LENGTH(C.SEMANA) - 5)) = 1 THEN '0' || SUBSTRING(C.SEMANA, 6, LENGTH(C.SEMANA) - 5) 
                            ELSE SUBSTRING(C.SEMANA, 6, LENGTH(C.SEMANA) - 5) END) AS WEEK_YEAR_KEY,			
                        C.zonaobservacion ZONE_ID,
                        C.origen VISITOR_ORIGIN_ID,
                        C.categoriadelvisitante VISITOR_CATEGORY_ID,
                        C.pais  country_des,
                        DECODE(C.PLACE_PROVINCE_DES, 
                            'Alicante', 'Alicante/Alacant',
                            'Castellón', 'Castellón/Castelló',
                            'Illes Balears', 'Islas Baleares',
                            'Valencia', 'Valencia/València', 
                        C.PLACE_PROVINCE_DES) AS PLACE_PROVINCE_DES,
                        C.PLACE_CITY_DES,
                        C.KPI_ID,
                        C.Volumen KPI_VALUE_NUM,
                        C.continuo,
                        C.LOAD_DATE,
                        REPLACE(C.FILE_NAME, 'San_Bartolome_de_Tirajana_Municipio_', 'San_Bartolome_de_Tirajana_Municipio' ) AS FILE_NAME
                    FROM CONTINUO C
                    LEFT JOIN CONSOLIDADO NC
                    ON C.SEMANA = NC.SEMANA
                    AND C.zonaobservacion = NC.zonaobservacion
                    AND C.KPI_ID = NC.KPI_ID
                    WHERE NC.SEMANA IS NULL
                )

                SELECT 
                    WEEK_YEAR_KEY,
                    ZONE_ID,
                    VISITOR_ORIGIN_ID,
                    VISITOR_CATEGORY_ID,
                    country_des,
                    PLACE_PROVINCE_DES,
                    PLACE_CITY_DES,
                    KPI_ID,
                    KPI_VALUE_NUM,  
                    CONTINUO,
                    LOAD_DATE AS FECHA_CARGA, 
                    FILE_NAME,
                    row_number() over (partition by WEEK_YEAR_KEY, ZONE_ID ,VISITOR_ORIGIN_ID, VISITOR_CATEGORY_ID, country_des, PLACE_PROVINCE_DES, PLACE_CITY_DES, KPI_ID, FILE_NAME order by LOAD_DATE desc) AS DUPLICATE_FILTER, 
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
              FROM PROCESS.AX_FLX_PLACE_BY_COUNTRY_WEEK_PRE_ALL_TODAY 
              WHERE  DUPLICATE_FILTER = 1;

              -- Verificar el valor de la comprobación inicial
              IF (COMPROBACION_INICIAL > 0) THEN
              -- Registrar el problema en la tabla de logs
              INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
              SELECT ''AX_FLX_PLACE_BY_COUNTRY_WEEK_PRE_ALL_TODAY'' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
              FROM PROCESS.AX_FLX_PLACE_BY_COUNTRY_WEEK_PRE_ALL_TODAY 
              WHERE  DUPLICATE_FILTER = 1;

              -- Finalizar el proceso con un mensaje indicando el error
              RETURN ''Proceso cancelado: Se detectaron duplicados en AX_FLX_PLACE_BY_COUNTRY_WEEK_PRE_ALL_TODAY. Total: '' || COMPROBACION_INICIAL;
              ELSE

                  */
                INSERT INTO PROCESS.AX_FLX_PLACE_BY_COUNTRY_WEEK_ALL_TODAY
                    (select 
                        WEEK_YEAR_KEY,
                        ZONE_ID,
                        VISITOR_ORIGIN_ID,
                        VISITOR_CATEGORY_ID,
                        COUNTRY_DES,
                        PLACE_PROVINCE_DES,
                        PLACE_CITY_DES,
                        KPI_ID,
                        SUM(KPI_VALUE_NUM) AS KPI_VALUE_NUM,  
                        current_timestamp() LOAD_TIME
                    from PROCESS.AX_FLX_PLACE_BY_COUNTRY_WEEK_PRE_ALL_TODAY
                    WHERE DUPLICATE_FILTER = 1
                    GROUP BY 
                        WEEK_YEAR_KEY,
                        ZONE_ID,
                        VISITOR_ORIGIN_ID,
                        VISITOR_CATEGORY_ID,
                        COUNTRY_DES,
                        PLACE_PROVINCE_DES,
                        PLACE_CITY_DES,
                        KPI_ID);
                        /*
                    insert into PROCESS.RL_COUNTRY_TRANSLATOR (
                    
                    WITH RAW AS (
                    select 
                        TRANSLATE(upper(COUNTRY_DES),''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’'', ''AEIOUAEIOUAEIOUO'') tra_country_des_JOIN,
                        country_DES 
                    from PROCESS.AX_FLX_PLACE_BY_COUNTRY_WEEK_ALL_TODAY),
                    DIM AS (
                    select MAX(LOAD_DATE) 
                        TRANSLATE(upper(a.COUNTRY_DES),''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔ.;:-_/¿?¡!+*#& ,’'', ''AEIOUAEIOUAEIOUO'') COUNTRY_DES_JOIN,
                        a.country_des,
                        country_id 
                    from DEV_TOURISM_DB.SIT.DM_GLB_COUNTRY_MLG a,
                        DEV_TOURISM_DB.SIT.DM_GLB_COUNTRY b 
                    where a.country_key=b.country_key )
                    
                    select 
                        a.COUNTRY_DES RAW_COUNTRY_DES,
                        ''AX_FLX_PLACE_BY_COUNTRY_WEEK_ALL_TODAY'' ORIGIN_DATA_ID,
                        nvl(max(b.country_des),''*********'') country_des,
                        nvl(max(b.country_id),''***'') country_id
                        ,current_timestamp() LOAD_TIME 
                    from RAW A 
                    LEFT JOIN DIM b
                    ON a.tra_country_des_JOIN=b.COUNTRY_DES_JOIN
                    LEFT JOIN PROCESS.RL_COUNTRY_TRANSLATOR tra
                    ON a.COUNTRY_DES=tra.RAW_COUNTRY_DES
                    
                    WHERE tra.RAW_COUNTRY_DES is null
                    group by a.COUNTRY_DES
                    );
                         */   
                         /*
                    insert into PROCESS.RL_CITY_TRANSLATOR (
                    
                    WITH RAW AS (
                    SELECT     
                        TRANSLATE(upper(place_city_des),''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔŔŃČŇĎ.;:-_/¿?¡!+*#& ,’()'', ''AEIOUAEIOUAEIOUOAÑEOI'') tra_city_des_JOIN, 
                        place_city_des
                        --,PLACE_PROVINCE_DES
                        from PROCESS.AX_FLX_PLACE_BY_COUNTRY_WEEK_ALL_TODAY
                    ),
                    DIMENSION AS (
                    select MAX(LOAD_DATE)
                        TRANSLATE(upper(city_des),''ÁÉÍÓÚÀÈÌÒÙÄËÏÖÜÔŔŃČŇĎ.;:-_/¿?¡!+*#& ,’()'', ''AEIOUAEIOUAEIOUOAÑEOI'') city_DES_JOIN,
                        city_des,
                        city_id 
                    from DEV_TOURISM_DB.SIT.DM_GLB_CITY
                    )
                    select
                        a.place_city_des RAW_city_DES,
                        --A.PLACE_PROVINCE_DES,
                        ''AX_FLX_PLACE_BY_COUNTRY_WEEK_ALL_TODAY'' ORIGIN_DATA_ID,
                        nvl(max(b.city_des),''*********'') city_des,
                        nvl(max(b.city_id),''***'') city_id,
                        current_timestamp() LOAD_TIME 
                    from RAW a
                    LEFT JOIN DIMENSION b
                    ON a.tra_city_des_JOIN=b.city_DES_JOIN
                    
                    LEFT JOIN PROCESS.RL_CITY_TRANSLATOR tra
                    ON a.place_city_des=tra.RAW_city_DES
                    
                    WHERE tra.RAW_city_DES is null
                    group by a.place_city_des--, A.PLACE_PROVINCE_DES
                    );
*/
                    INSERT INTO PROCESS.AX_FLX_PLACE_BY_COUNTRY_WEEK
                        (
                        
                        WITH CITY_PROV AS (
                            SELECT CITY_DES, PROVINCE_DES, CITY_KEY
                            FROM SIT.DM_GLB_CITY CITY
                            INNER JOIN SIT.DM_GLB_PROVINCE PRO
                            ON CITY.PROVINCE_KEY = PRO.PROVINCE_KEY 
                        )
                        
                        select 
                            WEEK_YEAR_KEY,
                            nvl(z.ZONE_KEY,-3) ZONE_KEY,
                            nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                            nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                            nvl(country.country_key,-3) country_key,
                            nvl(CITY_PROV.city_key,-3) PLACE_CITY_KEY,
                            nvl(KPI_KEY,-3) KPI_KEY,
                            a.KPI_VALUE_NUM,
                            current_timestamp() LOAD_TIME
                        from PROCESS.AX_FLX_PLACE_BY_COUNTRY_WEEK_ALL_TODAY a
                        LEFT JOIN SIT.DM_FLX_ZONE z
                        ON trim(a.ZONE_ID)=trim(z.ZONE_ID)
                        
                        LEFT JOIN SIT.DM_GLB_VISITOR_CATEGORY C
                        ON a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID
                        
                        LEFT JOIN SIT.DM_FLX_VISITOR_ORIGIN O
                        ON a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID
                        
                        -------PAÍS
                        LEFT JOIN PROCESS.RL_COUNTRY_TRANSLATOR TRA
                        ON a.country_des=tra.raw_country_des
                        
                        LEFT JOIN DEV_TOURISM_DB.SIT.DM_GLB_COUNTRY country
                        ON tra.country_id=country.country_id
                        
                        -------CITY
                        LEFT JOIN PROCESS.RL_CITY_TRANSLATOR TRA_CITY
                        ON a.PLACE_CITY_DES=TRA_CITY.RAW_CITY_DES
                        
                        LEFT JOIN CITY_PROV
                        ON TRA_CITY.CITY_DES=CITY_PROV.city_des
                        AND a.PLACE_PROVINCE_DES = CITY_PROV.PROVINCE_DES
                        
                        LEFT JOIN SIT.DM_IVE_KPI_MASTER KPI
                        ON a.kpi_id= kpi.kpi_id
                        
                        union all
                        
                        SELECT
                            A.WEEK_YEAR_KEY,
                            a.ZONE_KEY,
                            a.VISITOR_ORIGIN_KEY,
                            a.VISITOR_CATEGORY_KEY,
                            a.COUNTRY_KEY,
                            a.PLACE_CITY_KEY,
                            a.KPI_KEY,
                            a.KPI_VALUE_NUM,
                            a.LOAD_TIME
                        from (select 
                                a.*,
                                z.zone_id,
                                kpi.kpi_id 
                                from SIT.FC_FLX_PLACE_BY_COUNTRY_WEEK a,
                                SIT.DM_FLX_ZONE z,
                                SIT.DM_IVE_KPI_MASTER KPI 
                                where a.zone_key=z.zone_key and a.kpi_key=kpi.kpi_key) a,
                        (select distinct 
                                WEEK_YEAR_KEY,
                                zone_ID,
                                kpi_id 
                                from PROCESS.AX_FLX_PLACE_BY_COUNTRY_WEEK_ALL_TODAY) b
                        where a.WEEK_YEAR_KEY=b.WEEK_YEAR_KEY(+)
                        and trim(a.zone_id)=trim(b.zone_id(+))
                        and a.KPI_id=b.kpi_id(+)
                        and b.WEEK_YEAR_KEY is null
                        );
                  
                  --END IF;
                  --END;

            END;
             RETURN ''Task LOAD_AX_FLX_PLACE_V1 creada correctamente'';
    END;
';

