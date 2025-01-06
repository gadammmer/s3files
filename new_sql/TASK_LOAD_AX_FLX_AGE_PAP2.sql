
EXECUTE IMMEDIATE '
    BEGIN
        create or replace task PROCESS.LOAD_AX_FLX_AGE
          USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
          after PROCESS.LOAD_AX_FLX_2H
          as BEGIN

            INSERT INTO PROCESS.AX_FLX_BY_AGE_MONTH_PRE_ALL_TODAY
            (
            WITH CONSOLIDADO AS (
            select 
                TRIM(MES) MES,
                TRIM(zonaobservacion) AS zonaobservacion,
                TRIM(origen) AS origen,
                TRIM(categoriadelvisitante) AS categoriadelvisitante,
                TRIM(Edad) AS EDAD,
                ''Diario'' KPI_ID,
                Volumen,
                trim(continuo) continuo,
                trim(A.LOAD_DATE) LOAD_DATE  
            from STAGING.ST_FLX_DIARIO_MES_EDAD a
            ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_MES_EDAD) M
            where a.load_date=m.load_date  
            AND (trim(CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
            AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''   

            union all

            select 
                TRIM(MES) MES,
                trim(zonaobservacion) AS zonaobservacion,
                trim(origen) AS origen,
                trim(categoriadelvisitante) AS categoriadelvisitante,
                TRIM(Edad) AS EDAD,
                ''Nocturno'' KPI_ID,
                Volumen,
                trim(continuo) continuo,
                trim(A.LOAD_DATE) LOAD_DATE
            from STAGING.ST_FLX_NOCTURNO_MES_EDAD a
            ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCTURNO_MES_EDAD) M
            where a.load_date=m.load_date  
            AND (trim(CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
            AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''   
            ),

            CONTINUO AS (
            select 
                TRIM(MES) MES,
                trim(zonaobservacion) AS zonaobservacion,
                trim(origen) AS origen,
                trim(categoriadelvisitante) AS categoriadelvisitante,
                TRIM(Edad) AS EDAD,
                ''Diario'' KPI_ID,
                Volumen,
                trim(continuo) continuo,
                trim(A.LOAD_DATE) LOAD_DATE
            from STAGING.ST_FLX_DIARIO_MES_EDAD a
            ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_MES_EDAD) M
            where a.load_date=m.load_date  
            AND (trim(CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
            AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''

            union all

            select 
                TRIM(MES) MES,
                trim(zonaobservacion) AS zonaobservacion,
                trim(origen) AS origen,
                trim(categoriadelvisitante) AS categoriadelvisitante,
                TRIM(Edad) AS EDAD,
                ''Nocturno'' KPI_ID,
                Volumen,
                trim(continuo) continuo,
                trim(A.LOAD_DATE) LOAD_DATE
            from STAGING.ST_FLX_NOCTURNO_MES_EDAD a
            ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCTURNO_MES_EDAD) M
            where a.load_date=m.load_date  
            AND (trim(CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
            AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''
            ),

            TOTAL AS (
            SELECT 
                CAST(SUBSTRING(MES, 1, 4) AS INTEGER) YEAR_KEY,
                CAST(SUBSTRING(MES, 5, 2) AS INTEGER) MONTH_KEY,
                zonaobservacion ZONE_ID,
                origen VISITOR_ORIGIN_ID,
                categoriadelvisitante VISITOR_CATEGORY_ID,
                CASE WHEN trim(EDAD)=''65 o más'' THEN ''65+'' 
                    WHEN trim(EDAD)=''<18'' THEN ''14-17'' 
                ELSE trim(EDAD) END AGE_ID,
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
                CASE WHEN trim(C.EDAD)=''65 o más'' THEN ''65+'' 
                    WHEN trim(C.EDAD)=''<18'' THEN ''14-17'' 
                ELSE trim(C.EDAD) END AGE_ID,
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
            AGE_ID,
            KPI_ID,
            KPI_VALUE_NUM,
            CONTINUO,
            LOAD_DATE AS FECHA_CARGA,       
            row_number() over (partition by YEAR_KEY, MONTH_KEY,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,AGE_ID ,KPI_ID ,KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER ,  
            row_number() over (partition by YEAR_KEY, MONTH_KEY,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,AGE_ID ,KPI_ID order by LOAD_DATE DESC) AS HYBRID_FILTER ,         
            current_timestamp() LOAD_TIME
            FROM TOTAL

            );

            DECLARE
            COMPROBACION_INICIAL INT;
            BEGIN
            -- Comprobar si existen duplicados en la tabla AX_FLX_BY_AGE_PRE_ALL_TODAY
                SELECT COUNT(1) 
                INTO COMPROBACION_INICIAL
                FROM PROCESS.AX_FLX_BY_AGE_MONTH_PRE_ALL_TODAY 
                WHERE HYBRID_FILTER > 1 AND DUPLICATE_FILTER = 1;

            -- Verificar el valor de la comprobación inicial
                IF (COMPROBACION_INICIAL > 0) THEN
                -- Registrar el problema en la tabla de logs
                INSERT INTO PROCESS.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
                SELECT ''AX_FLX_BY_AGE_MONTH_PRE_ALL_TODAY'' AS DUPLICATE_DETAIL, COUNT(1), CURRENT_TIMESTAMP()
                FROM PROCESS.AX_FLX_BY_AGE_MONTH_PRE_ALL_TODAY 
                WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1;

                -- Finalizar el proceso con un mensaje indicando el error
                RETURN ''Proceso cancelado: Se detectaron duplicados en AX_FLX_BY_AGE_MONTH_PRE_ALL_TODAY. Total: '' || COMPROBACION_INICIAL;
            ELSE

                INSERT INTO PROCESS.AX_FLX_BY_AGE_MONTH_ALL_TODAY
                (
                SELECT 
                YEAR_KEY,
                MONTH_KEY,
                ZONE_ID,
                VISITOR_ORIGIN_ID,
                VISITOR_CATEGORY_ID,
                AGE_ID,
                KPI_ID,
                KPI_VALUE_NUM,
                current_timestamp() LOAD_TIME
                FROM PROCESS.AX_FLX_BY_AGE_MONTH_PRE_ALL_TODAY     
                WHERE DUPLICATE_FILTER = 1
                );

                INSERT INTO PROCESS.AX_FLX_BY_AGE_MONTH
                (
                select 
                a.YEAR_KEY,
                a.MONTH_KEY,
                nvl(z.ZONE_KEY,-3) ZONE_KEY,
                nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                nvl(AGE_KEY,-3) AGE_KEY,
                nvl(KPI_KEY,-3) KPI_KEY,
                a.KPI_VALUE_NUM,
                current_timestamp() LOAD_TIME
                from PROCESS.AX_FLX_BY_AGE_MONTH_ALL_TODAY a
                ,SIT.DM_FLX_ZONE z
                ,SIT.DM_GLB_VISITOR_CATEGORY C
                ,SIT.DM_FLX_VISITOR_ORIGIN O
                ,SIT.DM_ATR_AGE AGE
                ,SIT.DM_IVE_KPI_MASTER KPI

                where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
                and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
                and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
                and a.age_id  =AGE.age_id(+)
                and a.kpi_id= kpi.kpi_id(+)

                union all

                select 
                a.YEAR_KEY,
                a.MONTH_KEY, 
                a.ZONE_KEY,
                a.VISITOR_ORIGIN_KEY,
                a.VISITOR_CATEGORY_KEY,
                a.AGE_KEY,a.KPI_KEY,
                a.KPI_VALUE_NUM,
                a.LOAD_TIME
                from (select a.*,z.zone_id,kpi.kpi_id 
                    from SIT.FC_FLX_BY_AGE_MONTH a,
                        SIT.DM_FLX_ZONE z,
                        SIT.DM_IVE_KPI_MASTER KPI
                    where a.zone_key=z.zone_key
                    and a.kpi_key=kpi.kpi_key) a,
                    
                (select MAX(LOAD_DATE) YEAR_KEY,MONTH_KEY,zone_ID,kpi_id 
                from  PROCESS.AX_FLX_BY_AGE_MONTH_ALL_TODAY) b

                where a.MONTH_KEY=b.MONTH_KEY(+) 
                and a.YEAR_KEY=b.YEAR_KEY(+)     
                and trim(a.zone_id)=trim(b.zone_id(+))
                and a.KPI_id=b.kpi_id(+)
                and b.MONTH_KEY is null
                );

                END IF;
            END;

            INSERT INTO PROCESS.AX_FLX_BY_AGE_PRE_ALL_TODAY
            (
            WITH CONSOLIDADO AS (
                select 
                    to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    TRIM(Edad) AS EDAD,
                    ''Diario'' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
                from STAGING.ST_FLX_DIARIO_EDAD a
                ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_EDAD) M
                where a.load_date=m.load_date  
                AND (trim(CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
                AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''   
                
                union all
                
                select 
                    to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    TRIM(Edad) AS EDAD,
                    ''Llegada'' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
                from STAGING.ST_FLX_LLEGADA_EDAD a
                ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_LLEGADA_EDAD) M
                where a.load_date=m.load_date  
                AND (trim(CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
                AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''   
                
                union all
                
                select 
                    to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    TRIM(Edad) AS EDAD,
                    ''Salida'' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
                from STAGING.ST_FLX_SALIDA_EDAD a
                ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_SALIDA_EDAD) M
                where a.load_date=m.load_date  
                AND (trim(CONTINUO) = ''nc'' AND FILE_NAME NOT LIKE ''%INVA-1080-6642%'')
                AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''   
                
                union all
                
                select 
                    to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    TRIM(Edad) AS EDAD,
                    ''Nocturno'' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
                from STAGING.ST_FLX_NOCTURNO_EDAD a
                ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCTURNO_EDAD) M
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
                    TRIM(Edad) AS EDAD,
                    ''Diario'' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
                from STAGING.ST_FLX_DIARIO_EDAD a
                ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_DIARIO_EDAD) M
                where a.load_date=m.load_date  
                AND (trim(CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
                AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''
                
                union all
                
                select 
                    to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    TRIM(Edad) AS EDAD,
                    ''Llegada'' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
                from STAGING.ST_FLX_LLEGADA_EDAD a
                ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_LLEGADA_EDAD) M
                where a.load_date=m.load_date  
                AND (trim(CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
                AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''
                
                union all
                
                select 
                    to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    TRIM(Edad) AS EDAD,
                    ''Salida'' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
                from STAGING.ST_FLX_SALIDA_EDAD a
                ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_SALIDA_EDAD) M
                where a.load_date=m.load_date  
                AND (trim(CONTINUO) = ''c'' OR FILE_NAME LIKE ''%INVA-1080-6642%'')
                AND FILE_NAME NOT LIKE ''%INVA-1080-9999%''
                
                union all
                
                select 
                    to_number(to_char(fecha,''YYYYMMDD'')) period_key,
                    trim(zonaobservacion) AS zonaobservacion,
                    trim(origen) AS origen,
                    trim(categoriadelvisitante) AS categoriadelvisitante,
                    TRIM(Edad) AS EDAD,
                    ''Nocturno'' KPI_ID,
                    Volumen,
                    trim(continuo) continuo,
                    trim(A.LOAD_DATE) LOAD_DATE
                from STAGING.ST_FLX_NOCTURNO_EDAD a
                ,(select MAX(LOAD_DATE) LOAD_DATE from STAGING.ST_FLX_NOCTURNO_EDAD) M
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
                    CASE WHEN trim(EDAD)=''65 o más'' THEN ''65+'' 
                        WHEN trim(EDAD)=''<18'' THEN ''14-17'' 
                    ELSE trim(EDAD) END AGE_ID,
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
                    CASE WHEN trim(C.EDAD)=''65 o más'' THEN ''65+'' 
                        WHEN trim(C.EDAD)=''<18'' THEN ''14-17'' 
                    ELSE trim(C.EDAD) END AGE_ID,
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
                AGE_ID,
                KPI_ID,
                KPI_VALUE_NUM,
                CONTINUO,
                LOAD_DATE AS FECHA_CARGA,     
                row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,AGE_ID ,KPI_ID ,KPI_VALUE_NUM order by LOAD_DATE DESC) AS DUPLICATE_FILTER , 
                row_number() over (partition by period_key,ZONE_ID ,VISITOR_ORIGIN_ID ,VISITOR_CATEGORY_ID ,AGE_ID ,KPI_ID order by LOAD_DATE DESC) AS HYBRID_FILTER ,  
                current_timestamp() LOAD_TIME
            FROM TOTAL    
            );

            DECLARE

            COMPROBACION_INICIAL INT DEFAULT (SELECT COUNT(1) FROM PROCESS.AX_FLX_BY_AGE_PRE_ALL_TODAY WHERE HYBRID_FILTER > 1 and DUPLICATE_FILTER = 1);
            COMPROBACION_FINAL INT;

            BEGIN
                IF (COMPROBACION_INICIAL > 0 ) THEN	
                    RETURN ''Proceso cancelado: Hay duplicados en AX_FLX_BY_AGE_PRE_ALL_TODAY: '' || COMPROBACION_INICIAL;
                ELSE

                    INSERT INTO PROCESS.AX_FLX_BY_AGE_ALL_TODAY
                    (
                    SELECT 
                        period_key,
                        ZONE_ID,
                        VISITOR_ORIGIN_ID,
                        VISITOR_CATEGORY_ID,
                        AGE_ID,
                        KPI_ID,
                        KPI_VALUE_NUM,
                        current_timestamp() LOAD_TIME
                    FROM PROCESS.AX_FLX_BY_AGE_PRE_ALL_TODAY     
                    WHERE DUPLICATE_FILTER = 1
                    );

                    

                    INSERT INTO PROCESS.AX_FLX_BY_AGE
                    (
                    select 
                        a.period_key,
                        nvl(z.ZONE_KEY,-3) ZONE_KEY,
                        nvl(o.VISITOR_ORIGIN_key,-3) VISITOR_ORIGIN_key,
                        nvl(c.VISITOR_CATEGORY_KEY,-3) VISITOR_CATEGORY_KEY,
                        nvl(AGE_KEY,-3) AGE_KEY,
                        nvl(KPI_KEY,-3) KPI_KEY,
                        a.KPI_VALUE_NUM,
                        current_timestamp() LOAD_TIME
                    from PROCESS.AX_FLX_BY_AGE_ALL_TODAY a
                        ,SIT.DM_FLX_ZONE z
                        ,SIT.DM_GLB_VISITOR_CATEGORY C
                        ,SIT.DM_FLX_VISITOR_ORIGIN O
                        ,SIT.DM_ATR_AGE AGE
                        ,SIT.DM_IVE_KPI_MASTER KPI
                        
                    where a.VISITOR_ORIGIN_ID=o.VISITOR_ORIGIN_ID(+)
                    and trim(a.ZONE_ID)=trim(z.ZONE_ID(+))
                    and a.VISITOR_CATEGORY_ID=c.VISITOR_CATEGORY_ID(+)
                    and a.age_id  =AGE.age_id(+)
                    and a.kpi_id= kpi.kpi_id(+)
                    
                    union all
                    
                    select 
                        a.PERIOD_KEY,
                        a.ZONE_KEY,
                        a.VISITOR_ORIGIN_KEY,
                        a.VISITOR_CATEGORY_KEY,
                        a.AGE_KEY,
                        a.KPI_KEY,
                        a.KPI_VALUE_NUM,
                        a.LOAD_TIME
                    from (select 
                        a.*,
                        z.zone_id,
                        kpi.kpi_id 
                    from SIT.FC_FLX_BY_AGE a,
                    SIT.DM_FLX_ZONE z,
                    SIT.DM_IVE_KPI_MASTER KPI 
                    where a.zone_key=z.zone_key
                    and a.kpi_key=kpi.kpi_key) a,
                    (select MAX(LOAD_DATE) 
                        PERIOD_KEY,
                        zone_ID,
                        kpi_id 
                    from  PROCESS.AX_FLX_BY_AGE_ALL_TODAY) b
                    where a.period_key=b.period_key(+) 
                    and trim(a.zone_id)=trim(b.zone_id(+))
                    and a.KPI_id=b.kpi_id(+)
                    and b.period_key is null
                    );

                END IF;
            END;


        END;
        RETURN ''La task LOAD_AX_FLX_AGE se ha creado'';
    END;
';




