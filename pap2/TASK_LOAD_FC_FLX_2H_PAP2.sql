ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_2H SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_AGE SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_BY_CITY SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_BY_GENDER SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_LOS SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_NOS SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_PLACE_V1 SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_PLACE_V2 SUSPEND;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_REC SUSPEND;


DROP TASK IF EXISTS PROCESS.LOAD_FC_FLX_2H;
DROP TASK IF EXISTS PROCESS.LOAD_FC_FLX_AGE;
DROP TASK IF EXISTS PROCESS.LOAD_FC_FLX_BY_CITY;
DROP TASK IF EXISTS PROCESS.LOAD_FC_FLX_BY_GENDER;
DROP TASK IF EXISTS PROCESS.LOAD_FC_FLX_LOS;
DROP TASK IF EXISTS PROCESS.LOAD_FC_FLX_NOS;
DROP TASK IF EXISTS PROCESS.LOAD_FC_FLX_PLACE_V1;
DROP TASK IF EXISTS PROCESS.LOAD_FC_FLX_PLACE_V2;
DROP TASK IF EXISTS PROCESS.LOAD_FC_FLX_REC;

EXECUTE IMMEDIATE '
    BEGIN

        create or replace task SIT.LOAD_FC_FLX_2H
            schedule=''USING CRON  30 12 * * * Europe/Madrid''
            USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
            as BEGIN
                DECLARE
                    COMPROBACION_FINAL INT;
                    BEGIN
                    -- Inicializar la variable COMPROBACION_INICIAL
                    SELECT COUNT(1) 
                    INTO COMPROBACION_FINAL
                    FROM (
                    select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, period_key                                     
                    from PROCESS.AX_FLX_2H 
                    group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, period_key 
                    having count(1) > 1);

                    IF (COMPROBACION_FINAL > 0) THEN

                    INSERT INTO SIT.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
                    SELECT ''Hay duplicados en AX_FLX_2H'' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
                    select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, period_key 
                    from PROCESS.AX_FLX_2H
                    group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, period_key 
                    having count(1) > 1);

                    -- Mensaje del return
                    RETURN ''Proceso cancelado: Hay duplicados en AX_FLX_2H: '' || COMPROBACION_FINAL;	
                    ELSE

                        INSERT INTO SIT.FC_FLX_2H 
                        (
                        select 
                            a.period_key,
                            a.zone_key,
                            a.visitor_origin_key,
                            a.visitor_category_key,
                            a.HOUR_KEY,
                            a.KPI_VALUE_NUM,
                            LOAD_TIME
                        from PROCESS.AX_FLX_2H a
                        );

                    END IF;
                    END;

              
        DECLARE
        COMPROBACION_FINAL INT;
        BEGIN
        -- Inicializar la variable COMPROBACION_INICIAL
        SELECT COUNT(1) 
        INTO COMPROBACION_FINAL
        FROM (
        select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, period_key                                     
        from PROCESS.AX_FLX_BY_CITY_2H 
        group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, period_key 
        having count(1) > 1);

        IF (COMPROBACION_FINAL > 0) THEN

        INSERT INTO SIT.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
        SELECT ''Hay duplicados en AX_FLX_BY_CITY_2H'' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
        select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, CITY_KEY, HOUR_KEY, ZIPCODE_ID 
        from PROCESS.AX_FLX_BY_CITY_2H
        group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, CITY_KEY, HOUR_KEY,ZIPCODE_ID
        having count(1) > 1);

        -- Mensaje del return
        RETURN ''Proceso cancelado: Hay duplicados en AX_FLX_BY_CITY_2H: '' || COMPROBACION_FINAL;	
        ELSE
            INSERT INTO SIT.FC_FLX_BY_CITY_2H 
            (
            select 
                a.period_key,
                a.zone_key,
                a.visitor_origin_key,
                a.visitor_category_key,
                a.CITY_key, 
                a.HOUR_KEY,
                nvl(A.ZIPCODE_ID,''00000'') ZIPCODE_ID,
                a.KPI_VALUE_NUM,
                LOAD_TIME
            from PROCESS.AX_FLX_BY_CITY_2H a
            where city_key>0
            );  

            END IF;
            END;

        
                    DECLARE
                    COMPROBACION_FINAL INT;
                    BEGIN
                    -- Inicializar la variable COMPROBACION_INICIAL
                    SELECT COUNT(1) 
                    INTO COMPROBACION_FINAL
                    FROM (
                    select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, COUNTRY_KEY, HOUR_KEY                                     
                    from PROCESS.AX_FLX_BY_COUNTRY_2H 
                    group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, COUNTRY_KEY, HOUR_KEY 
                    having count(1) > 1);

                    IF (COMPROBACION_FINAL > 0) THEN

                    INSERT INTO SIT.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
                    SELECT ''Hay duplicados en AX_FLX_BY_COUNTRY_2H'' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
                    select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, period_key 
                    from PROCESS.AX_FLX_BY_COUNTRY_2H
                    group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, period_key 
                    having count(1) > 1);

                    -- Mensaje del return
                    RETURN ''Proceso cancelado: Hay duplicados en AX_FLX_BY_COUNTRY_2H: '' || COMPROBACION_FINAL;	
                    ELSE

                               INSERT INTO SIT.FC_FLX_BY_COUNTRY_2H
                                (
                                select 
                                    a.period_key,
                                    a.zone_key,
                                    a.visitor_origin_key,
                                    a.visitor_category_key,
                                    a.COUNTRY_key, 
                                    a.HOUR_key,
                                    a.KPI_VALUE_NUM,
                                    LOAD_TIME
                                from PROCESS.AX_FLX_BY_COUNTRY_2H a
                                where country_key>0
                                );

                    END IF;
                    END;
    END;

    RETURN ''Task LOAD_AX_FLX_BY_COUNTRY_2H creada correctamente'';
END;

';
