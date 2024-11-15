EXECUTE IMMEDIATE '
  BEGIN
        create or replace task SIT.LOAD_FC_FLX_NOS
          USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
          after SIT.LOAD_FC_FLX_LOS
          as BEGIN

                  DECLARE
                  COMPROBACION_FINAL INT;
                  BEGIN
                  -- Inicializar la variable COMPROBACION_INICIAL
                  SELECT COUNT(1) 
                  INTO COMPROBACION_FINAL
                  FROM (
                  select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY                                    
                  from PROCESS.AX_FLX_NOS_BY_CITY 
                  group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
                  having count(1) > 1);

                  IF (COMPROBACION_FINAL > 0) THEN

                  INSERT INTO SIT.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
                  SELECT ''Hay duplicados en AX_FLX_NOS_BY_CITY'' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
                  select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
                  from PROCESS.AX_FLX_NOS_BY_CITY
                  group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY 
                  having count(1) > 1);

                  -- Mensaje del return
                  RETURN ''Proceso cancelado: Hay duplicados en AX_FLX_NOS_BY_CITY: '' || COMPROBACION_FINAL;	
                  ELSE
                      
                  INSERT INTO SIT.FC_FLX_NOS_BY_CITY 
                  (
                  select 
                      a.period_key,
                      a.zone_key,
                      a.visitor_origin_key,
                      a.visitor_category_key,
                      a.CITY_key,
                      a.NIGHTS_STAY_KEY,
                      nvl(A.ZIPCODE_ID,''00000'') ZIPCODE_ID,
                      a.KPI_VALUE_NUM,
                      LOAD_TIME
                  from PROCESS.AX_FLX_NOS_BY_CITY a
                  where city_key>0
                  );

                RETURN ''Proceso finalizado correctamente.'';

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
                from PROCESS.AX_FLX_NOS_BY_COUNTRY 
                group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
                having count(1) > 1);

                IF (COMPROBACION_FINAL > 0) THEN

                INSERT INTO SIT.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
                SELECT ''Hay duplicados en AX_FLX_NOS_BY_COUNTRY'' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
                select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
                from PROCESS.AX_FLX_NOS_BY_COUNTRY
                group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY 
                having count(1) > 1);

                -- Mensaje del return
                RETURN ''Proceso cancelado: Hay duplicados en AX_FLX_NOS_BY_COUNTRY: '' || COMPROBACION_FINAL;	
                ELSE
                    INSERT INTO SIT.FC_FLX_NOS_BY_COUNTRY 
                    (
                    select 
                        a.period_key,
                        a.zone_key,
                        a.visitor_origin_key,
                        a.visitor_category_key,
                        a.COUNTRY_key,
                        NIGHTS_STAY_KEY,
                        a.KPI_VALUE_NUM,
                        LOAD_TIME
                    from PROCESS.AX_FLX_NOS_BY_COUNTRY a
                    where country_key>0
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
                select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY                                    
                from PROCESS.AX_FLX_NOS 
                group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY 
                having count(1) > 1);

                IF (COMPROBACION_FINAL > 0) THEN

                INSERT INTO SIT.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
                SELECT ''Hay duplicados en AX_FLX_NOS'' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
                select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
                from PROCESS.AX_FLX_NOS
                group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY
                having count(1) > 1);

                -- Mensaje del return
                RETURN ''Proceso cancelado: Hay duplicados en AX_FLX_NOS: '' || COMPROBACION_FINAL;	
                ELSE
                    INSERT INTO SIT.FC_FLX_NOS 
                    (
                    select 
                        a.period_key,
                        a.zone_key,
                        a.visitor_origin_key,
                        a.visitor_category_key,
                        a.NIGHTS_STAY_KEY,
                        a.KPI_VALUE_NUM,
                        LOAD_TIME
                    from PROCESS.AX_FLX_NOS a
                    );
                END IF;
                END;
          END;
          RETURN ''Task LOAD_AX_FLX_NOS creada correctamente'';
    END;
';
            



