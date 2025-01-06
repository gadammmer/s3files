EXECUTE IMMEDIATE '
  BEGIN
        create or replace task SIT.LOAD_FC_FLX_REC
          USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
          after SIT.LOAD_FC_FLX_PLACE_V2
          as BEGIN
                  DECLARE
                  COMPROBACION_FINAL INT;
                  BEGIN
                  -- Inicializar la variable COMPROBACION_INICIAL
                  SELECT COUNT(1) 
                  INTO COMPROBACION_FINAL
                  FROM (
                  select count(1), PLACE_CITY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY                                    
                  from PROCESS.AX_FLX_PLACE_WEEK 
                  group by PLACE_CITY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
                  having count(1) > 1);

                  IF (COMPROBACION_FINAL > 0) THEN

                  INSERT INTO SIT.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
                  SELECT ''Hay duplicados en AX_FLX_PLACE_WEEK'' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
                  select count(1), PLACE_CITY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
                  from PROCESS.AX_FLX_PLACE_WEEK
                  group by PLACE_CITY_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
                  having count(1) > 1);

                  -- Mensaje del return
                  RETURN ''Proceso cancelado: Hay duplicados en AX_FLX_PLACE_WEEK: '' || COMPROBACION_FINAL;	
                  ELSE
                      INSERT INTO SIT.FC_FLX_PLACE_WEEK 
                      (
                      select 
                          A.WEEK_YEAR_KEY,
                          a.zone_key,
                          a.visitor_origin_key,
                          a.visitor_category_key,
                          a.PLACE_CITY_KEY, 
                          a.kpi_key,
                          a.KPI_VALUE_NUM,
                          LOAD_TIME
                      from PROCESS.AX_FLX_PLACE_WEEK a
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
                  select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY                                    
                  from PROCESS.AX_FLX_PLACE 
                  group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
                  having count(1) > 1);

                  IF (COMPROBACION_FINAL > 0) THEN

                  INSERT INTO SIT.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
                  SELECT ''Hay duplicados en AX_FLX_PLACE'' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
                  select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
                  from PROCESS.AX_FLX_PLACE
                  group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY , KPI_KEY
                  having count(1) > 1);

                  -- Mensaje del return
                  RETURN ''Proceso cancelado: Hay duplicados en AX_FLX_PLACE: '' || COMPROBACION_FINAL;	
                  ELSE
                    
                        INSERT INTO SIT.FC_FLX_PLACE
                        (
                        select 
                            a.period_key,
                            a.zone_key,
                            a.visitor_origin_key,
                            a.visitor_category_key,
                            a.PLACE_CITY_KEY, 
                            a.kpi_key,
                            a.KPI_VALUE_NUM,
                            LOAD_TIME
                        from PROCESS.AX_FLX_PLACE a
                        );

                        RETURN ''Proceso finalizado correctamente.'';
                END IF;    
                END;
          END;
           RETURN ''Task LOAD_FC_FLX_REC creada correctamente'';
      END;   
';      

ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_REC RESUME;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_PLACE_V2 RESUME;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_PLACE_V1 RESUME;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_NOS RESUME;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_LOS RESUME;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_BY_GENDER RESUME;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_BY_CITY RESUME;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_AGE RESUME;
ALTER TASK IF EXISTS PROCESS.LOAD_FC_FLX_2H RESUME;

