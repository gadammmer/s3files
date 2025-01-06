 EXECUTE IMMEDIATE '
    BEGIN
        create or replace task SIT.LOAD_FC_FLX_AGE
          USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
          after SIT.LOAD_FC_FLX_2H
          as BEGIN
              DECLARE
              COMPROBACION_FINAL INT;
              BEGIN
              -- Inicializar la variable COMPROBACION_INICIAL
              SELECT COUNT(1) 
              INTO COMPROBACION_FINAL
              FROM (
              select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, age_key , KPI_KEY                                    
              from PROCESS.AX_FLX_BY_AGE 
              group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, age_key , KPI_KEY
              having count(1) > 1);

              IF (COMPROBACION_FINAL > 0) THEN

                  INSERT INTO SIT.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
                  SELECT ''Hay duplicados en AX_FLX_BY_AGE_MONTH'' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
                  select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, age_key , KPI_KEY
                  from PROCESS.AX_FLX_BY_AGE_MONTH
                  group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, age_key , KPI_KEY
                  having count(1) > 1);
                  
                  -- Mensaje del return
                  RETURN ''Proceso cancelado: Hay duplicados en AX_FLX_BY_AGE_MONTH: '' || COMPROBACION_FINAL;	
              ELSE
                  
                  INSERT INTO SIT.FC_FLX_BY_AGE_MONTH 
                  (
                  select 
                      a.YEAR_KEY,
                      a.MONTH_KEY, 
                      a.zone_key,
                      a.visitor_origin_key,
                      a.visitor_category_key,
                      a.age_key,
                      a.kpi_key,
                      a.KPI_VALUE_NUM,
                      LOAD_TIME
                  from PROCESS.AX_FLX_BY_AGE_MONTH a
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
                  select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, age_key , KPI_KEY                                    
                  from PROCESS.AX_FLX_BY_AGE 
                  group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, age_key , KPI_KEY
                  having count(1) > 1);

                  IF (COMPROBACION_FINAL > 0) THEN

                      INSERT INTO SIT.WK_FLX_ALARM_DUPLICATE_LOG (DUPLICATE_DETAIL, DUPLICATE_COUNT, LOAD_TIME)
                      SELECT ''Hay duplicados en AX_FLX_BY_AGE'' AS DUPLICATE_DETAIL, COUNT(1),  CURRENT_TIMESTAMP() FROM (
                      select count(1), PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, age_key , KPI_KEY
                      from PROCESS.AX_FLX_BY_AGE
                      group by PERIOD_KEY, ZONE_KEY, VISITOR_ORIGIN_KEY, VISITOR_CATEGORY_KEY, age_key , KPI_KEY
                      having count(1) > 1);

                  -- Mensaje del return
                      RETURN ''Proceso cancelado: Hay duplicados en AX_FLX_BY_AGE: '' || COMPROBACION_FINAL;	
                  ELSE
                      INSERT INTO SIT.FC_FLX_BY_AGE 
                      (
                      select 
                          a.period_key,
                          a.zone_key,
                          a.visitor_origin_key,
                          a.visitor_category_key,
                          a.age_key, 
                          a.kpi_key,
                          a.KPI_VALUE_NUM,
                          LOAD_TIME
                      from PROCESS.AX_FLX_BY_AGE a
                      );
                  END IF;
                  END;
          END;

          RETURN ''Task LOAD_AX_FLX_AGE creada correctamente'';
    END;
';
