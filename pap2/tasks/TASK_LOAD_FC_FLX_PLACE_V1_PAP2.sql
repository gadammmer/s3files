EXECUTE IMMEDIATE '
    BEGIN
        create or replace task SIT.LOAD_FC_FLX_PLACE_V1
          USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
          after SIT.LOAD_FC_FLX_NOS
          as BEGIN
                   
                
                INSERT INTO SIT.FC_FLX_PLACE_BY_CITY_WEEK
                (
                select
                    A.WEEK_YEAR_KEY,
                    a.zone_key,
                    a.visitor_origin_key,
                    a.visitor_category_key,
                    a.CITY_key,
                    a.ZIPCODE_ID,
                    a.PLACE_CITY_KEY,
                    a.kpi_key,
                    a.KPI_VALUE_NUM,
                    LOAD_TIME
                from PROCESS.AX_FLX_PLACE_BY_CITY_WEEK a
                where CITY_key>0
                );
                    INSERT INTO SIT.FC_FLX_PLACE_BY_CITY
                    (
                    select
                        a.period_key,
                        a.zone_key,
                        a.visitor_origin_key,
                        a.visitor_category_key,
                        a.CITY_key,
                        nvl(A.ZIPCODE_ID,''00000'') ZIPCODE_ID,
                        a.PLACE_CITY_KEY,
                        a.kpi_key,
                        a.KPI_VALUE_NUM,
                        LOAD_TIME
                    from PROCESS.AX_FLX_PLACE_BY_CITY a
                    where CITY_key>0
                    );
                
                    INSERT INTO SIT.FC_FLX_PLACE_BY_COUNTRY_WEEK
                    (
                    select
                        WEEK_YEAR_KEY,
                        a.zone_key,
                        a.visitor_origin_key,
                        a.visitor_category_key,
                        a.COUNTRY_key,
                        PLACE_CITY_KEY,
                        a.kpi_key,
                        a.KPI_VALUE_NUM,
                        LOAD_TIME
                    from
                    PROCESS.AX_FLX_PLACE_BY_COUNTRY_WEEK a
                    where country_key>0
                    );
                        INSERT INTO SIT.FC_FLX_PLACE_BY_COUNTRY
                        (
                        select
                            a.period_key,
                            a.zone_key,
                            a.visitor_origin_key,
                            a.visitor_category_key,
                            a.COUNTRY_key,
                            PLACE_CITY_KEY,
                            a.kpi_key,
                            a.KPI_VALUE_NUM,
                            LOAD_TIME
                        from
                        PROCESS.AX_FLX_PLACE_BY_COUNTRY a
                        where country_key>0
                        );
            END;
             RETURN ''Task LOAD_FC_FLX_PLACE_V1 creada correctamente'';
    END; ';
