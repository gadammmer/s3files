
EXECUTE IMMEDIATE '
    BEGIN
    CREATE OR REPLACE VIEW SIT.VW_INE_FLX_TOURISTS_REGION AS (

            WITH 
            --KPIs FLUX, viajeros/salidas y pernoctaciones/nocturnos
            AX_FLUX AS (
            
                SELECT
                    LEFT(PERIOD_KEY, 6) AS YEAR_MONTH,
                    10 as ACCOMMODATION_TYPE_KEY, --Estimación otros
                    DECODE(VISITOR_ORIGIN_KEY, 2, 1, 3, 2, VISITOR_ORIGIN_KEY) as RESIDENCE_KEY,
                    DECODE(ZONE_KEY, 19, 10, ZONE_KEY) as REGION_KEY,
                    DECODE (KPI_KEY, 31, 12, 32, 13, KPI_KEY) as KPI_KEY,
                    SUM(KPI_VALUE_NUM) AS KPI_VALUE_NUM
            
                FROM SIT.FC_FLX_GENERAL
            
                WHERE KPI_KEY in (31, 32) -- Salida: Número de personas que salen por día
                AND ZONE_KEY in (19)  --Valencia Comunidad Autonoma
                AND VISITOR_CATEGORY_KEY IN (6)
                
                GROUP BY
                    LEFT(PERIOD_KEY, 6),
                    DECODE(VISITOR_ORIGIN_KEY, 2, 1, 3, 2, VISITOR_ORIGIN_KEY),
                    DECODE(ZONE_KEY, 19, 10, ZONE_KEY),
                    DECODE (KPI_KEY, 31, 12, 32, 13, KPI_KEY)
            ),
            
            --KPIs INE, Viajeros y Pernoctaciones
            AX_INE AS (
            
                SELECT
                    LEFT(PERIOD_KEY, 6) AS YEAR_MONTH,
                    RESIDENCE_KEY,
                    REGION_KEY,
                    KPI_KEY,
                    SUM(KPI_VALUE_NUM) AS KPI_VALUE_NUM
                    
                FROM SIT.FC_INE_OCCUPSURVEY
            
                WHERE REGION_KEY in (10) --Valencia Comunidad Autonoma
                AND KPI_KEY in (12, 13) --Viajeros, Pernoctaciones
                                    
                GROUP BY
                    LEFT(PERIOD_KEY, 6),
                    RESIDENCE_KEY,
                    REGION_KEY,
                    KPI_KEY
            ),
            
            --ESTIMACION OTROS de Viajeros y Pernoctaciones, RESTA DE FLUX - INE
            AX_OTROS AS(
            
                SELECT
                    FLX.YEAR_MONTH,
                    FLX.ACCOMMODATION_TYPE_KEY,
                    FLX.RESIDENCE_KEY,
                    FLX.REGION_KEY,
                    FLX.KPI_KEY,
                    (FLX.KPI_VALUE_NUM - INE.KPI_VALUE_NUM) as KPI_VALUE_NUM
            
                FROM AX_FLUX FLX
            
                INNER JOIN AX_INE INE 
                    ON  FLX.YEAR_MONTH = INE.YEAR_MONTH 
                    AND FLX.RESIDENCE_KEY = INE.RESIDENCE_KEY
                    AND FLX.REGION_KEY = INE.REGION_KEY
                    AND FLX.KPI_KEY = INE.KPI_KEY
            
            ),
            
            -- ESTANCIA MEDIA FLUX
            AX_EM AS(
                SELECT
                    OTRO1.YEAR_MONTH,
                    OTRO1.ACCOMMODATION_TYPE_KEY,
                    ''3'' as RESIDENCE_KEY,
                    OTRO1.REGION_KEY,
                    ''14'' as KPI_KEY,
                    ROUND((SUM(OTRO1.KPI_VALUE_NUM)/SUM(OTRO2.KPI_VALUE_NUM)), 2) as KPI_VALUE_NUM
            
                FROM AX_OTROS OTRO1
                LEFT JOIN AX_OTROS OTRO2 
                ON  OTRO1.YEAR_MONTH = OTRO2.YEAR_MONTH 
                AND OTRO1.REGION_KEY = OTRO2.REGION_KEY
                WHERE OTRO1.KPI_KEY = 13 --Pernoctaciones
                AND OTRO2.KPI_KEY = 12 --Viajeros
            
                GROUP BY
                    OTRO1.YEAR_MONTH,
                    OTRO1.ACCOMMODATION_TYPE_KEY,
                    OTRO1.REGION_KEY
            ),
            
            -- TOTAL INE
            INE_TOTAL AS (
                SELECT 
                    LEFT(PERIOD_KEY, 6) AS YEAR_MONTH,
                    ACCOMMODATION_TYPE_KEY,
                    RESIDENCE_KEY,
                    REGION_KEY,
                    KPI_KEY AS KPI_KEY,
                    SUM(KPI_VALUE_NUM) as KPI_VALUE_NUM
            
                FROM SIT.FC_INE_OCCUPSURVEY
                
                WHERE REGION_KEY in (10) --Comunidad Valenciana
                AND KPI_KEY in (12, 13, 14) --Viajeros, Pernoctaciones, EM 
            
                GROUP BY
                    LEFT(PERIOD_KEY, 6),
                    ACCOMMODATION_TYPE_KEY,
                    RESIDENCE_KEY,
                    REGION_KEY,
                    KPI_KEY
            )
            
                SELECT * FROM INE_TOTAL
                
                UNION ALL
                
                SELECT * FROM AX_OTROS
                
                UNION ALL
                
                SELECT * FROM AX_EM
    );

END;';