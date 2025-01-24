EXECUTE IMMEDIATE '
BEGIN

create or replace task PROCESS.LOAD_AX_INE_ALL_TODAY
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE=''XLARGE''
	after PROCESS.LOAD_AX_INE_OFERTA_ALOJAMIENTOS
	as BEGIN

              create or replace TABLE PROCESS.AX_INE_OCCUPSURVEY_ALL_TODAY (
                  FILTER_ID VARCHAR(20),
                  PERIOD_KEY NUMBER(9,0),
                  CODE_DES VARCHAR(200),
                  SCOPE_DES VARCHAR(100),
                  KPI_ID VARCHAR(200),
                  REGION_DES VARCHAR(200),
                  PROVINCE_DES VARCHAR(200),
                  CITY_DES VARCHAR(200),
                  RESIDENCE_ID VARCHAR(200),
                  TYPE_ID VARCHAR(200),
                  TOURISTIC_ZONE_DES VARCHAR(200),     
                  KPI_VALUE_NUM FLOAT,
                  TABLE_ID VARCHAR(10),
                  ACCOMMODATION_TYPE_ID VARCHAR(50),
                  LOAD_TIME TIMESTAMP_LTZ(0)
              );

              insert into PROCESS.AX_INE_OCCUPSURVEY_ALL_TODAY 
       (
              SELECT 
                  FILTER_ID,
                  to_number(to_char(a.FECHA,''YYYYMMDD'')) period_key,
                  COD code_des,
                  nvl(TRIM(TOTALES_TERRITORIALES),''**********'') Scope_Des,
                  CASE WHEN CONCEPTO_TURISTICO = ''Viajero'' AND TIPO_DE_DATO = ''Porcentaje'' THEN ''Distribución porcentual de viajeros''
                       WHEN CONCEPTO_TURISTICO = ''Pernoctaciones'' AND TIPO_DE_DATO = ''Porcentaje'' THEN ''Distribución porcentual de pernoctaciones''
                  ELSE REPLACE(CONCEPTO_TURISTICO,''Viajero'',''Viajeros'') END AS KPI_ID,
                  COMUNIDADES_Y_CIUDADES_AUTONOMAS REGION_DES,
                  TRANSLATE(UPPER(DECODE(PROVINCIAS
                              ,''Asturias (Principado de)'' ,''ASTURIAS''
                              ,''Baleares (Illes)'' ,''ISLAS BALEARES''
                              ,''Murcia (Región de)'',''MURCIA''
                              ,''Madrid (Comunidad de)'',''MADRID''
                              ,''Navarra (Comunidad Foral de)'',''NAVARRA''
                              ,''Rioja (La)'',''LA RIOJA''
                              ,''Vizcaya'',''BIZKAIA''
                              ,''Santa Cruz Tenerife'',''SANTA CRUZ DE TENERIFE''
                              ,''Guipúzcoa'',''GIPUZKOA''
                              ,''Alicante/Alacant'',''ALICANTE''
                              ,''Araba/Álava'',''ALAVA''
                              ,''Balears, Illes'',''ISLAS BALEARES''
                              ,''Coruña, A'',''A CORUÑA''
                              ,''Valencia/València'',''VALENCIA''
                              ,''Palmas, Las'',''LAS PALMAS''
                              ,''Castellón/Castelló'',''CASTELLON''
                              ,''Rioja, La'',''LA RIOJA''
                             ,PROVINCIAS)),''ÁÉÍÓÚÜ()-/'',''AEIOUU'')  PROVINCE_DES
                    ,TRANSLATE(UPPER(DECODE(PUNTOS_TURISTICOS
                              ,''Peñiscola'' ,''PENISCOLAPEÑISCOLA''
                              ,''Naut Arant'',''NAUT ARAN''
                              ,''Alicante'',''ALACANTALICANTE''
                              ,''Palma De Mallorca'',''PALMA''
                              ,''El Puerto De Santa María'',''PUERTO DE SANTA MARIA EL''
                              ,''Castellón De La Plana'',''CASTELLO''
                              ,''Coruña'',''CORUÑA A''
                              ,''Las Palmas De Gran Canaria'',''PALMAS DE GRAN CANARIA LAS''
                              ,''Pamplona'',''PAMPLONAIRUÑA''
                              ,''Javea/Xabia'',''XABIAJAVEA''
                              ,''Calpe/Calp'',''CALP''
                             ,PUNTOS_TURISTICOS)),''ÀÁÈÉÍÓÚÜ()-/ ,'''''',''AAEEIOUU'') CITY_DES
                     ,TRIM(nvl(RESIDENCIA_ORIGEN,''**********'')) RESIDENCE_ID,
                   TIPO_DE_CATEGORIA Type_Des,
                   ZONAS_TURISTICAS AS TOURISTIC_ZONE_DES,
                   NVL(a.VALOR,0) KPI_VALUE_NUM,
                   TABLA,
                   decode(FAMILIA
                      ,''EOH'',''Hoteles''
                      ,''EOAP'',''Apartamentos''
                      ,''EOC'',''Campings''
                      ,''EOATR'',''Rurales'' 
                      ,''EOAL'',''Albergues''
                      ,FAMILIA) ACCOMMODATION_TYPE_ID,
                   CURRENT_TIMESTAMP(0) LOAD_TIME
              from 
              (
                  select 
                      ''OCCUPSURVEY'' filter_id,   
                       FECHA,
                       VALOR,
                       FAMILIA,
                       TABLA,
                       LOAD_TIME,
                       COD,
                       NOMBRE,
                       COMUNIDADES_Y_CIUDADES_AUTONOMAS,
                       CONCEPTO_TURISTICO,
                       PROVINCIAS,
                       PUNTOS_TURISTICOS,
                       RESIDENCIA_ORIGEN,
                       TIPO_DE_CATEGORIA,
                       TIPO_DE_DATO,
                       TIPO_DE_ESTABLECIMIENTO,
                       TOTALES_TERRITORIALES,
                       TOTAL_NACIONAL,
                       ZONAS_TURISTICAS 
                 from PROCESS.AX_INE_VIAJEROS_Y_PERNOCTACIONES a
                 
                  union all
                  
                 select 
                    ''OFFER'' filter_id,
                       FECHA,
                       VALOR,
                       FAMILIA,
                       TABLA,
                       LOAD_TIME,
                       COD,
                       NOMBRE,
                       COMUNIDADES_Y_CIUDADES_AUTONOMAS,
                       CONCEPTO_TURISTICO,
                       PROVINCIAS,
                       PUNTOS_TURISTICOS,
                       RESIDENCIA_ORIGEN,
                       TIPO_DE_CATEGORIA,
                       TIPO_DE_DATO,
                       TIPO_DE_ESTABLECIMIENTO,
                       TOTALES_TERRITORIALES,
                       TOTAL_NACIONAL,
                       ZONAS_TURISTICAS 
                from PROCESS.AX_INE_OFERTA_ALOJAMIENTOS a
              ) a);

              create or replace TABLE PROCESS.AX_INE_OCCUPSURVEY (
                FILTER_ID VARCHAR(20),
                PERIOD_KEY NUMBER(9),
                ACCOMMODATION_TYPE_KEY NUMBER(16),
                RESIDENCE_KEY NUMBER(16),
                REGION_DES VARCHAR(200),
                PROVINCE_KEY NUMBER(16),
                CITY_KEY NUMBER(16) ,
                TOURISTIC_ZONE_ID NUMBER(16),
                KPI_KEY NUMBER(16),
                KPI_VALUE_NUM FLOAT,
                LOAD_TIME DATETIME
            );


          insert into PROCESS.AX_INE_OCCUPSURVEY  
          (
          select 
              filter_id,
              period_key,
              nvl(ACCOMMODATION_TYPE_KEY,-3) ACCOMMODATION_TYPE_KEY,
              NVL(R.ID,-3) RESIDENCE_KEY ,
              DECODE(TRANSLATE(UPPER(A.REGION_DES), ''ÁÉÍÓÚ'', ''AEIOU'')
                    ,''ASTURIAS, PRINCIPADO DE'',''ASTURIAS''
                    ,''CASTILLA - LA MANCHA'',''CASTILLA LA MANCHA''
                    ,''BALEARS, ILLES'',''ISLAS BALEARES''
                    ,''NAVARRA, COMUNIDAD FORAL DE'',''NAVARRA''
                    ,''COMUNITAT VALENCIANA'',''VALENCIA''
                    ,''MADRID, COMUNIDAD DE'',''MADRID''
                    ,''RIOJA, LA'',''LA RIOJA''
                    ,''MURCIA, REGION DE'',''MURCIA''
                    ,TRANSLATE(UPPER(A.REGION_DES), ''ÁÉÍÓÚ'', ''AEIOU'')
            ) AS REGION_DES,
              nvl(province_key,-3) province_key,
              nvl(CITY_key,-3) CITY_key,
              nvl(ZT.ID,-3) TOURISTIC_ZONE_ID,
              nvl(kpi_key,-3) kpi_key,
              kpi_value_num,
              CURRENT_TIMESTAMP(0) LOAD_TIME
              FROM PROCESS.AX_INE_OCCUPSURVEY_ALL_TODAY A
              LEFT JOIN SIT.DM_IVE_KPI_MASTER b
              ON a.kpi_id=b.kpi_id

              LEFT JOIN SIT.DM_INE_RESIDENCIA_ORIGEN R
              ON A.RESIDENCE_ID=R.NOMBRE

              LEFT JOIN SIT.DM_IVE_ACCOMMODATION_TYPE AT
              ON A.ACCOMMODATION_TYPE_ID=AT.ACCOMMODATION_TYPE_ID

              LEFT JOIN (SELECT DISTINCT TRANSLATE(UPPER(TRIM(NOMBRE)), ''ÑÁÉÍÓÚ/()- '', ''NAEIOU_'') AS NOMBRE, ID FROM SIT.DM_INE_ZONAS_TURISTICAS) ZT
              ON TRANSLATE(UPPER(TRIM(A.TOURISTIC_ZONE_DES)), ''ÑÁÉÍÓÚ/()- '', ''NAEIOU_'') = ZT.NOMBRE

              LEFT JOIN (select c.city_key,translate(c.city_norm_des,'' '''''','''') city_norm_des from SIT.DM_GLB_COUNTRY co,SIT.DM_GLB_CITY c where country_des=''España'' and c.country_key=co.country_key) c        
              ON A.CITY_DES=C.city_norm_des

              LEFT JOIN (select c.province_key,c.province_norm_des,c.region_key from SIT.DM_GLB_COUNTRY co,SIT.DM_GLB_PROVINCE c where country_des=''España'' and c.country_key=co.country_key) d
              ON A.PROVINCE_DES=d.province_norm_des

              WHERE UPPER(a.RESIDENCE_ID)!=''TOTAL'' --Eliminamos los totales 
              AND (UPPER(a.SCOPE_DES) !=''TOTAL NACIONAL'' OR A.TABLE_ID = 2004) --Eliminamos los totales, menos de la tabla 2004, cuyo KPI se agrupa por País de Residencia
          );

        
        END;
   END;';