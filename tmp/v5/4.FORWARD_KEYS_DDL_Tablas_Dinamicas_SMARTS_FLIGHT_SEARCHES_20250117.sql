create or replace dynamic table SIT.FC_FWK_SMARTS_FLIGHT_SEARCHES(
	SEARCH_DATE_CALENDAR_KEY,
	--SEARCH_LEVEL_KEY,
	SEARCH_LEAD_TIME,
	SEARCH_DISTANCE_CAT_KEY,
	SEARCH_DISTANCE_KM,
	SEARCH_INTERNATIONAL,
	SEARCH_ORIGIN_CITY_KEY,
	SEARCH_ORIGIN_COUNTRY_KEY,
	SEARCH_DESTINATION_CITY_KEY,
	SEARCH_DESTINATION_COUNTRY_KEY,
	SEGMENT_TYPE_KEY,
	TRIP_TYPE_KEY,
	SEARCH_DEPARTURE_DATE_CALENDAR_KEY,
	--DEPARTURE_DATE_FLEXIBILITY_AVG,
	EXTRACTION_DATE_CALENDAR_KEY,
	--LOS_ORIGIN_CAT_KEY,
	--LOS_ORIGIN_HOURS,
	LOS_DESTINATION_CAT_KEY,
	LOS_DESTINATION_HOURS,
	TRIP_ORIGIN_CITY_KEY,
	TRIP_ORIGIN_COUNTRY_KEY,
	PAX_NATIONALITY_KEY,
	SEARCHES,
	SEARCH_PAX,
	LOAD_TIME
) target_lag = '1 day' refresh_mode = AUTO initialize = ON_CREATE warehouse = PRE_ETL_WH
 cluster by (SEARCH_DATE_CALENDAR_KEY) as 
          SELECT
            TO_NUMBER(YEAR(ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DATE) 
                || LPAD(MONTH(ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DATE), 2, '0') 
                || LPAD(DAY(ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DATE), 2, '0')) AS SEARCH_DATE_CALENDAR_KEY
            --,DM_FWK_SEARCH_LEVEL.ID AS SEARCH_LEVEL_KEY --,4
			,ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_LEAD_TIME AS SEARCH_LEAD_TIME
			,DM_FWK_SEARCH_DISTANCE_CAT.ID AS SEARCH_DISTANCE_CAT_KEY
			,ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DISTANCE_KM AS SEARCH_DISTANCE_KM
            ,DECODE(UPPER(ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_INTERNATIONAL), 'TRUE', 1, 'FALSE', 0, null) AS SEARCH_INTERNATIONAL
            ,SEARCH_ORIGIN_CITY.ID AS SEARCH_ORIGIN_CITY_KEY
            ,SEARCH_ORIGIN_COUNTRY.ID AS SEARCH_ORIGIN_COUNTRY_KEY
            ,SEARCH_DESTINATION_CITY.ID AS SEARCH_DESTINATION_CITY_KEY
            ,SEARCH_DESTINATION_COUNTRY.ID AS SEARCH_DESTINATION_COUNTRY_KEY
            ,DM_FWK_SEGMENT_TYPE.ID AS SEGMENT_TYPE_KEY
            ,DM_FWK_TRIP_TYPE.ID AS TRIP_TYPE_KEY
            ,TO_NUMBER(YEAR(ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DEPARTURE_DATE) 
                || LPAD(MONTH(ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DEPARTURE_DATE), 2, '0') 
                || LPAD(DAY(ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DEPARTURE_DATE), 2, '0')) AS SEARCH_DEPARTURE_DATE_CALENDAR_KEY
            --,ST_FWK_SMARTS_FLIGHT_SEARCHES.DEPARTURE_DATE_FLEXIBILITY_AVG AS DEPARTURE_DATE_FLEXIBILITY_AVG
            --,0
            ,TO_NUMBER(YEAR(ST_FWK_SMARTS_FLIGHT_SEARCHES.EXTRACTION_DATE) 
                || LPAD(MONTH(ST_FWK_SMARTS_FLIGHT_SEARCHES.EXTRACTION_DATE), 2, '0') 
                || LPAD(DAY(ST_FWK_SMARTS_FLIGHT_SEARCHES.EXTRACTION_DATE), 2, '0')) AS EXTRACTION_DATE_CALENDAR_KEY
            --,LOS_ORIGIN_CAT.ID AS LOS_ORIGIN_CAT_KEY
            --,1
            --,ST_FWK_SMARTS_FLIGHT_SEARCHES.LOS_ORIGIN_HOURS AS LOS_ORIGIN_HOURS
            --,0
            ,LOS_DESTINATION_CAT.ID AS LOS_DESTINATION_CAT_KEY
            --,1
            ,ST_FWK_SMARTS_FLIGHT_SEARCHES.LOS_AT_DESTINATION_HOURS AS LOS_DESTINATION_HOURS
            --,0
            ,TRIP_ORIGIN_CITY.ID AS TRIP_ORIGIN_CITY_KEY
            ,TRIP_ORIGIN_COUNTRY.ID AS TRIP_ORIGIN_COUNTRY_KEY
            ,DM_REF_COUNTRY.ID AS PAX_NATIONALITY_KEY
            ,ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCHES AS SEARCHES
            ,ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_PAX AS SEARCH_PAX
			,SYSDATE() AS LOAD_TIME
            FROM
            STAGING.ST_FWK_SMARTS_FLIGHT_SEARCHES_V2 ST_FWK_SMARTS_FLIGHT_SEARCHES
            ,(select * from DM_REF_CITY where ACTIVESTATUS = 1) SEARCH_ORIGIN_CITY 
            ,(select * from DM_REF_CITY where ACTIVESTATUS = 1) SEARCH_DESTINATION_CITY 
            ,(select * from DM_REF_CITY where ACTIVESTATUS = 1) TRIP_ORIGIN_CITY 
            ,DM_FWK_SEARCH_DISTANCE_CAT
			,DM_REF_COUNTRY SEARCH_ORIGIN_COUNTRY 
            ,DM_REF_COUNTRY SEARCH_DESTINATION_COUNTRY 
            ,DM_REF_COUNTRY TRIP_ORIGIN_COUNTRY 
            ,DM_FWK_SEGMENT_TYPE
            ,DM_FWK_TRIP_TYPE
            --,DM_FWK_SEARCH_LEVEL
            --,DM_FWK_LOS_CAT LOS_ORIGIN_CAT
            ,DM_FWK_LOS_CAT LOS_DESTINATION_CAT
            ,DM_REF_COUNTRY
            WHERE 1=1
			AND ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DISTANCE_CAT = DM_FWK_SEARCH_DISTANCE_CAT.NAME
            AND NVL(ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_ORIGIN_CITY, '000') = SEARCH_ORIGIN_CITY.CITYCODE
            AND ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_ORIGIN_COUNTRY = SEARCH_ORIGIN_COUNTRY.COUNTRYCODE
            AND NVL(ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DESTINATION_CITY, '000') = SEARCH_DESTINATION_CITY.CITYCODE
            AND ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DESTINATION_COUNTRY = SEARCH_DESTINATION_COUNTRY.COUNTRYCODE
            AND NVL(ST_FWK_SMARTS_FLIGHT_SEARCHES.TRIP_ORIGIN_CITY, '000') = TRIP_ORIGIN_CITY.CITYCODE
            AND ST_FWK_SMARTS_FLIGHT_SEARCHES.TRIP_ORIGIN_COUNTRY = TRIP_ORIGIN_COUNTRY.COUNTRYCODE
            AND ST_FWK_SMARTS_FLIGHT_SEARCHES.SEGMENT_TYPE = DM_FWK_SEGMENT_TYPE.NAME
            AND ST_FWK_SMARTS_FLIGHT_SEARCHES.TRIP_TYPE = DM_FWK_TRIP_TYPE.NAME
            --AND ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_LEVEL = DM_FWK_SEARCH_LEVEL.NAME
            --AND ST_FWK_SMARTS_FLIGHT_SEARCHES.LOS_ORIGIN_CAT = LOS_ORIGIN_CAT.NAME
            AND ST_FWK_SMARTS_FLIGHT_SEARCHES.LOS_AT_DESTINATION_CAT = LOS_DESTINATION_CAT.NAME
            AND ST_FWK_SMARTS_FLIGHT_SEARCHES.PAX_NATIONALITY = DM_REF_COUNTRY.COUNTRYCODE
            AND ADD_MONTHS(ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DATE, 12) >= ST_FWK_SMARTS_FLIGHT_SEARCHES.SEARCH_DEPARTURE_DATE;