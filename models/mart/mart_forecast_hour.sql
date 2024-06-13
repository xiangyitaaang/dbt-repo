
WITH joined_table_forecast_hour_loction AS 
	(
SELECT * FROM {{ref('prep_forecast_hour')}}
LEFT JOIN {{ref('staging_location')}}
USING (city,region,country)
	)
,
	
    hourly_metrics AS (

SELECT 
	date
    ,time
    ,hour
    ,city
	,region
	,country
    ,date_time
    ,TEXT(CASE WHEN is_day = 0 THEN 'night' ELSE 'day' END) AS day_night
    ,condition_text
    ,condition_icon
    ,temp_c
    ,wind_kph
    ,wind_degree
    ,wind_dir
    ,pressure_mb
    ,precip_mm
    ,snow_cm
    ,humidity
    ,cloud
    ,feelslike_c
    ,windchill_c
    ,heatindex_C
    ,dewpoint_c
    ,will_it_rain
    ,chance_of_rain
    ,will_it_snow
    ,chance_of_snow
    ,vis_km
    ,gust_kph
    ,uv

FROM joined_table_forecast_hour_loction

)

SELECT * FROM hourly_metrics