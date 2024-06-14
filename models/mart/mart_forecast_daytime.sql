--Goal: A table showing daily metrics, including location and geo information. 
--For urls in condition_icon column we need a markdown version as a new column. Example:
--![weather_icon](//cdn.weatherapi.com/weather/64x64/day/353.png?width=35)


WITH joined_table_forecast_day_loction AS 
	(
SELECT * FROM {{ref('prep_forecast_day')}}
LEFT JOIN {{ref('staging_location')}}
USING (city,region,country)
	)
,
adding_features AS (

SELECT 
	city
	,region
	,country
	,date
	,lat
	,lon
	,timezone_id
	,year
	,(CASE WHEN sunrise = 'No sunrise' THEN null ELSE sunrise END)::TIME AS sunrise_time ---needs work
	,(CASE WHEN sunset = 'No sunset' THEN null ELSE sunset END)::TIME AS sunset_time ---needs work
	,max_temp_c
	,min_temp_c
	, CASE 
		WHEN max_temp_c > 30 THEN 'hot'
		WHEN max_temp_c > 20 THEN 'warm'
		WHEN max_temp_c > 10 THEN 'comfortable'
		ELSE 'cold'
		END
		AS feels
	
FROM joined_table_forecast_day_loction

)
,
daylight_metrics AS (
	SELECT *
	,(sunset_time - sunrise_time) AS daylight_length
	,(max_temp_c - min_temp_c) AS temp_diff
	
	FROM adding_features
)

SELECT * FROM daylight_metrics


