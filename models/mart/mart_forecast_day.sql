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
daily_metrics AS (
SELECT 
	city
	,region
	,country
	,date
	,lat
	,lon
	,timezone_id
	,year
	,max_temp_c
	,min_temp_c
	,avg_temp_c
	,max_wind_kph
	,total_precip_mm
	,total_snow_cm
	,avg_vis_km
	,avg_humidity
	,daily_will_it_rain
	,daily_chance_of_rain
	,daily_chance_of_snow
	,daily_will_it_snow
	,uv
	,condition_text
	,condition_icon ---needs work
	,condition_code
	,(CASE WHEN sunrise = 'No sunrise' THEN null ELSE sunrise END)::TIME AS sunrise_time ---needs work
	,(CASE WHEN sunset = 'No sunset' THEN null ELSE sunset END)::TIME AS sunset_time ---needs work
	,(CASE WHEN moonrise = 'No moonrise' THEN null ELSE moonrise END)::TIME AS moonrise_time ---needs work
	,(CASE WHEN moonset = 'No moonset' THEN null ELSE moonset END)::TIME AS moonset_time ---needs work
	,moon_phase
	,moon_illumination
	
FROM joined_table_forecast_day_loction

)

SELECT * FROM daily_metrics


