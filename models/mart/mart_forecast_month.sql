
WITH joined_table_forecast_day_loction AS 
	(
SELECT * FROM {{ref('prep_forecast_day')}}
LEFT JOIN {{ref('staging_location')}}
USING (city,region,country)
	)
,
	
    monthly_conditions AS (

SELECT 
	city
	,region
	,country
	,lat
	,lon
	,timezone_id
	,year
	,month_of_year
	,month_of_year_n
	
	,MAX(max_temp_c) AS max_temp_c_m
	,MIN(min_temp_c) AS min_temp_c_m
	,AVG(avg_temp_c) AS avg_temp_c_m
	,SUM(total_precip_mm) AS total_precip_mm_m
	,SUM(total_snow_cm) AS total_snow_cm_m
	,AVG(avg_vis_km) AS avg_vis_km_m
	,AVG(avg_humidity) AS avg_humidity_m
	,SUM(daily_will_it_rain) AS monthly_will_rain_days
	,SUM(daily_will_it_snow) AS monthly_will_snow_days
	,AVG(daily_chance_of_rain) AS avg_chance_rain_m
	,AVG(daily_chance_of_snow) AS avg_chance_snow_m
	,AVG(uv) AS avg_uv_m
	,AVG(moon_illumination) AS avg_moon_illumination_m
	,SUM(CASE 
			WHEN condition_text = 'Sunny' THEN 1
			ELSE 0
			END) AS n_sunny_days
	,SUM(CASE 
			WHEN condition_text in ('Overcast', 'Partly cloudy', 'Cloudy', 'Freezing fog') THEN 1
			ELSE 0
			END) AS n_cloudy_days
	,SUM(CASE 
			WHEN condition_text in ('Patchy rain possible','Moderate or heavy rain shower', 'Light rain shower',
                            'Mist', 'Moderate rain at times', 'Patchy light rain with thunder',
                            'Patchy light drizzle', 'Thundery outbreaks possible', 'Heavy rain at times', 
                            'Fog', 'Moderate or heavy rain with thunder',  'Light drizzle', 'Light rain', 
                            'Patchy light rain', 'Heavy rain', 'Moderate rain', 'Torrential rain shower', 
                            'Light snow showers', 'Moderate or heavy snow showers', 'Light freezing rain',
                            'Moderate or heavy freezing rain', 'Heavy freezing drizzle') THEN 1
			ELSE 0
			END) AS n_rainy_days
	,SUM(CASE 
			WHEN condition_text in ('Patchy light snow', 'Heavy snow', 'Light sleet', 'Light snow', 
                            'Moderate snow', 'Light sleet showers', 'Patchy heavy snow',
                            'Patchy moderate snow', 'Moderate or heavy snow with thunder',
                            'Moderate or heavy sleet', 'Blizzard', 'Blowing snow', 'Patchy snow possible', 
                            'Moderate or heavy showers of ice pellets', 'Patchy light snow with thunder',
                            'Patchy sleet possible', 'Ice pellets') THEN 1
			ELSE 0
			END) AS n_snowy_days
	
	
FROM joined_table_forecast_day_loction
GROUP BY city
	,region
	,country
	,lat
	,lon
	,timezone_id
	,year
	,month_of_year
	)

SELECT * FROM monthly_conditions