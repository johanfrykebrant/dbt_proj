{{
    config(
		unique_key= 'forecast_timestamp'
    )
}}

-- implement incremental merge updates!

WITH 	forecasts_all AS (
	SELECT 	name AS forecast_name
			,forecast_timestamp,
			CAST(EXTRACT(epoch FROM (forecast_timestamp - forecast_approved_timestamp)/(60*60)) AS INT) AS hours_diff
			,forecast_value
	FROM staging.forecasts
		JOIN staging.forecast_name_codes
		ON forecast_code = code
	WHERE CAST(EXTRACT(epoch FROM (forecast_timestamp - forecast_approved_timestamp)/(60*60)) AS INT) IN (12,24,48)
		AND name IN ('Air temperature', 'Maximum precipitation intensity','Minimum precipitation intensity','Mean precipitation intensity')
),
-- postgres does not support PIVOT(). Below is a cumbersome way of doing the same thing...
forecast_pivoted AS(
	SELECT	forecast_timestamp
			, AVG(CASE WHEN forecast_name = 'Air temperature'
				  			AND hours_diff = 12
				  THEN CAST(forecast_value AS NUMERIC(5,2)) END) AS temperature_degC_12hrs
			, AVG(CASE WHEN forecast_name = 'Air temperature'
				  			AND hours_diff = 24
				  THEN CAST(forecast_value AS NUMERIC(5,2)) END) AS temperature_degC_24hrs
			, AVG(CASE WHEN forecast_name = 'Air temperature'
				  			AND hours_diff = 48
				  THEN CAST(forecast_value AS NUMERIC(5,2)) END) AS temperature_degC_48hrs
			, AVG(CASE WHEN forecast_name = 'Mean precipitation intensity'
				  			AND hours_diff = 12
			-- unit in source is kg/m2/h but for water kg/m2/h ~ mm/h
				  THEN CAST(forecast_value AS NUMERIC(5,2)) END) AS mean_rain_mm_12hrs
			, AVG(CASE WHEN forecast_name = 'Mean precipitation intensity'
				  			AND hours_diff = 24
				  THEN CAST(forecast_value AS NUMERIC(5,2)) END) AS mean_rain_mm_24hrs
			, AVG(CASE WHEN forecast_name = 'Mean precipitation intensity'
				  			AND hours_diff = 48
				  THEN CAST(forecast_value AS NUMERIC(5,2)) END) AS mean_rain_mm_48hrs
	FROM forecasts_all
	GROUP BY forecast_timestamp
)
	
SELECT *
FROM forecast_pivoted
