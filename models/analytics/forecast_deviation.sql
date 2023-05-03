
{{
    config(
		unique_key= 'timestamp'
    )
}}


WITH cte AS (
    SELECT *
    FROM staging_warehouse.observations
    FULL OUTER JOIN staging_warehouse.forecasts 
        ON staging_warehouse.observations.observation_timestamp = staging_warehouse.forecasts.forecast_timestamp
)

SELECT observation_timestamp AS timestamp,
        cte.temperature_degc_malmo - cte.temperature_degc_12hrs AS temperature_deviation_12hrs,
        cte.temperature_degc_malmo - cte.temperature_degc_24hrs AS temperature_deviation_24hrs,
        cte.temperature_degc_malmo - cte.temperature_degc_48hrs AS temperature_deviation_48hrs
FROM cte