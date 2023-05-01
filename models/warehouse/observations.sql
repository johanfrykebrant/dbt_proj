{{
    config(
		unique_key= 'forecast_timestamp'
    )
}}


WITH 	observations_all AS (
			SELECT 	date_trunc('hour', observation_timestamp) AS observation_timestamp,
					observation_station,
					observation_name,
					observation_value
			FROM staging.observations
			WHERE observation_name IN ('Lufttemperatur','Nederbördsmängd')
			)
		,observations_pivoted AS (
			SELECT 	observation_timestamp, 
					AVG(CASE WHEN observation_name = 'Lufttemperatur'
								AND observation_station = 'pi-node'
					THEN observation_value END) AS temperature_degC_pi_node,
					AVG(CASE WHEN observation_name = 'Lufttemperatur'
								AND observation_station = 'Malmö A'
					THEN observation_value END) AS temperature_degC_malmo,
					AVG(CASE WHEN observation_name = 'Nederbördsmängd'
								AND observation_station = 'Malmö A'
					THEN observation_value END) AS rain_mm_malmo
			FROM observations_all
			GROUP BY observation_timestamp
)

SELECT 	*
FROM observations_pivoted
ORDER BY observation_timestamp DESC