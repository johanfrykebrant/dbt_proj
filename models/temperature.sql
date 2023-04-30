{{
    config(
        materialized='incremental'
		, schema='analytics'
    )
}}

WITH pi_node_temp AS(
SELECT 	date_trunc('hour', observation_timestamp) AS observation_timestamp_pi_node,
		observation_value AS observation_value_pi_node
FROM staging.observations
WHERE 	observation_name = 'Lufttemperatur'
		AND observation_station = 'pi-node'
),
malmo_temp AS (
SELECT 	observation_timestamp AS observation_timestamp_malmo,
		observation_value AS observation_value_malmö
FROM staging.observations
WHERE 	observation_name = 'Lufttemperatur'
		AND observation_station = 'Malmö A'
)

SELECT COALESCE (malmo_temp.observation_timestamp_malmo, pi_node_temp.observation_timestamp_pi_node) AS observation_timestamp
    ,observation_value_malmö
    ,observation_value_pi_node
FROM malmo_temp
FULL OUTER JOIN pi_node_temp 
ON malmo_temp.observation_timestamp_malmo=pi_node_temp.observation_timestamp_pi_node

{% if is_incremental() %}
	AND COALESCE (malmo_temp.observation_timestamp_malmo, pi_node_temp.observation_timestamp_pi_node) > (SELECT MAX(observation_timestamp) FROM {{ this }})
{% endif %}
