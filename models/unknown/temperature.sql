{{
    config(
        materialized='incremental'
        , unique_key='observation_timestamp'
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
),
final AS (
SELECT COALESCE (malmo_temp.observation_timestamp_malmo, pi_node_temp.observation_timestamp_pi_node) AS observation_timestamp
    ,observation_value_malmö
    ,observation_value_pi_node
FROM malmo_temp
FULL OUTER JOIN pi_node_temp 
ON malmo_temp.observation_timestamp_malmo=pi_node_temp.observation_timestamp_pi_node
)

SELECT *
FROM final

{% if is_incremental() %}
	WHERE observation_timestamp > (SELECT MAX(observation_timestamp) FROM {{ this }})
{% endif %}
