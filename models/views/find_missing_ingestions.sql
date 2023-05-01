{{
    config(
		unique_key= 'ingestion_timestamp'
    )
}}

WITH 	ingestion_timestamps AS 
    (SELECT date_trunc('hour', created_timestamp_utc) AS ingestion_timestamp 
    , COUNT( DISTINCT date_trunc('hour', created_timestamp_utc)) AS nbr_of_ingestion
    , 'staging.forecasts' AS sink_table
    FROM staging.forecasts
    GROUP BY created_timestamp_utc

    UNION ALL

    SELECT date_trunc('hour', created_timestamp_utc) AS ingestion_timestamp
    , COUNT( DISTINCT date_trunc('hour', created_timestamp_utc)) AS nbr_of_ingestion
    , 'staging.observations' AS sink_table
    FROM staging.observations
    GROUP BY created_timestamp_utc
    )

SELECT 	SUM(nbr_of_ingestion) AS nbr_of_ingestion
		, ingestion_timestamp
FROM ingestion_timestamps
GROUP BY ingestion_timestamp