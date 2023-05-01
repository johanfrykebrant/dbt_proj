SELECT created_timestamp_utc 
, COUNT(created_timestamp_utc) as nbr_of_rows
, 'staging.forecasts' AS sink_table
FROM staging.forecasts
GROUP BY created_timestamp_utc

UNION ALL

SELECT created_timestamp_utc
, COUNT(created_timestamp_utc) as nbr_of_rows
, 'staging.observations' AS sink_table
FROM staging.observations
GROUP BY created_timestamp_utc

ORDER BY created_timestamp_utc DESC