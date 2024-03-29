SELECT *, EXTRACT(YEAR from date) as year
FROM {{ .sourceTable }}
WHERE shard = {{ .partitionID }}
ORDER BY continent_code, country_code, date, bucket_min
