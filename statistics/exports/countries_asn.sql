SELECT *, EXTRACT(YEAR from date) as year
FROM {{ .sourceTable }}
WHERE shard = {{ .partitionID }}
ORDER BY continent_code, country_code, asn, date, bucket_min
