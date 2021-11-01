SELECT *, EXTRACT(YEAR from date) as year
FROM {{ .sourceTable }}
WHERE shard = {{ .partitionID }}
ORDER BY continent_code, country_code, ISO3166_2region1, city, asn, date, bucket_min
