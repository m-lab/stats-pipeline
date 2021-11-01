SELECT *, EXTRACT(YEAR from date) as year
FROM {{ .sourceTable }}
WHERE shard = {{ .partitionID }}
ORDER BY asn, date, bucket_min
