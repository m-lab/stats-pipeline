SELECT *, EXTRACT(YEAR from date) as year
FROM {{ .sourceTable }}
{{ .whereClause }}
ORDER BY continent_code, country_code, year, bucket_min