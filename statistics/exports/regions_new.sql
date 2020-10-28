SELECT *, EXTRACT(YEAR from date) as year
FROM {{ .sourceTable }}
{{ .whereClause }}
ORDER BY continent_code, country_code, country_name, ISO3166_2region1, date, bucket_min