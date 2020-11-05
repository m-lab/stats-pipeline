SELECT *, EXTRACT(YEAR from date) as year
FROM {{ .sourceTable }}
{{ .whereClause }}
ORDER BY continent_code, country_code, ISO3166_2region1, city, year, bucket_min