SELECT 
	client.geo.continent_code AS continent_code,
	client.geo.country_code AS country_code,
	client.Geo.region AS region
FROM `ndt.unified_downloads`
WHERE 
	test_date >= '2020-01-01'
	AND client.geo.continent_code IS NOT NULL AND client.geo.continent_code != ''
	AND client.geo.country_code IS NOT NULL AND client.geo.country_code != ''
	AND client.Geo.region IS NOT NULL AND client.Geo.region != ''
GROUP BY
	continent_code,
	country_code,
	region
ORDER BY
	continent_code,
	country_code,
	region