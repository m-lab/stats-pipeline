SELECT 
  client.geo.continent_code AS continent_code
FROM `ndt.unified_downloads`
WHERE test_date >= '2020-01-01'
AND client.geo.continent_code IS NOT NULL AND client.geo.continent_code != ''
GROUP BY continent_code
ORDER BY continent_code
