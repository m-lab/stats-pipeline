WITH
# Select the initial set of download results
dl_per_location AS (
  SELECT
    test_date,
    client.Geo.continent_code AS continent_code,
    client.Geo.country_code AS country_code,
    client.Geo.country_name AS country_name,
    CONCAT(client.Geo.country_code, '-', client.Geo.region) AS ISO3166_2region1,
    client.Geo.city AS city,
    client.Network.ASNumber AS ASNumber,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip
  FROM `measurement-lab.ndt.unified_downloads`
  WHERE test_date = @startday
),
# With good locations and valid IPs
dl_per_location_cleaned AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    ASNumber,
    ip
  FROM dl_per_location
  WHERE 
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND country_name IS NOT NULL AND country_name != ""
    AND ISO3166_2region1 IS NOT NULL AND ISO3166_2region1 != ""
    AND city IS NOT NULL AND city != ""
    AND ip IS NOT NULL
    AND ASNumber IS NOT NULL AND ASNumber != ""
),
# Gather statistics per geo, day, ASN
dl_stats_per_day_per_as AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    ASNumber,
    COUNT(*) AS total_dl_tests_per_as, 
    COUNT(DISTINCT(ip)) AS total_dl_ips_per_as
  FROM dl_per_location_cleaned
  GROUP BY test_date, continent_code, country_code, country_name,
    ISO3166_2region1, city, ASNumber
),
##
# Select the initial set of upload results
ul_per_location AS (
  SELECT
    test_date,
    client.Geo.continent_code AS continent_code,
    client.Geo.country_code AS country_code,
    client.Geo.country_name AS country_name,
    CONCAT(client.Geo.country_code, '-', client.Geo.region) AS ISO3166_2region1,
    client.Geo.city AS city,
    client.Network.ASNumber AS ASNumber,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip
  FROM `measurement-lab.ndt.unified_uploads`
  WHERE test_date = @startday
),
# With good locations and valid IPs
ul_per_location_cleaned AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    ASNumber,
    ip
  FROM ul_per_location
  WHERE 
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND country_name IS NOT NULL AND country_name != ""
    AND ISO3166_2region1 IS NOT NULL AND ISO3166_2region1 != ""
    AND city IS NOT NULL AND city != ""
    AND ip IS NOT NULL
    AND ASNumber IS NOT NULL AND ASNumber != ""
),
# Gather statistics per geo, day, ASN
ul_stats_per_day_per_as AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    ASNumber,
    COUNT(*) AS total_ul_tests_per_as, 
    COUNT(DISTINCT(ip)) AS total_ul_ips_per_as
  FROM ul_per_location_cleaned
  GROUP BY test_date, continent_code, country_code, country_name, 
    ISO3166_2region1, city, ASNumber
)
# Show the results
SELECT * FROM dl_stats_per_day_per_as
JOIN ul_stats_per_day_per_as USING (test_date, continent_code, 
    country_code, country_name, ISO3166_2region1, city, ASNumber)
ORDER BY test_date, continent_code, country_code, country_name,
    ISO3166_2region1, city, ASNumber
