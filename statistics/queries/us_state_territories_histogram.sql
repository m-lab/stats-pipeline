-- Aggregation by US State geographies, including US territories. Note that
-- the fields continent_code, country_code, and Subdivision1ISOCode reflect
-- M-Lab's annotated IP address based geolocation values, which may be incorrect
-- for a subset of tests conducted near international boundaries. They are
-- included for completeness, but for these results specifically, it is
-- recommended to use the fields GEOID, state, and state_name, since these are
-- taken from the US Geographies using a point-in-polygon lookup.
WITH
--Generate equal sized buckets in log-space between near 0 Mbps and ~1 Gbps+
buckets AS (
  SELECT POW(10, x-.25) AS bucket_left, POW(10,x+.25) AS bucket_right
  FROM UNNEST(GENERATE_ARRAY(0, 3.5, .5)) AS x
),
--US States are identified for test results using a GIS approach. The lat/lon
-- annotated on each test row is looked up in the polygons of states provided
-- by Google Public Datasets. 
us_states AS (
  SELECT *
  FROM
	`bigquery-public-data.geo_us_boundaries.states`  
),
--Select the initial set of tests
dl_per_location AS (
  SELECT
    date,
    client.Geo.ContinentCode AS continent_code,
    client.Geo.CountryCode AS country_code,
    CASE WHEN client.Geo.Subdivision1ISOCode != "" AND client.Geo.Subdivision1ISOCode IS NOT NULL
    THEN CONCAT(client.Geo.CountryCode,"-",client.Geo.Subdivision1ISOCode)
    ELSE CONCAT(client.Geo.CountryCode,"-",client.Geo.region)
    END AS ISO3166_2region1,
    us_states.geo_id AS GEOID,
    us_states.state_fips_code AS FIPS,
	  us_states.state AS state,
	  us_states.state_name AS state_name,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    id,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM `measurement-lab.ndt.unified_downloads`, us_states
  WHERE date BETWEEN @startdate AND @enddate
  AND ST_WITHIN(
    ST_GeogPoint(
      client.Geo.Longitude,
      client.Geo.Latitude
    ), us_states.state_geom
  )  
  AND a.MeanThroughputMbps != 0
),
--Filter for only tests With good locations and valid IPs
dl_per_location_cleaned AS (
  SELECT * FROM dl_per_location
  WHERE
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND state IS NOT NULL AND state != ""
    AND GEOID IS NOT NULL AND GEOID != ""
    AND state_name IS NOT NULL
    AND ip IS NOT NULL
),
--Fingerprint all cleaned tests, in an arbitrary but repeatable order
dl_fingerprinted AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    GEOID,
    FIPS,
    state,
	  state_name,
    ip,
    ARRAY_AGG(STRUCT(ABS(FARM_FINGERPRINT(id)) AS ffid, mbps, MinRTT) ORDER BY ABS(FARM_FINGERPRINT(id))) AS members
  FROM dl_per_location_cleaned
  GROUP BY date, continent_code, country_code, ISO3166_2region1, GEOID, FIPS, state, state_name, ip
),
--Select two random rows for each IP using a prime number larger than the 
--  total number of tests. random1 is used for per day/geo statistics in 
--  `dl_stats_per_day` and log averages using both random1 and random2
dl_random_ip_rows_perday AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    GEOID,
    FIPS,
    state,
	  state_name,
    ip,
    ARRAY_LENGTH(members) AS tests,
    members[SAFE_OFFSET(MOD(511232941,ARRAY_LENGTH(members)))] AS random1,
    members[SAFE_OFFSET(MOD(906686609,ARRAY_LENGTH(members)))] AS random2
  FROM dl_fingerprinted
),
--Calculate log averages and statistics per day from random samples
dl_stats_per_day AS (
  SELECT
    date, continent_code, country_code, ISO3166_2region1, GEOID, FIPS, state, state_name,
    COUNT(*) AS dl_samples_day,
    ROUND(POW(10,AVG(Safe.LOG10(random1.mbps))),3) AS dl_LOG_AVG_rnd1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.mbps))),3) AS dl_LOG_AVG_rnd2,
    ROUND(POW(10,AVG(Safe.LOG10(random1.MinRtt))),3) AS dl_minRTT_LOG_AVG_rnd1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.MinRtt))),3) AS dl_minRTT_LOG_AVG_rnd2,
    ROUND(MIN(random1.mbps),3) AS download_MIN,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(10)],3) AS download_Q10,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(25)],3) AS download_Q25,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(50)],3) AS download_MED,
    ROUND(AVG(random1.mbps),3) AS download_AVG,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(75)],3) AS download_Q75,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(90)],3) AS download_Q90,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(95)],3) AS download_Q95,
    ROUND(MAX(random1.mbps),3) AS download_MAX,
    ROUND(APPROX_QUANTILES(random1.MinRTT, 100) [SAFE_ORDINAL(50)],3) AS download_minRTT_MED,
  FROM dl_random_ip_rows_perday
  GROUP BY date, continent_code, country_code, ISO3166_2region1, GEOID, FIPS, state, state_name
),
--Count the samples that fall into each bucket and get frequencies
dl_histogram AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
	  GEOID,
    FIPS,
    state,
    state_name,
    --Set the lowest bucket's min to zero, so all tests below the generated min of the lowest bin are included. 
    CASE WHEN bucket_left = 0.5623413251903491 THEN 0
    ELSE bucket_left END AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(random1.mbps < bucket_right AND random1.mbps >= bucket_left) AS dl_samples_bucket,
    ROUND(COUNTIF(random1.mbps < bucket_right AND random1.mbps >= bucket_left) / COUNT(*), 3) AS dl_frac_bucket
  FROM dl_random_ip_rows_perday CROSS JOIN buckets
  GROUP BY
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
	  GEOID,
    FIPS,
    state,
    state_name,
    bucket_min,
    bucket_max
),
--Repeat for Upload tests
--Select the initial set of tests
ul_per_location AS (
  SELECT
    date,
    client.Geo.ContinentCode AS continent_code,
    client.Geo.CountryCode AS country_code,
    CASE WHEN client.Geo.Subdivision1ISOCode != "" AND client.Geo.Subdivision1ISOCode IS NOT NULL
    THEN CONCAT(client.Geo.CountryCode,"-",client.Geo.Subdivision1ISOCode)
    ELSE CONCAT(client.Geo.CountryCode,"-",client.Geo.region)
    END AS ISO3166_2region1,
    us_states.geo_id AS GEOID,
    us_states.state_fips_code AS FIPS,
  	us_states.state AS state,
  	us_states.state_name AS state_name,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    id,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM `measurement-lab.ndt.unified_uploads`, us_states
  WHERE date BETWEEN @startdate AND @enddate
  AND ST_WITHIN(
    ST_GeogPoint(
      client.Geo.Longitude,
      client.Geo.Latitude
    ), us_states.state_geom
  )  
  AND a.MeanThroughputMbps != 0
),
--Filter for only tests With good locations and valid IPs
ul_per_location_cleaned AS (
  SELECT * FROM ul_per_location
  WHERE
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND GEOID IS NOT NULL AND GEOID != ""
    AND state IS NOT NULL
    AND state_name IS NOT NULL
    AND ip IS NOT NULL
),
--Fingerprint all cleaned tests, in an arbitrary but repeatable order.
ul_fingerprinted AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
	  GEOID,
    FIPS,
    state,
    state_name,
    ip,
    ARRAY_AGG(STRUCT(ABS(FARM_FINGERPRINT(id)) AS ffid, mbps, MinRTT) ORDER BY ABS(FARM_FINGERPRINT(id))) AS members
  FROM ul_per_location_cleaned
  GROUP BY date, continent_code, country_code, ISO3166_2region1, GEOID, FIPS, state, state_name, ip
),
--Select two random rows for each IP using a prime number larger than the 
--  total number of tests. random1 is used for per day/geo statistics in 
--  `ul_stats_per_day` and log averages using both random1 and random2
ul_random_ip_rows_perday AS (
  SELECT
    date,
    continent_code,
    country_code,
	  ISO3166_2region1,
    GEOID,
    FIPS,
    state,
	  state_name,
    ip,
    ARRAY_LENGTH(members) AS tests,
    members[SAFE_OFFSET(MOD(511232941,ARRAY_LENGTH(members)))] AS random1,
    members[SAFE_OFFSET(MOD(906686609,ARRAY_LENGTH(members)))] AS random2
  FROM ul_fingerprinted
),
--Calculate log averages and statistics per day from random samples
ul_stats_per_day AS (
  SELECT
    date, continent_code, country_code, ISO3166_2region1, GEOID, FIPS, state, state_name,
    COUNT(*) AS ul_samples_day,
    ROUND(POW(10,AVG(Safe.LOG10(random1.mbps))),3) AS ul_LOG_AVG_rnd1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.mbps))),3) AS ul_LOG_AVG_rnd2,
    ROUND(POW(10,AVG(Safe.LOG10(random1.MinRtt))),3) AS ul_minRTT_LOG_AVG_rnd1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.MinRtt))),3) AS ul_minRTT_LOG_AVG_rnd2,
    ROUND(MIN(random1.mbps),3) AS upload_MIN,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(10)],3) AS upload_Q10,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(25)],3) AS upload_Q25,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(50)],3) AS upload_MED,
    ROUND(AVG(random1.mbps),3) AS upload_AVG,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(75)],3) AS upload_Q75,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(90)],3) AS upload_Q90,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(95)],3) AS upload_Q95,
    ROUND(MAX(random1.mbps),3) AS upload_MAX,
    ROUND(APPROX_QUANTILES(random1.MinRTT, 100) [SAFE_ORDINAL(50)],3) AS upload_minRTT_MED,
  FROM ul_random_ip_rows_perday
  GROUP BY date, continent_code, country_code, ISO3166_2region1, GEOID, FIPS, state, state_name 
),
--Count the samples that fall into each bucket and get frequencies
ul_histogram AS (
  SELECT
    date,
    continent_code,
    country_code,
	  ISO3166_2region1,
    GEOID,
    FIPS,
    state,
    state_name,
    --Set the lowest bucket's min to zero, so all tests below the generated min of the lowest bin are included. 
    CASE WHEN bucket_left = 0.5623413251903491 THEN 0
    ELSE bucket_left END AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(random1.mbps < bucket_right AND random1.mbps >= bucket_left) AS ul_samples_bucket,
    ROUND(COUNTIF(random1.mbps < bucket_right AND random1.mbps >= bucket_left) / COUNT(*), 3) AS ul_frac_bucket
  FROM ul_random_ip_rows_perday CROSS JOIN buckets
  GROUP BY
    date,
    continent_code,
    country_code,
	  ISO3166_2region1,
    GEOID,
    FIPS,
    state,
    state_name,
    bucket_min,
    bucket_max
),
--Gather final result set
results AS (
  SELECT *, MOD(ABS(FARM_FINGERPRINT(state)), 4000) as shard FROM dl_histogram
  JOIN ul_histogram USING (date, continent_code, country_code, ISO3166_2region1, GEOID, FIPS, state, state_name, bucket_min, bucket_max)
  JOIN dl_stats_per_day USING (date, continent_code, country_code, ISO3166_2region1, GEOID, FIPS, state, state_name )
  JOIN ul_stats_per_day USING (date, continent_code, country_code, ISO3166_2region1, GEOID, state, FIPS, state_name )
)
--Show the results
SELECT * FROM results
