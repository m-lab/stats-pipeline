WITH
--Generate equal sized buckets in log-space between near 0 Mbps and ~1 Gbps+
buckets AS (
  SELECT POW(10, x-.25) AS bucket_left, POW(10,x+.25) AS bucket_right
  FROM UNNEST(GENERATE_ARRAY(0, 3.5, .5)) AS x
),
--EU NUTS 2 geographies are identified for test results using a GIS approach.
-- '2016' in this filename refers to the 2016 release of NUTS geographies from
-- the European Union.
-- The lat/lon annotated on each test row is looked up in the NUTS 2 polygons
-- provided by https://www.broadband-mapping.eu/
nuts2 AS (
  SELECT
    geometry AS WKT, LEVL_CODE, NUTS_ID, CNTR_CODE, NAME_LATN, NUTS_NAME, MOUNT_TYPE, URBN_TYPE, COAST_TYPE, FID
  FROM
    `measurement-lab.geographies.eu_NUTS_2_2016_01m`
),
--Select the initial set of tests
--Filter for only tests With good locations and valid IPs
dl_per_location_cleaned AS (
  SELECT
    id,
    date,
    client.Geo.ContinentCode AS continent_code,
    client.Geo.CountryCode AS country_code,
	  nuts2.NUTS_ID,
	  nuts2.NUTS_NAME,
    client.Network.ASNumber AS asn,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM `measurement-lab.ndt.unified_downloads` tests, nuts2
  WHERE date BETWEEN "2018-01-01" AND "2018-12-31" #@startdate AND @enddate
  AND a.MeanThroughputMbps != 0
  AND client.Geo.ContinentCode IS NOT NULL AND client.Geo.ContinentCode != ""
  AND client.Geo.CountryCode IS NOT NULL AND client.Geo.CountryCode != ""
  AND client.Network.ASNumber IS NOT NULL
  AND Client.IP IS NOT NULL
  AND ST_WITHIN(
    ST_GeogPoint(
      client.Geo.Longitude,
      client.Geo.Latitude
    ), nuts2.WKT
  )    
),
--Fingerprint all cleaned tests, in an arbitrary but repeatable order
dl_fingerprinted AS (
  SELECT
    date,
      continent_code,
      country_code,
	  NUTS_ID,
	  NUTS_NAME,
      asn,
      ip,
      ARRAY_AGG(STRUCT(ABS(FARM_FINGERPRINT(id)) AS ffid, mbps, MinRTT) ORDER BY ABS(FARM_FINGERPRINT(id))) AS members
  FROM dl_per_location_cleaned
  GROUP BY 
  date, continent_code, country_code, NUTS_ID, NUTS_NAME, asn, ip
),
--Select two random rows for each IP using a prime number larger than the 
--  total number of tests. random1 is used for per day/geo statistics in 
--  `dl_stats_per_day` and log averages using both random1 and random2
dl_random_ip_rows_perday AS (
  SELECT
    date,
    continent_code,
    country_code,
	NUTS_ID,
	NUTS_NAME,
    asn,
    ip,
    ARRAY_LENGTH(members) AS tests,
    members[SAFE_OFFSET(MOD(511232941,ARRAY_LENGTH(members)))] AS random1,
    members[SAFE_OFFSET(MOD(906686609,ARRAY_LENGTH(members)))] AS random2
  FROM dl_fingerprinted
),
--Calculate log averages and statistics per day from random samples
dl_stats_per_day AS (
  SELECT
    date, continent_code, country_code, NUTS_ID, NUTS_NAME, asn,
    COUNT(*) AS dl_samples_day,
    ROUND(POW(10,AVG(Safe.LOG10(random1.mbps))),3) AS dl_LOG_AVG_rnd1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.mbps))),3) AS dl_LOG_AVG_rnd2,
    ROUND(POW(10,AVG(Safe.LOG10(random1.MinRtt))),3) AS dl_minRTT_LOG_AVG_rnd1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.MinRtt))),3) AS dl_minRTT_LOG_AVG_rnd2,
    ROUND(MIN(random1.mbps),3) AS download_MIN,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(25)],3) AS download_Q25,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(50)],3) AS download_MED,
    ROUND(AVG(random1.mbps),3) AS download_AVG,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(75)],3) AS download_Q75,
    ROUND(MAX(random1.mbps),3) AS download_MAX,
    ROUND(APPROX_QUANTILES(random1.MinRTT, 100) [SAFE_ORDINAL(50)],3) AS download_minRTT_MED,
  FROM dl_random_ip_rows_perday
  GROUP BY date, continent_code, country_code, NUTS_ID, NUTS_NAME, asn
),
--Count the samples that fall into each bucket and get frequencies
dl_histogram AS (
  SELECT
    date,
    continent_code,
    country_code,
	NUTS_ID,
	NUTS_NAME,
    asn,
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
	NUTS_ID,
	NUTS_NAME,
    asn,
    bucket_min,
    bucket_max
),
--Repeat for Upload tests
--Select the initial set of tests
--Filter for only tests With good locations and valid IPs
ul_per_location_cleaned AS (
  SELECT
    id,
    date,
    client.Geo.ContinentCode AS continent_code,
    client.Geo.CountryCode AS country_code,
    nuts2.NUTS_ID,
    nuts2.NUTS_NAME,
    client.Network.ASNumber AS asn,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM `measurement-lab.ndt.unified_uploads`, nuts2
  WHERE date BETWEEN "2018-01-01" AND "2018-12-31" #@startdate AND @enddate
  AND a.MeanThroughputMbps != 0
  AND client.Geo.ContinentCode IS NOT NULL AND client.Geo.ContinentCode != ""
  AND client.Geo.CountryCode IS NOT NULL AND client.Geo.CountryCode != ""
  AND client.Network.ASNumber IS NOT NULL
  AND Client.IP IS NOT NULL
  AND ST_WITHIN(
    ST_GeogPoint(
      client.Geo.Longitude,
      client.Geo.Latitude
    ), nuts2.WKT
  )    
),
--Fingerprint all cleaned tests, in an arbitrary but repeatable order.
ul_fingerprinted AS (
  SELECT
    date,
    continent_code,
    country_code,
	NUTS_ID,
	NUTS_NAME,
    asn,
    ip,
    ARRAY_AGG(STRUCT(ABS(FARM_FINGERPRINT(id)) AS ffid, mbps, MinRTT) ORDER BY ABS(FARM_FINGERPRINT(id))) AS members
  FROM ul_per_location_cleaned
  GROUP BY date, continent_code, country_code, NUTS_ID, NUTS_NAME, asn, ip
),
--Select two random rows for each IP using a prime number larger than the 
--  total number of tests. random1 is used for per day/geo statistics in 
--  `ul_stats_per_day` and log averages using both random1 and random2
ul_random_ip_rows_perday AS (
  SELECT
    date,
    continent_code,
    country_code,
    NUTS_ID,
	NUTS_NAME,
    asn,
	  ip,
    ARRAY_LENGTH(members) AS tests,
    members[SAFE_OFFSET(MOD(511232941,ARRAY_LENGTH(members)))] AS random1,
    members[SAFE_OFFSET(MOD(906686609,ARRAY_LENGTH(members)))] AS random2
  FROM ul_fingerprinted
),
--Calculate log averages and statistics per day from random samples
ul_stats_per_day AS (
  SELECT
    date, continent_code, country_code, NUTS_ID, NUTS_NAME, asn,
    COUNT(*) AS ul_samples_day,
    ROUND(POW(10,AVG(Safe.LOG10(random1.mbps))),3) AS ul_LOG_AVG_rnd1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.mbps))),3) AS ul_LOG_AVG_rnd2,
    ROUND(POW(10,AVG(Safe.LOG10(random1.MinRtt))),3) AS ul_minRTT_LOG_AVG_rnd1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.MinRtt))),3) AS ul_minRTT_LOG_AVG_rnd2,
    ROUND(MIN(random1.mbps),3) AS upload_MIN,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(25)],3) AS upload_Q25,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(50)],3) AS upload_MED,
    ROUND(AVG(random1.mbps),3) AS upload_AVG,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(75)],3) AS upload_Q75,
    ROUND(MAX(random1.mbps),3) AS upload_MAX,
    ROUND(APPROX_QUANTILES(random1.MinRTT, 100) [SAFE_ORDINAL(50)],3) AS upload_minRTT_MED,
  FROM ul_random_ip_rows_perday
  GROUP BY date, continent_code, country_code, NUTS_ID, NUTS_NAME, asn
),
--Count the samples that fall into each bucket and get frequencies
ul_histogram AS (
  SELECT
    date,
    continent_code,
    country_code,
    NUTS_ID,
	NUTS_NAME,
    asn,
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
    NUTS_ID,
	NUTS_NAME,
    asn,
    bucket_min,
    bucket_max
),
--Gather final result set
results AS (
  SELECT *, MOD(ABS(FARM_FINGERPRINT(CAST(asn AS STRING))), 1000) AS shard FROM
  dl_histogram
  JOIN ul_histogram USING (date, continent_code, country_code,
  NUTS_ID, NUTS_NAME, asn, bucket_min, bucket_max)
  JOIN dl_stats_per_day USING (date, continent_code, country_code,
  NUTS_ID, NUTS_NAME, asn)
  JOIN ul_stats_per_day USING (date, continent_code, country_code,
  NUTS_ID, NUTS_NAME, asn)
)
--Show the results
SELECT * FROM results
ORDER BY date, continent_code, country_code,
  NUTS_ID, NUTS_NAME, asn, bucket_min, bucket_max