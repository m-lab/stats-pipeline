#standardSQL
WITH
# Generate equal sized buckets in log-space between near 0 and 10Gbps
buckets AS (
  SELECT POW(10, x-.2) AS bucket_left, POW(10,x) AS bucket_right
  FROM UNNEST(GENERATE_ARRAY(-5, 4.2, .2)) AS x
),
counties AS (
  SELECT
    county_name,
    county_geom AS WKT,
    CAST(geo_id AS STRING) AS GEOID
  FROM
    `bigquery-public-data.geo_us_boundaries.counties`
),
counties_noWKT AS (
  SELECT
    county_name,
    CAST(geo_id AS STRING) AS GEOID
  FROM
    `bigquery-public-data.geo_us_boundaries.counties`
),
dl AS (
  SELECT
    date,
    counties.GEOID AS GEOID,
    CONCAT(client.Geo.country_code,"-",client.Geo.region) AS state,
    NET.SAFE_IP_FROM_STRING(client.IP) AS ip,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM
    `measurement-lab.ndt.unified_downloads` tests, counties
  WHERE
    date BETWEEN @startdate AND @enddate
    AND client.Geo.country_code = "US"
    AND client.Geo.region IS NOT NULL
    AND client.Geo.region != ""
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), counties.WKT
    )
    AND a.MeanThroughputMbps != 0
),
dl_stats_perip_perday AS (
  SELECT
    date,
    state,
    GEOID,
    ip,
    MIN(mbps) AS download_MIN,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS download_Q25,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS download_MED,
    AVG(mbps) AS download_AVG,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS download_Q75,
    MAX(mbps) AS download_MAX
  FROM dl
  GROUP BY date, state, GEOID, ip
),
dl_stats_perday AS (
  SELECT
    date,
    state,
    GEOID,
    MIN(download_MIN) AS download_MIN,
    APPROX_QUANTILES(download_Q25, 100) [SAFE_ORDINAL(25)] AS download_Q25,
    APPROX_QUANTILES(download_MED, 100) [SAFE_ORDINAL(50)] AS download_MED,
    AVG(download_AVG) AS download_AVG,
    APPROX_QUANTILES(download_Q75, 100) [SAFE_ORDINAL(75)] AS download_Q75,
    MAX(download_MAX) AS download_MAX
  FROM dl_stats_perip_perday
  GROUP BY date, state, GEOID
),
# Count the difference in the number of tests from the same IPs on the same
#   day, to the number of tests used in the daily statistics.
dl_samples_total AS (
  SELECT
    date,
    COUNT(*) AS dl_total_samples,
    state,
    GEOID
  FROM dl
  GROUP BY
    date,
    state,
    GEOID
),
dl_samples_stats AS (
  SELECT
    COUNT(*) AS dl_stats_samples,
    date,
    state,
    GEOID
  FROM dl_stats_perip_perday
  GROUP BY
    date,
    state,
    GEOID
),
# Count the samples that fall into each bucket and get frequencies
dl_histogram AS (
  SELECT
    date,
    state,
    GEOID,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(download_MAX < bucket_right AND download_MAX >= bucket_left) AS dl_samples_bucket,
    COUNT(*) AS dl_samples_day,
    COUNTIF(download_MAX < bucket_right AND download_MAX >= bucket_left) / COUNT(*) AS dl_frac
  FROM dl_stats_perip_perday CROSS JOIN buckets
  GROUP BY
    date,
    state,
    GEOID,
    bucket_min,
    bucket_max
),
#############
ul AS (
  SELECT
    date,
    counties.GEOID AS GEOID,
    CONCAT(client.Geo.country_code,"-",client.Geo.region) AS state,
    NET.SAFE_IP_FROM_STRING(client.IP) AS ip,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM
    `measurement-lab.ndt.unified_uploads` tests, counties
  WHERE
    date BETWEEN @startdate AND @enddate
    AND client.Geo.country_code = "US"
    AND client.Geo.region IS NOT NULL
    AND client.Geo.region != ""
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), counties.WKT
    )
    AND a.MeanThroughputMbps != 0
),
ul_stats_perip_perday AS (
  SELECT
    date,
    state,
    GEOID,
    ip,
    MIN(mbps) AS upload_MIN,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS upload_Q25,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS upload_MED,
    AVG(mbps) AS upload_AVG,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS upload_Q75,
    MAX(mbps) AS upload_MAX
  FROM ul
  GROUP BY date, state, GEOID, ip
),
ul_stats_perday AS (
  SELECT
    date,
    state,
    GEOID,
    MIN(upload_MIN) AS upload_MIN,
    APPROX_QUANTILES(upload_Q25, 100) [SAFE_ORDINAL(25)] AS upload_Q25,
    APPROX_QUANTILES(upload_MED, 100) [SAFE_ORDINAL(50)] AS upload_MED,
    AVG(upload_AVG) AS upload_AVG,
    APPROX_QUANTILES(upload_Q75, 100) [SAFE_ORDINAL(75)] AS upload_Q75,
    MAX(upload_MAX) AS upload_MAX
  FROM ul_stats_perip_perday
  GROUP BY date, state, GEOID
),
ul_samples_total AS (
  SELECT
    date,
    COUNT(*) AS ul_total_samples,
    state,
    GEOID
  FROM ul
  GROUP BY
    date,
    state,
    GEOID
),
ul_samples_stats AS (
  SELECT
    COUNT(*) AS ul_stats_samples,
    date,
    state,
    GEOID
  FROM ul_stats_perip_perday
  GROUP BY
    date,
    state,
    GEOID
),
# Generate the histogram with samples per bucket and frequencies
ul_histogram AS (
  SELECT
    date,
    state,
    GEOID,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(upload_MAX < bucket_right AND upload_MAX >= bucket_left) AS ul_samples_bucket,
    COUNT(*) AS ul_samples_day,
    COUNTIF(upload_MAX < bucket_right AND upload_MAX >= bucket_left) / COUNT(*) AS ul_frac
  FROM ul_stats_perip_perday CROSS JOIN buckets
  GROUP BY
    date,
    state,
    GEOID,
    bucket_min,
    bucket_max
)
# Show the results
SELECT *, MOD(ABS(FARM_FINGERPRINT(GEOID)), 4000) as shard FROM dl_histogram
JOIN ul_histogram USING (date, state, GEOID, bucket_min, bucket_max)
JOIN dl_stats_perday USING (date, state, GEOID)
JOIN ul_stats_perday USING (date, state, GEOID)
JOIN counties_noWKT USING (GEOID)
JOIN dl_samples_total USING (date, state, GEOID)
JOIN ul_samples_total USING (date, state, GEOID)
JOIN dl_samples_stats USING (date, state, GEOID)
JOIN ul_samples_stats USING (date, state, GEOID)