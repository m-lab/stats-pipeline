#standardSQL
WITH
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
dl_total_samples_pergeo_perday AS (
  SELECT
    date,
    COUNT(*) AS dl_total_samples,
    state,
    GEOID
  FROM dl
  GROUP BY date, state, GEOID
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
ul_total_samples_pergeo_perday AS (
  SELECT
    date,
    COUNT(*) AS ul_total_samples,
    state,
    GEOID
  FROM ul
  GROUP BY date, state, GEOID
),
# Now generate the daily histograms of the Maximum measured download speed, per IP, per day.

# First, select the MAX download metric and geo fields from the original cleaned data.
max_dl_per_day_ip AS (
  SELECT
    date,
    state,
    GEOID,
    ip,
    MAX(mbps) AS download_MAX
  FROM dl
  GROUP BY
    date,
    state,
    GEOID,
    ip
),
# Count the samples for the daily histogram of Max dowload tests.
#   The counts here are drawn from: dl_stats_perip_perday > max_dl_per_day_ip
#   and therefore represent the **one** MAX download value per IP on that day.
#
#   This count is different from "dl_total_samples_pergeo_perday" because the
#   histogram is meant to communicate the number of **testers** who could or could
#   not reach specific bucket thresholds, while dl_total_samples_pergeo_perday
#   is the count of **all tests** from all IPs in the sample on that day.
dl_sample_counts AS (
  SELECT
    date,
    state,
    GEOID,
    COUNT(*) AS samples
  FROM max_dl_per_day_ip
  GROUP BY
    date,
    state,
    GEOID
),
# Generate equal sized buckets in log-space. This returns 21 buckets pergeo perday from 0.63 to 10000.
# Five steps per logarithmic decade, from 0.63 to 10000, i.e. 0.63, 1.0, 1.58, 2.51, 3.98, 6.30, 10.0, ...

buckets AS (
  SELECT POW(10, x-.2) AS bucket_left, POW(10,x) AS bucket_right
  FROM UNNEST(GENERATE_ARRAY(-5, 4.2, .2)) AS x
),
# Count the samples that fall into each bucket
dl_histogram_counts AS (
  SELECT
    date,
    state,
    GEOID,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(download_MAX BETWEEN bucket_left AND bucket_right) AS bucket_count
  FROM max_dl_per_day_ip CROSS JOIN buckets
  GROUP BY
    date,
    state,
    GEOID,
    bucket_min,
    bucket_max
),
# Turn the counts into frequencies
dl_histogram AS (
  SELECT
    date,
    state,
    GEOID,
    bucket_min,
    bucket_max,
    bucket_count / samples AS dl_frac,
    samples AS dl_samples
  FROM dl_histogram_counts
  JOIN dl_sample_counts USING (date, state, GEOID)
),
# Generate histogram for uploads
max_ul_per_day_ip AS (
  SELECT
    date,
    state,
    GEOID,
    ip,
    MAX(mbps) AS upload_MAX
  FROM ul
  GROUP BY
    date,
    state,
    GEOID,
    ip
),
# Count the samples
ul_sample_counts AS (
  SELECT
    date,
    state,
    GEOID,
    COUNT(*) AS samples
  FROM max_ul_per_day_ip
  GROUP BY
    date,
    state,
    GEOID
),
# Count the samples that fall into each bucket
ul_histogram_counts AS (
  SELECT
    date,
    state,
    GEOID,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(upload_MAX BETWEEN bucket_left AND bucket_right) AS bucket_count
  FROM max_ul_per_day_ip CROSS JOIN buckets
  GROUP BY
    date,
    state,
    GEOID,
    bucket_min,
    bucket_max
),
# Turn the counts into frequencies
ul_histogram AS (
  SELECT
    date,
    state,
    GEOID,
    bucket_min,
    bucket_max,
    bucket_count / samples AS ul_frac,
    samples AS ul_samples
  FROM ul_histogram_counts
  JOIN ul_sample_counts USING (date, state, GEOID)
)
# Show the results
SELECT * FROM dl_histogram
JOIN ul_histogram USING (date, state, GEOID, bucket_min, bucket_max)
JOIN dl_stats_perday USING (date, state, GEOID)
JOIN dl_total_samples_pergeo_perday USING (date, state, GEOID)
JOIN ul_stats_perday USING (date, state, GEOID)
JOIN ul_total_samples_pergeo_perday USING (date, state, GEOID)
JOIN counties_noWKT USING (GEOID)
