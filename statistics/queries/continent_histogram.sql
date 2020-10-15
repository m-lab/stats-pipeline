WITH
# Select the initial set of results
dl_per_location AS (
  SELECT
    date,
    client.Geo.continent_code AS continent_code,
    a.MeanThroughputMbps AS mbps,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip
  FROM `measurement-lab.ndt.unified_downloads`
  WHERE date BETWEEN @startdate AND @enddate
  AND a.MeanThroughputMbps != 0
),
# With good locations and valid IPs
dl_per_location_cleaned AS (
  SELECT
    date,
    continent_code,
    mbps,
    ip
  FROM dl_per_location
  WHERE
    continent_code IS NOT NULL AND continent_code != ""
    AND ip IS NOT NULL
),
# Gather descriptive statistics per geo, day, per ip
dl_stats_perip_perday AS (
  SELECT
    date,
    continent_code,
    ip,
    MIN(mbps) AS download_MIN,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS download_Q25,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS download_MED,
    AVG(mbps) AS download_AVG,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS download_Q75,
    MAX(mbps) AS download_MAX
  FROM dl_per_location_cleaned
  GROUP BY date, continent_code, ip
),
# Calculate final stats per day from 1x test per ip per day normalization in prev. step
dl_stats_per_day AS (
  SELECT
    date,
    continent_code,
    MIN(download_MIN) AS download_MIN,
    APPROX_QUANTILES(download_Q25, 100) [SAFE_ORDINAL(25)] AS download_Q25,
    APPROX_QUANTILES(download_MED, 100) [SAFE_ORDINAL(50)] AS download_MED,
    AVG(download_AVG) AS download_AVG,
    APPROX_QUANTILES(download_Q75, 100) [SAFE_ORDINAL(75)] AS download_Q75,
    MAX(download_MAX) AS download_MAX
  FROM
    dl_stats_perip_perday
  GROUP BY date, continent_code
),
dl_total_samples_per_geo AS (
  SELECT
    date,
    COUNT(*) AS dl_total_samples,
    continent_code
  FROM dl_per_location_cleaned
  GROUP BY date, continent_code
),
# Now generate daily histograms of Max DL
max_dl_per_day_ip AS (
  SELECT
    date,
    continent_code,
    ip,
    MAX(mbps) AS mbps
  FROM dl_per_location_cleaned
  GROUP BY
    date,
    continent_code,
    ip
),
# Count the samples
dl_sample_counts AS (
  SELECT
    date,
    continent_code,
    COUNT(*) AS samples
  FROM max_dl_per_day_ip
  GROUP BY
    date,
    continent_code
),
# Generate equal sized buckets in log-space
buckets AS (
  SELECT POW(10, x-.2) AS bucket_left, POW(10,x) AS bucket_right
  FROM UNNEST(GENERATE_ARRAY(-5, 4.2, .2)) AS x
),
# Count the samples that fall into each bucket
dl_histogram_counts AS (
  SELECT
    date,
    continent_code,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(mbps < bucket_right AND mbps >= bucket_left) AS bucket_count
  FROM max_dl_per_day_ip CROSS JOIN buckets
  GROUP BY
    date,
    continent_code,
    bucket_min,
    bucket_max
),
# Turn the counts into frequencies
dl_histogram AS (
  SELECT
    date,
    continent_code,
    bucket_min,
    bucket_max,
    bucket_count / samples AS dl_frac,
    samples AS dl_samples
  FROM dl_histogram_counts
  JOIN dl_sample_counts USING (date, continent_code)
),
# Repeat for Upload tests
# Select the initial set of upload results
ul_per_location AS (
  SELECT
    date,
    client.Geo.continent_code AS continent_code,
    a.MeanThroughputMbps AS mbps,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip
  FROM `measurement-lab.ndt.unified_uploads`
  WHERE date BETWEEN @startdate AND @enddate
  AND a.MeanThroughputMbps != 0
),
# With good locations and valid IPs
ul_per_location_cleaned AS (
  SELECT
    date,
    continent_code,
    mbps,
    ip
  FROM ul_per_location
  WHERE
    continent_code IS NOT NULL AND continent_code != ""
    AND ip IS NOT NULL
),
# Gather descriptive statistics per geo, day, per ip
ul_stats_perip_perday AS (
  SELECT
    date,
    continent_code,
    ip,
    MIN(mbps) AS upload_MIN,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS upload_Q25,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS upload_MED,
    AVG(mbps) AS upload_AVG,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS upload_Q75,
    MAX(mbps) AS upload_MAX
  FROM ul_per_location_cleaned
  GROUP BY date, continent_code, ip
),
# Calculate final stats per day from 1x test per ip per day normalization in prev. step
ul_stats_per_day AS (
  SELECT
    date,
    continent_code,
    MIN(upload_MIN) AS upload_MIN,
    APPROX_QUANTILES(upload_Q25, 100) [SAFE_ORDINAL(25)] AS upload_Q25,
    APPROX_QUANTILES(upload_MED, 100) [SAFE_ORDINAL(50)] AS upload_MED,
    AVG(upload_AVG) AS upload_AVG,
    APPROX_QUANTILES(upload_Q75, 100) [SAFE_ORDINAL(75)] AS upload_Q75,
    MAX(upload_MAX) AS upload_MAX
  FROM
    ul_stats_perip_perday
  GROUP BY date, continent_code
),
ul_total_samples_per_geo AS (
  SELECT
    date,
    COUNT(*) AS ul_total_samples,
    continent_code,
  FROM ul_per_location_cleaned
  GROUP BY date, continent_code
),
# Now generate daily histograms of Max UL
max_ul_per_day_ip AS (
  SELECT
    date,
    continent_code,
    ip,
    MAX(mbps) AS mbps
  FROM ul_per_location_cleaned
  GROUP BY
    date,
    continent_code,
    ip
),
# Count the samples
ul_sample_counts AS (
  SELECT
    date,
    continent_code,
    COUNT(*) AS samples
  FROM max_ul_per_day_ip
  GROUP BY
    date,
    continent_code
),
# Count the samples that fall into each bucket
ul_histogram_counts AS (
  SELECT
    date,
    continent_code,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(mbps < bucket_right AND mbps >= bucket_left) AS bucket_count
  FROM max_ul_per_day_ip CROSS JOIN buckets
  GROUP BY
    date,
    continent_code,
    bucket_min,
    bucket_max
),
# Turn the counts into frequencies
ul_histogram AS (
  SELECT
    date,
    continent_code,
    bucket_min,
    bucket_max,
    bucket_count / samples AS ul_frac,
    samples AS ul_samples
  FROM ul_histogram_counts
  JOIN ul_sample_counts USING (date, continent_code)
)
# Show the results
SELECT * FROM dl_histogram
JOIN ul_histogram USING (date, continent_code, bucket_min, bucket_max)
JOIN dl_stats_per_day USING (date, continent_code)
JOIN dl_total_samples_per_geo USING (date, continent_code)
JOIN ul_stats_per_day USING (date, continent_code)
JOIN ul_total_samples_per_geo USING (date, continent_code)
