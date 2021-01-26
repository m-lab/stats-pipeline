WITH
# Generate equal sized buckets in log-space between near 0 Mbps and ~1 Gbps+
buckets AS (
  SELECT POW(10, x-.25) AS bucket_left, POW(10,x+.25) AS bucket_right
  FROM UNNEST(GENERATE_ARRAY(0, 3.5, .5)) AS x
),
# Select the initial set of results
dl_per_location AS (
  SELECT
    date,
    client.Geo.ContinentCode AS continent_code,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    id,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM `measurement-lab.ndt.unified_downloads`
  WHERE date BETWEEN @startdate AND @enddate
  AND a.MeanThroughputMbps != 0
),
# With good locations and valid IPs
dl_per_location_cleaned AS (
  SELECT * FROM dl_per_location
  WHERE
    continent_code IS NOT NULL
    AND continent_code != ""
    AND ip IS NOT NULL
),

# Gather rows per day and ip
dl_rows_perip_perday AS (
  SELECT
    date,
    continent_code,
    ip,
    # This organizes all tests, in an arbitrary but repeatable order.
    ARRAY_AGG(STRUCT(ABS(FARM_FINGERPRINT(id)) AS ffid, mbps, MinRTT) ORDER BY ABS(FARM_FINGERPRINT(id))) AS members,
    STRUCT(
      ROUND(MIN(mbps),3) AS download_MIN,
      ROUND(APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)],3) AS download_Q25,
      ROUND(APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)],3) AS download_MED,
      ROUND(POW(10,AVG(Safe.LOG10(mbps))),3) AS download_LOG_AVG,
      ROUND(AVG(mbps),3) AS download_AVG,
      ROUND(APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)],3) AS download_Q75,
      ROUND(MAX(mbps),3) AS download_MAX,
      ROUND(APPROX_QUANTILES(MinRTT, 100) [SAFE_ORDINAL(50)],3) AS download_minRTT_MED
    ) AS stats
  FROM dl_per_location_cleaned
  GROUP BY date, continent_code, ip
  --ORDER BY ip, date
),

dl_random_perip_perday AS (
  SELECT
    date,
    continent_code,
    ip,
    ARRAY_LENGTH(members) AS tests,
    # Use a prime number larger than the number of total rows in an aggregation to select a random row 
    members[SAFE_OFFSET(MOD(511232941,ARRAY_LENGTH(members)))] AS random1,
    members[SAFE_OFFSET(MOD(906686609,ARRAY_LENGTH(members)))] AS random2,
    stats
  FROM dl_rows_perip_perday
),

# Calculate log average per day from random samples
dl_stats_per_day AS (
  SELECT
    date, continent_code,
    ROUND(POW(10,AVG(Safe.LOG10(stats.download_MED))),3) AS dl_day_log_avg_median,
    ROUND(POW(10,AVG(Safe.LOG10(random1.MinRtt))),3) AS dl_min_rtt_day_log_avg,
    ROUND(POW(10,AVG(Safe.LOG10(random1.mbps))),3) AS dl_day_log_avg_random1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.mbps))),3) AS dl_day_log_avg_random2
  FROM dl_random_perip_perday
  GROUP BY continent_code, date
  # ORDER BY continent_code, date
),
# Count the samples that fall into each bucket and get frequencies
dl_histogram AS (
  SELECT
    date,
    continent_code,
    CASE WHEN bucket_left = 0.31622776601683794 THEN 0
    ELSE bucket_left END AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(stats.download_MED < bucket_right AND stats.download_MED >= bucket_left) AS dl_samples_bucket,
    ROUND(COUNTIF(stats.download_MED < bucket_right AND stats.download_MED >= bucket_left) / COUNT(*), 3) AS dl_frac_bucket
  FROM dl_random_perip_perday CROSS JOIN buckets
  GROUP BY
    date,
    continent_code,
    bucket_min,
    bucket_max
),
# Repeat for Upload tests
# Select the initial set of upload results
ul_per_location AS (
  SELECT
    date,
    client.Geo.ContinentCode AS continent_code,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    id,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM `measurement-lab.ndt.unified_uploads`
  WHERE date BETWEEN @startdate AND @enddate
  AND a.MeanThroughputMbps != 0
),
# With good locations and valid IPs
ul_per_location_cleaned AS (
  SELECT * FROM ul_per_location
  WHERE
    continent_code IS NOT NULL AND continent_code != ""
    AND ip IS NOT NULL
),

# Gather rows per day and ip
ul_rows_perip_perday AS (
  SELECT
    date,
    continent_code,
    ip,
    # This organizes all tests, in an arbitrary but repeatable order.
    ARRAY_AGG(STRUCT(ABS(FARM_FINGERPRINT(id)) AS ffid, mbps, MinRTT) ORDER BY ABS(FARM_FINGERPRINT(id))) AS members,
    STRUCT(
      ROUND(MIN(mbps),3) AS upload_MIN,
      ROUND(APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)],3) AS upload_Q25,
      ROUND(APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)],3) AS upload_MED,
      ROUND(POW(10,AVG(Safe.LOG10(mbps))),3) AS upload_LOG_AVG,
      ROUND(AVG(mbps),3) AS upload_AVG,
      ROUND(MAX(mbps),3) AS upload_MAX,
      ROUND(APPROX_QUANTILES(MinRTT, 100) [SAFE_ORDINAL(50)],3) AS upload_minRTT_MED
    ) AS stats
  FROM ul_per_location_cleaned
  GROUP BY date, continent_code, ip
  --ORDER BY ip, date
),

ul_random_perip_perday AS (
  SELECT
    date,
    continent_code,
    ip,
    ARRAY_LENGTH(members) AS tests,
    # Use a prime number larger than the number of total rows in an aggregation to select a random row 
    members[SAFE_OFFSET(MOD(511232941,ARRAY_LENGTH(members)))] AS random1,
    members[SAFE_OFFSET(MOD(906686609,ARRAY_LENGTH(members)))] AS random2,
    stats
  FROM ul_rows_perip_perday
),
# Calculate log average per day from random samples
ul_stats_per_day AS (
  SELECT
    continent_code, date, 
    ROUND(POW(10,AVG(Safe.LOG10(stats.upload_MED))),3) AS ul_day_log_avg_median,
    ROUND(POW(10,AVG(Safe.LOG10(random1.MinRtt))),3) AS ul_min_rtt_day_log_avg,
    ROUND(POW(10,AVG(Safe.LOG10(random1.mbps))),3) AS ul_day_log_avg_random1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.mbps))),3) AS ul_day_log_avg_random2,
  FROM ul_random_perip_perday
  GROUP BY continent_code, date
  # ORDER BY continent_code, date
),
# Count the samples that fall into each bucket and get frequencies
ul_histogram AS (
  SELECT
    date,
    continent_code,
    CASE WHEN bucket_left = 0.31622776601683794 THEN 0
    ELSE bucket_left END AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(stats.upload_MED < bucket_right AND stats.upload_MED >= bucket_left) AS ul_samples_bucket,
    ROUND(COUNTIF(stats.upload_MED < bucket_right AND stats.upload_MED >= bucket_left) / COUNT(*),3) AS ul_frac_bucket
  FROM ul_random_perip_perday CROSS JOIN buckets
  GROUP BY
    date,
    continent_code,
    bucket_min,
    bucket_max
)
# Show the results
SELECT *, MOD(ABS(FARM_FINGERPRINT(continent_code)), 1000) as shard FROM dl_histogram
JOIN ul_histogram USING (date, continent_code, bucket_min, bucket_max)
JOIN dl_stats_per_day USING (date, continent_code)
JOIN ul_stats_per_day USING (date, continent_code)
ORDER BY date, continent_code, bucket_min
