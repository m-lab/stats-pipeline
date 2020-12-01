WITH
# Generate equal sized buckets in log-space between near 0 and 10Gbps
buckets AS (
  SELECT POW(10, x-.2) AS bucket_left, POW(10,x) AS bucket_right
  FROM UNNEST(GENERATE_ARRAY(-5, 4.2, .2)) AS x
),
# Select the initial set of results
dl_per_location AS (
  SELECT
    date,
    client.Geo.ContinentCode AS continent_code,
    client.Geo.CountryCode AS country_code,
    CONCAT(client.Geo.CountryCode, '-', client.Geo.region) AS ISO3166_2region1,
    client.Geo.City AS city,
    client.Network.ASNumber AS asn,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM `measurement-lab.ndt.unified_downloads`
  WHERE date BETWEEN @startdate AND @enddate
  AND a.MeanThroughputMbps != 0
),
# With good locations and valid IPs
dl_per_location_cleaned AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    mbps,
    MinRTT,
    asn,
    ip
  FROM dl_per_location
  WHERE
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND ISO3166_2region1 IS NOT NULL AND ISO3166_2region1 != ""
    AND city IS NOT NULL AND city != ""
    AND ip IS NOT NULL
    AND asn IS NOT NULL
),
# Gather descriptive statistics per geo, day, per ip
dl_stats_perip_perday AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn,
    ip,
    MIN(mbps) AS download_MIN,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS download_Q25,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS download_MED,
    AVG(mbps) AS download_AVG,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS download_Q75,
    MAX(mbps) AS download_MAX,
    APPROX_QUANTILES(MinRTT, 100) [SAFE_ORDINAL(50)] AS download_minRTT_MED
  FROM dl_per_location_cleaned
  GROUP BY date, continent_code, country_code, ISO3166_2region1, city, asn, ip
),
# Calculate final stats per day from 1x test per ip per day normalization in prev. step
dl_stats_per_day AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn,
    MIN(download_MIN) AS download_MIN,
    APPROX_QUANTILES(download_Q25, 100) [SAFE_ORDINAL(25)] AS download_Q25,
    APPROX_QUANTILES(download_MED, 100) [SAFE_ORDINAL(50)] AS download_MED,
    AVG(download_AVG) AS download_AVG,
    APPROX_QUANTILES(download_Q75, 100) [SAFE_ORDINAL(75)] AS download_Q75,
    MAX(download_MAX) AS download_MAX,
    APPROX_QUANTILES(download_minRTT_MED, 100) [SAFE_ORDINAL(50)] AS download_minRTT_MED
  FROM
    dl_stats_perip_perday
  GROUP BY date, continent_code, country_code, ISO3166_2region1, city, asn
),
# Count the difference in the number of tests from the same IPs on the same
#   day, to the number of tests used in the daily statistics.
dl_samples_total AS (
  SELECT
    COUNT(*) AS dl_total_samples,
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn
  FROM dl_per_location_cleaned
  GROUP BY
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn
),
# Count the samples that fall into each bucket and get frequencies
dl_histogram AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(download_MED < bucket_right AND download_MAX >= bucket_left) AS dl_samples_bucket,
    COUNT(*) AS dl_samples_day,
    COUNTIF(download_MED < bucket_right AND download_MAX >= bucket_left) / COUNT(*) AS dl_frac
  FROM dl_stats_perip_perday CROSS JOIN buckets
  GROUP BY
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn,
    bucket_min,
    bucket_max
),
# Repeat for Upload tests
# Select the initial set of upload results
ul_per_location AS (
  SELECT
    date,
    client.Geo.ContinentCode AS continent_code,
    client.Geo.CountryCode AS country_code,
    CONCAT(client.Geo.CountryCode, '-', client.Geo.Region) AS ISO3166_2region1,
    client.Geo.City AS city,
    client.Network.ASNumber AS asn,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM `measurement-lab.ndt.unified_uploads`
  WHERE date BETWEEN @startdate AND @enddate
  AND a.MeanThroughputMbps != 0
),
# With good locations and valid IPs
ul_per_location_cleaned AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn,
    mbps,
    MinRTT,
    ip
  FROM ul_per_location
  WHERE
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND ISO3166_2region1 IS NOT NULL AND ISO3166_2region1 != ""
    AND city IS NOT NULL AND city != ""
    AND ip IS NOT NULL
    AND asn IS NOT NULL
),
# Gather descriptive statistics per geo, day, per ip
ul_stats_perip_perday AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn,
    ip,
    MIN(mbps) AS upload_MIN,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS upload_Q25,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS upload_MED,
    AVG(mbps) AS upload_AVG,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS upload_Q75,
    MAX(mbps) AS upload_MAX,
    APPROX_QUANTILES(MinRTT, 100) [SAFE_ORDINAL(50)] AS upload_minRTT_MED
  FROM ul_per_location_cleaned
  GROUP BY date, continent_code, country_code, ISO3166_2region1, city, asn, ip
),
# Calculate final stats per day from 1x test per ip per day normalization in prev. step
ul_stats_per_day AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn,
    MIN(upload_MIN) AS upload_MIN,
    APPROX_QUANTILES(upload_Q25, 100) [SAFE_ORDINAL(25)] AS upload_Q25,
    APPROX_QUANTILES(upload_MED, 100) [SAFE_ORDINAL(50)] AS upload_MED,
    AVG(upload_AVG) AS upload_AVG,
    APPROX_QUANTILES(upload_Q75, 100) [SAFE_ORDINAL(75)] AS upload_Q75,
    MAX(upload_MAX) AS upload_MAX,
    APPROX_QUANTILES(upload_minRTT_MED, 100) [SAFE_ORDINAL(50)] AS upload_minRTT_MED
  FROM
    ul_stats_perip_perday
  GROUP BY date, continent_code, country_code, ISO3166_2region1, city, asn
),
# Show the total number of samples (all tests from all IPs)
ul_samples_total AS (
  SELECT
    COUNT(*) AS ul_total_samples,
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn
  FROM ul_per_location_cleaned
  GROUP BY
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn
),
# Generate the histogram with samples per bucket and frequencies
ul_histogram AS (
  SELECT
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(upload_MED < bucket_right AND upload_MAX >= bucket_left) AS ul_samples_bucket,
    COUNT(*) AS ul_samples_day,
    COUNTIF(upload_MED < bucket_right AND upload_MAX >= bucket_left) / COUNT(*) AS ul_frac
  FROM ul_stats_perip_perday CROSS JOIN buckets
  GROUP BY
    date,
    continent_code,
    country_code,
    ISO3166_2region1,
    city,
    asn,
    bucket_min,
    bucket_max
)
# Show the results
SELECT *, MOD(ABS(FARM_FINGERPRINT(ISO3166_2region1)), 1000) as shard FROM dl_histogram
JOIN ul_histogram USING (date, continent_code, country_code, ISO3166_2region1, city, asn, bucket_min, bucket_max)
JOIN dl_stats_per_day USING (date, continent_code, country_code, ISO3166_2region1, city, asn)
JOIN ul_stats_per_day USING (date, continent_code, country_code, ISO3166_2region1, city, asn)
JOIN dl_samples_total USING (date, continent_code, country_code, ISO3166_2region1, city, asn)
JOIN ul_samples_total USING (date, continent_code, country_code, ISO3166_2region1, city, asn)
