WITH
# Select the initial set of results
dl_per_location AS (
  SELECT
    date,
    client.Geo.continent_code AS continent_code,
    client.Geo.country_code AS country_code,
    client.Geo.country_name AS country_name,
    CONCAT(client.Geo.country_code, '-', client.Geo.region) AS ISO3166_2region1,
    client.Geo.city AS city,
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
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    mbps,
    ip
  FROM dl_per_location
  WHERE
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND country_name IS NOT NULL AND country_name != ""
    AND ISO3166_2region1 IS NOT NULL AND ISO3166_2region1 != ""
    AND city IS NOT NULL AND city != ""
    AND ip IS NOT NULL
),
# Gather descriptive statistics per geo, day, per ip
dl_stats_perip_perday AS (
  SELECT
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    ip,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps
  FROM dl_per_location_cleaned
  GROUP BY date, continent_code, country_code, country_name, ISO3166_2region1, city, ip
),
# Calculate final stats per day from 1x test per ip per day normalization in prev. step
dl_stats_per_day AS (
  SELECT
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    MIN(MIN_download_Mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(LOWER_QUART_download_Mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(MED_download_Mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(MEAN_download_Mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(UPPER_QUART_download_Mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(MAX_download_Mbps) AS MAX_download_Mbps
  FROM
    dl_stats_perip_perday
  GROUP BY date, continent_code, country_code, country_name, ISO3166_2region1, city
),
dl_total_samples_per_geo AS (
  SELECT
    date,
    COUNT(*) AS dl_total_samples,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city
  FROM dl_per_location_cleaned
  GROUP BY date, continent_code, country_code, country_name, ISO3166_2region1, city
),
# Now generate daily histograms of Max DL Maximum measured value per IP, per day
max_dl_per_day_ip AS (
  SELECT
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    ip,
    MAX(mbps) AS mbps
  FROM dl_per_location_cleaned
  GROUP BY
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    ip
),
# Count the samples
dl_sample_counts AS (
  SELECT
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    COUNT(*) AS samples
  FROM max_dl_per_day_ip
  GROUP BY
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city
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
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(mbps < bucket_right AND mbps >= bucket_left) AS bucket_count
  FROM max_dl_per_day_ip CROSS JOIN buckets
  GROUP BY
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    bucket_min,
    bucket_max
),
# Turn the counts into frequencies
dl_histogram AS (
  SELECT
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    bucket_min,
    bucket_max,
    bucket_count / samples AS dl_frac,
    samples AS dl_samples
  FROM dl_histogram_counts
  JOIN dl_sample_counts USING (date, continent_code, country_code,
                            country_name, ISO3166_2region1, city)
),
# Repeat for Upload tests
# Select the initial set of upload results
ul_per_location AS (
  SELECT
    date,
    client.Geo.continent_code AS continent_code,
    client.Geo.country_code AS country_code,
    client.Geo.country_name AS country_name,
    CONCAT(client.Geo.country_code, '-', client.Geo.region) AS ISO3166_2region1,
    client.Geo.city AS city,
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
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    mbps,
    ip
  FROM ul_per_location
  WHERE
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND country_name IS NOT NULL AND country_name != ""
    AND ISO3166_2region1 IS NOT NULL AND ISO3166_2region1 != ""
    AND city IS NOT NULL AND city != ""
    AND ip IS NOT NULL
),
# Gather descriptive statistics per geo, day, per ip
ul_stats_perip_perday AS (
  SELECT
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    ip,
    MIN(mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(mbps) AS MAX_upload_Mbps
  FROM dl_per_location_cleaned
  GROUP BY date, continent_code, country_code, country_name, ISO3166_2region1, city, ip
),
# Calculate final stats per day from 1x test per ip per day normalization in prev. step
ul_stats_per_day AS (
  SELECT
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    MIN(MIN_upload_Mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(LOWER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(MED_upload_Mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(MEAN_upload_Mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(UPPER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(MAX_upload_Mbps) AS MAX_upload_Mbps
  FROM
    ul_stats_perip_perday
  GROUP BY date, continent_code, country_code, country_name, ISO3166_2region1, city
),
ul_total_samples_per_geo AS (
  SELECT
    date,
    COUNT(*) AS ul_total_samples,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city
  FROM ul_per_location_cleaned
  GROUP BY date, continent_code, country_code, country_name, ISO3166_2region1, city
),
# Now generate daily histograms of Max DL Maximum measured value per IP, per day
max_ul_per_day_ip AS (
  SELECT
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    ip,
    MAX(mbps) AS mbps
  FROM ul_per_location_cleaned
  GROUP BY
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    ip
),
# Count the samples
ul_sample_counts AS (
  SELECT
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    COUNT(*) AS samples
  FROM max_ul_per_day_ip
  GROUP BY
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city
),
# Count the samples that fall into each bucket
ul_histogram_counts AS (
  SELECT
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(mbps < bucket_right AND mbps >= bucket_left) AS bucket_count
  FROM max_ul_per_day_ip CROSS JOIN buckets
  GROUP BY
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    bucket_min,
    bucket_max
),
# Turn the counts into frequencies
ul_histogram AS (
  SELECT
    date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    city,
    bucket_min,
    bucket_max,
    bucket_count / samples AS ul_frac,
    samples AS ul_samples
  FROM ul_histogram_counts
  JOIN ul_sample_counts USING (date, continent_code, country_code,
                            country_name, ISO3166_2region1, city)
)

# Show the results
SELECT * FROM dl_histogram
JOIN ul_histogram USING (date, continent_code, country_code, country_name, ISO3166_2region1, city, bucket_min, bucket_max)
JOIN dl_stats_per_day USING (date, continent_code, country_code, country_name, ISO3166_2region1, city)
JOIN dl_total_samples_per_geo USING (date, continent_code, country_code, country_name, ISO3166_2region1, city)
JOIN ul_stats_per_day USING (date, continent_code, country_code, country_name, ISO3166_2region1, city)
JOIN ul_total_samples_per_geo USING (date, continent_code, country_code, country_name, ISO3166_2region1, city)
ORDER BY date, continent_code, country_code, country_name, ISO3166_2region1, city, bucket_min, bucket_max
