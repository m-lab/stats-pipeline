WITH
# Select the initial set of download results
dl_per_location AS (
  SELECT
    test_date,
    client.Geo.continent_code AS continent_code,
    client.Geo.country_code AS country_code,
    client.Geo.country_name AS country_name,
    CONCAT(client.Geo.country_code, '-', client.Geo.region) AS ISO3166_2region1,
    client.Network.ASNumber AS ASNumber,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    a.MeanThroughputMbps AS mbps
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
    ASNumber,
    ip,
    mbps
  FROM dl_per_location
  WHERE 
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND country_name IS NOT NULL AND country_name != ""
    AND ISO3166_2region1 IS NOT NULL AND ISO3166_2region1 != ""
    AND ip IS NOT NULL
    AND ASNumber IS NOT NULL AND ASNumber != ""
),
dl_samples_per_as_per_day AS (
  SELECT
    COUNT(*) AS dl_tests_per_as_day, 
    COUNT(DISTINCT(ip)) AS dl_ips_per_as_day,
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,  
    ASNumber
  FROM dl_per_location_cleaned
  GROUP BY 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,  
    ASNumber
),
# Gather statistics per geo, day, ASN, ip
dl_stats_per_day_per_as_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    ip,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps,
  FROM dl_per_location_cleaned
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
dl_stats_per_day_per_as AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_download_Mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(LOWER_QUART_download_Mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(MED_download_Mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(MEAN_download_Mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(UPPER_QUART_download_Mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(MAX_download_Mbps) AS MAX_download_Mbps,
    COUNT(*) AS total_dl_tests_per_as_ip, 
    COUNT(DISTINCT(ip)) AS total_dl_ips_per_as
  FROM dl_stats_per_day_per_as_ip
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber
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
    client.Network.ASNumber AS ASNumber,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    a.MeanThroughputMbps AS mbps
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
    ASNumber,
    ip,
    mbps
  FROM ul_per_location
  WHERE 
    continent_code IS NOT NULL AND continent_code != ""
    AND country_code IS NOT NULL AND country_code != ""
    AND country_name IS NOT NULL AND country_name != ""
    AND ISO3166_2region1 IS NOT NULL AND ISO3166_2region1 != ""
    AND ip IS NOT NULL
    AND ASNumber IS NOT NULL AND ASNumber != ""
),
ul_samples_per_as_per_day AS (
  SELECT
    COUNT(*) AS ul_tests_per_as_day, 
    COUNT(DISTINCT(ip)) AS ul_ips_per_as_day,
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,  
    ASNumber
  FROM ul_per_location_cleaned
  GROUP BY 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber
),
# Gather statistics per geo, day, ASN, ip
ul_stats_per_day_per_as_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    ip,
    MIN(mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(mbps) AS MAX_upload_Mbps,
  FROM ul_per_location_cleaned
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
ul_stats_per_day_per_as AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_upload_Mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(LOWER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(MED_upload_Mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(MEAN_upload_Mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(UPPER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(MAX_upload_Mbps) AS MAX_upload_Mbps,
    COUNT(*) AS total_ul_tests_per_as_ip, 
    COUNT(DISTINCT(ip)) AS total_ul_ips_per_as
  FROM ul_stats_per_day_per_as_ip
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
## Histograms ##
# Now generate daily histograms of Max DL Maximum measured value per IP, per day 
max_dl_per_day_ip AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    ip,
    MAX(mbps) AS mbps
  FROM dl_per_location_cleaned
  GROUP BY 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    ip
),
# Count the samples
dl_sample_counts AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    COUNT(*) AS samples
  FROM max_dl_per_day_ip
  GROUP BY
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber 
),
# Generate equal sized buckets in log-space
buckets AS (
  SELECT POW(10,x) AS bucket_right, POW(10, x-.2) AS bucket_left
  FROM UNNEST(GENERATE_ARRAY(0, 4.2, .2)) AS x
),
# Count the samples that fall into each bucket
dl_histogram_counts AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(mbps < bucket_right AND mbps >= bucket_left) AS bucket_count
  FROM max_dl_per_day_ip CROSS JOIN buckets
  GROUP BY 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    bucket_min,
    bucket_max
),
# Turn the counts into frequencies
dl_histogram AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    bucket_min,
    bucket_max,
    bucket_count / samples AS dl_frac,
    samples AS dl_samples
  FROM dl_histogram_counts 
  JOIN dl_sample_counts USING (test_date, continent_code, country_code,
                            country_name, ISO3166_2region1, ASNumber)
),
# Repeat for Upload tests
max_ul_per_day_ip AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    ip,
    MAX(mbps) AS mbps
  FROM ul_per_location_cleaned
  GROUP BY 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    ip
),
# Count the samples
ul_sample_counts AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    COUNT(*) AS samples
  FROM max_ul_per_day_ip
  GROUP BY
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber
),
# Count the samples that fall into each bucket
ul_histogram_counts AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(mbps < bucket_right AND mbps >= bucket_left) AS bucket_count
  FROM max_ul_per_day_ip CROSS JOIN buckets
  GROUP BY 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    bucket_min,
    bucket_max
),
# Turn the counts into frequencies
ul_histogram AS (
  SELECT 
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    bucket_min,
    bucket_max,
    bucket_count / samples AS ul_frac,
    samples AS ul_samples
  FROM ul_histogram_counts 
  JOIN ul_sample_counts USING (test_date, continent_code, country_code,
                            country_name, ISO3166_2region1, ASNumber)
),
## Rolling stats (past x days from each day)
last7d_down AS (
  SELECT * FROM dl_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 7 DAY)
),
last7d_down_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps
  FROM last7d_down
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d7_download AS (
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_download_Mbps) AS d7_MIN_download_Mbps,
    APPROX_QUANTILES(LOWER_QUART_download_Mbps, 100) [SAFE_ORDINAL(25)] AS d7_LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(MED_download_Mbps, 100) [SAFE_ORDINAL(50)] AS d7_MED_download_Mbps,
    AVG(MEAN_download_Mbps) AS d7_MEAN_download_Mbps,
    APPROX_QUANTILES(UPPER_QUART_download_Mbps, 100) [SAFE_ORDINAL(75)] AS d7_UPPER_QUART_download_Mbps,
    MAX(MAX_download_Mbps) AS d7_MAX_download_Mbps,
    STDDEV(MAX_download_Mbps) AS d7_STDEV_max_dl
  FROM last7d_down_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
last7d_up AS (
  SELECT * FROM ul_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 7 DAY)
),
last7d_up_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(mbps) AS MAX_upload_Mbps
  FROM last7d_up
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d7_upload AS (
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_upload_Mbps) AS d7_MIN_upload_Mbps,
    APPROX_QUANTILES(LOWER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(25)] AS d7_LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(MED_upload_Mbps, 100) [SAFE_ORDINAL(50)] AS d7_MED_upload_Mbps,
    AVG(MEAN_upload_Mbps) AS d7_MEAN_upload_Mbps,
    APPROX_QUANTILES(UPPER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(75)] AS d7_UPPER_QUART_upload_Mbps,
    MAX(MAX_upload_Mbps) AS d7_MAX_upload_Mbps,
    STDDEV(MAX_upload_Mbps) AS d7_STDEV_max_ul
  FROM last7d_up_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
last30d_down AS (
  SELECT * FROM dl_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 30 DAY)
),
last30d_down_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps
  FROM last30d_down
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d30_download AS (
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_download_Mbps) AS d30_MIN_download_Mbps,
    APPROX_QUANTILES(LOWER_QUART_download_Mbps, 100) [SAFE_ORDINAL(25)] AS d30_LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(MED_download_Mbps, 100) [SAFE_ORDINAL(50)] AS d30_MED_download_Mbps,
    AVG(MEAN_download_Mbps) AS d30_MEAN_download_Mbps,
    APPROX_QUANTILES(UPPER_QUART_download_Mbps, 100) [SAFE_ORDINAL(75)] AS d30_UPPER_QUART_download_Mbps,
    MAX(MAX_download_Mbps) AS d30_MAX_download_Mbps,
    STDDEV(MAX_download_Mbps) AS d30_STDEV_max_dl
  FROM last30d_down_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
last30d_up AS (
  SELECT * FROM ul_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 30 DAY)
),
last30d_up_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(mbps) AS MAX_upload_Mbps
  FROM last30d_up
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d30_upload AS ( 
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_upload_Mbps) AS d30_MIN_upload_Mbps,
    APPROX_QUANTILES(LOWER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(25)] AS d30_LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(MED_upload_Mbps, 100) [SAFE_ORDINAL(50)] AS d30_MED_upload_Mbps,
    AVG(MEAN_upload_Mbps) AS d30_MEAN_upload_Mbps,
    APPROX_QUANTILES(UPPER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(75)] AS d30_UPPER_QUART_upload_Mbps,
    MAX(MAX_upload_Mbps) AS d30_MAX_upload_Mbps,
    STDDEV(MAX_upload_Mbps) AS d30_STDEV_max_ul
  FROM last30d_up_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
last60d_down AS (
  SELECT * FROM dl_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 60 DAY)
),
last60d_down_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps
  FROM last60d_down
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d60_download AS (
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_download_Mbps) AS d60_MIN_download_Mbps,
    APPROX_QUANTILES(LOWER_QUART_download_Mbps, 100) [SAFE_ORDINAL(25)] AS d60_LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(MED_download_Mbps, 100) [SAFE_ORDINAL(50)] AS d60_MED_download_Mbps,
    AVG(MEAN_download_Mbps) AS d60_MEAN_download_Mbps,
    APPROX_QUANTILES(UPPER_QUART_download_Mbps, 100) [SAFE_ORDINAL(75)] AS d60_UPPER_QUART_download_Mbps,
    MAX(MAX_download_Mbps) AS d60_MAX_download_Mbps,
    STDDEV(MAX_download_Mbps) AS d60_STDEV_max_dl
  FROM last60d_down_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
last60d_up AS (
  SELECT * FROM ul_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 60 DAY)
),
last60d_up_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(mbps) AS MAX_upload_Mbps
  FROM last60d_up
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d60_upload AS (
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_upload_Mbps) AS d60_MIN_upload_Mbps,
    APPROX_QUANTILES(LOWER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(25)] AS d60_LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(MED_upload_Mbps, 100) [SAFE_ORDINAL(50)] AS d60_MED_upload_Mbps,
    AVG(MEAN_upload_Mbps) AS d60_MEAN_upload_Mbps,
    APPROX_QUANTILES(UPPER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(75)] AS d60_UPPER_QUART_upload_Mbps,
    MAX(MAX_upload_Mbps) AS d60_MAX_upload_Mbps,
    STDDEV(MAX_upload_Mbps) AS d60_STDEV_max_ul
  FROM last60d_up_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
last90d_down AS (
  SELECT * FROM dl_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 90 DAY)
),
last90d_down_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps
  FROM last90d_down
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d90_download AS (
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_download_Mbps) AS d90_MIN_download_Mbps,
    APPROX_QUANTILES(LOWER_QUART_download_Mbps, 100) [SAFE_ORDINAL(25)] AS d90_LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(MED_download_Mbps, 100) [SAFE_ORDINAL(50)] AS d90_MED_download_Mbps,
    AVG(MEAN_download_Mbps) AS d90_MEAN_download_Mbps,
    APPROX_QUANTILES(UPPER_QUART_download_Mbps, 100) [SAFE_ORDINAL(75)] AS d90_PPER_QUART_download_Mbps,
    MAX(MAX_download_Mbps) AS d90_MAX_download_Mbps,
    STDDEV(MAX_download_Mbps) AS d90_STDEV_max_dl
  FROM last90d_down_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
last90d_up AS (
  SELECT * FROM ul_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 90 DAY)
),
last90d_up_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(mbps) AS MAX_upload_Mbps
  FROM last90d_up
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d90_upload AS (
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_upload_Mbps) AS d90_MIN_upload_Mbps,
    APPROX_QUANTILES(LOWER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(25)] AS d90_LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(MED_upload_Mbps, 100) [SAFE_ORDINAL(50)] AS d90_MED_upload_Mbps,
    AVG(MEAN_upload_Mbps) AS d90_MEAN_upload_Mbps,
    APPROX_QUANTILES(UPPER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(75)] AS d90_UPPER_QUART_upload_Mbps,
    MAX(MAX_upload_Mbps) AS d90_MAX_upload_Mbps,
    STDDEV(MAX_upload_Mbps) AS d90_STDEV_max_ul
  FROM last90d_up_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
last180d_down AS (
  SELECT * FROM dl_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 180 DAY)
),
last180d_down_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps
  FROM last180d_down
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d180_download AS (
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_download_Mbps) AS d180_MIN_download_Mbps,
    APPROX_QUANTILES(LOWER_QUART_download_Mbps, 100) [SAFE_ORDINAL(25)] AS d180_LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(MED_download_Mbps, 100) [SAFE_ORDINAL(50)] AS d180_MED_download_Mbps,
    AVG(MEAN_download_Mbps) AS d180_MEAN_download_Mbps,
    APPROX_QUANTILES(UPPER_QUART_download_Mbps, 100) [SAFE_ORDINAL(75)] AS d180_UPPER_QUART_download_Mbps,
    MAX(MAX_download_Mbps) AS d180_MAX_download_Mbps,
    STDDEV(MAX_download_Mbps) AS d180_STDEV_max_dl
  FROM last180d_down_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
last180d_up AS (
  SELECT * FROM ul_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 180 DAY)
),
last180d_up_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(mbps) AS MAX_upload_Mbps
  FROM last180d_up
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d180_upload AS (
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_upload_Mbps) AS d180_MIN_upload_Mbps,
    APPROX_QUANTILES(LOWER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(25)] AS d180_LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(MED_upload_Mbps, 100) [SAFE_ORDINAL(50)] AS d180_MED_upload_Mbps,
    AVG(MEAN_upload_Mbps) AS d180_MEAN_upload_Mbps,
    APPROX_QUANTILES(UPPER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(75)] AS d180_UPPER_QUART_upload_Mbps,
    MAX(MAX_upload_Mbps) AS d180_MAX_upload_Mbps,
    STDDEV(MAX_upload_Mbps) AS d180_STDEV_max_ul
  FROM last180d_up_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
last365d_down AS (
  SELECT * FROM dl_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 365 DAY)
),
last365d_down_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps
  FROM last365d_down
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d365_download AS (
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_download_Mbps) AS d365_MIN_download_Mbps,
    APPROX_QUANTILES(LOWER_QUART_download_Mbps, 100) [SAFE_ORDINAL(25)] AS d365_LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(MED_download_Mbps, 100) [SAFE_ORDINAL(50)] AS d365_MED_download_Mbps,
    AVG(MEAN_download_Mbps) AS d365_MEAN_download_Mbps,
    APPROX_QUANTILES(UPPER_QUART_download_Mbps, 100) [SAFE_ORDINAL(75)] AS d365_UPPER_QUART_download_Mbps,
    MAX(MAX_download_Mbps) AS d365_MAX_download_Mbps,
    STDDEV(MAX_download_Mbps) AS d365_STDEV_max_dl
  FROM last365d_down_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
),
last365d_up AS (
  SELECT * FROM ul_per_location_cleaned
  WHERE test_date > DATE_SUB(DATE_TRUNC(@startday, DAY), INTERVAL 365 DAY)
),
last365d_up_per_ip AS (
  SELECT
    test_date,
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(mbps) AS MAX_upload_Mbps
  FROM last365d_up
  GROUP BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, ip
),
d365_upload AS (
  SELECT 
    continent_code,
    country_code,
    country_name,
    ISO3166_2region1,
    ASNumber,
    MIN(MIN_upload_Mbps) AS d365_MIN_upload_Mbps,
    APPROX_QUANTILES(LOWER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(25)] AS d365_LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(MED_upload_Mbps, 100) [SAFE_ORDINAL(50)] AS d365_MED_upload_Mbps,
    AVG(MEAN_upload_Mbps) AS d365_MEAN_upload_Mbps,
    APPROX_QUANTILES(UPPER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(75)] AS d365_UPPER_QUART_upload_Mbps,
    MAX(MAX_upload_Mbps) AS d365_MAX_upload_Mbps,
    STDDEV(MAX_upload_Mbps) AS d365_STDEV_max_ul
  FROM last365d_up_per_ip
  GROUP BY continent_code, country_code, country_name, ISO3166_2region1, ASNumber
)
# Show the results
SELECT * FROM dl_histogram
JOIN ul_histogram USING (test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber, bucket_min, bucket_max)
JOIN dl_stats_per_day_per_as USING (test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN ul_stats_per_day_per_as USING (test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN dl_samples_per_as_per_day USING (test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN ul_samples_per_as_per_day USING (test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d7_download USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d7_upload USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d30_download USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d30_upload USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d60_download USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d60_upload USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d90_download USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d90_upload USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d180_download USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d180_upload USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d365_download USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
JOIN d365_upload USING (continent_code, country_code, country_name, ISO3166_2region1, ASNumber)
ORDER BY test_date, continent_code, country_code, country_name, ISO3166_2region1, ASNumber
