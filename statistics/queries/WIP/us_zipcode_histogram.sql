#standardSQL
WITH zip_codes AS (
  SELECT
    zip_code AS zipcode, city, county, state_code, state_name, zip_code_geom AS WKT
  FROM `bigquery-public-data.geo_us_boundaries.zip_codes`
),
dl_per_location_cleaned AS (
  SELECT
    test_date,
    CONCAT(client.Geo.country_code,"-",client.Geo.region) AS state,
    zip_codes.zipcode AS zipcode,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    a.MeanThroughputMbps AS mbps
  FROM
    `measurement-lab.ndt.unified_downloads` tests, zip_codes
  WHERE
    client.Geo.country_name = "United States"
    AND test_date = @startday
    AND client.Geo.country_code IS NOT NULL
    AND client.Geo.country_code != ""
    AND client.Geo.region IS NOT NULL
    AND client.Geo.region != ""
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), zip_codes.WKT
    )
),
dl_stats_perip_perday AS (
  SELECT
    test_date,
    state,
    zipcode,
    ip,
    MIN(mbps) AS MIN_download,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS Q25_download,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download,
    AVG(mbps) AS MEAN_download,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS Q75_download,
    MAX(mbps) AS MAX_download
  FROM dl_per_location_cleaned
  GROUP BY test_date, state, zipcode, ip
),
# Final stats per day
dl_stats_per_day AS (
  SELECT
    test_date,
    state,
    zipcode,
    MIN(MIN_download) AS MIN_download,
    APPROX_QUANTILES(Q25_download, 100) [SAFE_ORDINAL(25)] AS Q25_download,
    APPROX_QUANTILES(MED_download, 100) [SAFE_ORDINAL(50)] AS MED_download,
    AVG(MEAN_download) AS MEAN_download,
    APPROX_QUANTILES(Q75_download, 100) [SAFE_ORDINAL(75)] AS Q75_download,
    MAX(MAX_download) AS MAX_download
  FROM
    dl_stats_perip_perday
  GROUP BY test_date, state, zipcode
),
dl_total_samples_per_geo AS (
  SELECT
    test_date,
    COUNT(*) AS dl_total_samples,
    state,
    zipcode
  FROM dl_per_location_cleaned
  GROUP BY test_date, state, zipcode
),
# Now begin generating histograms of Max DL
max_dl_per_day_ip AS (
  SELECT
    test_date,
    state,
    zipcode,
    ip,
    MAX(mbps) AS mbps
  FROM dl_per_location_cleaned
  GROUP BY test_date, state, zipcode, ip
),
# Count the samples
dl_sample_counts AS (
  SELECT
    test_date,
    state,
    zipcode,
    COUNT(*) AS samples
  FROM max_dl_per_day_ip
  GROUP BY
    test_date,
    state,
    zipcode
),
# Generate equal sized buckets in log-space
buckets AS (
  SELECT POW(10, x-.2) AS bucket_min, POW(10,x) AS bucket_max
  FROM UNNEST(GENERATE_ARRAY(0, 4.2, .2)) AS x
),
dl_histogram_counts AS (
  SELECT
    test_date,
    state,
    zipcode,
    bucket_min,
    bucket_max,
    COUNTIF(mbps < bucket_max AND mbps >= bucket_min) AS bucket_count
  FROM max_dl_per_day_ip CROSS JOIN buckets
  GROUP BY
    test_date,
    state,
    zipcode,
    bucket_min,
    bucket_max
  ORDER BY
    test_date,
    state,
    zipcode,
    bucket_min,
    bucket_max
),
dl_histogram AS (
  SELECT
    test_date,
    state,
    zipcode,
    bucket_min,
    bucket_max,
    bucket_count / samples AS dl_frac,
    samples AS dl_samples
  FROM dl_histogram_counts
  JOIN dl_sample_counts USING (test_date, state, zipcode)
),
ul_per_location_cleaned AS (
  SELECT
    test_date,
    CONCAT(client.Geo.country_code,"-",client.Geo.region) AS state,
    zip_codes.zipcode AS zipcode,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    a.MeanThroughputMbps AS mbps
  FROM
    `measurement-lab.ndt.unified_uploads` tests, zip_codes
  WHERE
    client.Geo.country_name = "United States"
    AND test_date = @startday
    AND client.Geo.country_code IS NOT NULL
    AND client.Geo.country_code != ""
    AND client.Geo.region IS NOT NULL
    AND client.Geo.region != ""
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), zip_codes.WKT
    )
),
max_ul_per_day_ip AS (
  SELECT
    test_date,
    state,
    zipcode,
    ip,
    MAX(mbps) AS mbps
  FROM ul_per_location_cleaned
  GROUP BY test_date, state, zipcode, ip
),
# Count the samples
ul_sample_counts AS (
  SELECT
    test_date,
    state,
    zipcode,
    COUNT(*) AS samples
  FROM max_ul_per_day_ip
  GROUP BY
    test_date,
    state,
    zipcode
),
ul_histogram_counts AS (
  SELECT
    test_date,
    state,
    zipcode,
    bucket_min,
    bucket_max,
    COUNTIF(mbps < bucket_max AND mbps >= bucket_min) AS bucket_count
  FROM max_ul_per_day_ip CROSS JOIN buckets
  GROUP BY
    test_date,
    state,
    zipcode,
    bucket_min,
    bucket_max
  ORDER BY
    test_date,
    state,
    zipcode,
    bucket_min,
    bucket_max
),
ul_histogram AS (
  SELECT
    test_date,
    state,
    zipcode,
    bucket_min,
    bucket_max,
    bucket_count / samples AS ul_frac,
    samples AS ul_samples
  FROM ul_histogram_counts
  JOIN ul_sample_counts USING (test_date, state, zipcode)
),
# Generate upload test stats for each geo per ip, per day
ul_stats_perip_perday AS (
  SELECT
    test_date,
    state,
    zipcode,
    ip,
    MIN(mbps) AS MIN_upload,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS Q25_upload,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload,
    AVG(mbps) AS MEAN_upload,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS Q75_upload,
    MAX(mbps) AS MAX_upload
  FROM ul_per_location_cleaned
  GROUP BY test_date, state, zipcode, ip
),
# Final upload stats per day
ul_stats_per_day AS (
  SELECT
    test_date,
    state,
    zipcode,
    MIN(MIN_upload) AS MIN_upload,
    APPROX_QUANTILES(Q25_upload, 100) [SAFE_ORDINAL(25)] AS Q25_upload,
    APPROX_QUANTILES(MED_upload, 100) [SAFE_ORDINAL(50)] AS MED_upload,
    AVG(MEAN_upload) AS MEAN_upload,
    APPROX_QUANTILES(Q75_upload, 100) [SAFE_ORDINAL(75)] AS Q75_upload,
    MAX(MAX_upload) AS MAX_upload
  FROM
    ul_stats_perip_perday
  GROUP BY test_date, state, zipcode
),
ul_total_samples_per_geo AS (
  SELECT
    test_date,
    COUNT(*) AS ul_total_samples,
    state,
    zipcode
  FROM ul_per_location_cleaned
  GROUP BY test_date, state, zipcode
)
SELECT * FROM dl_histogram
JOIN ul_histogram USING (test_date, state, zipcode, bucket_min, bucket_max)
JOIN dl_stats_per_day USING (test_date, state, zipcode)
JOIN dl_total_samples_per_geo USING (test_date, state, zipcode)
JOIN ul_stats_per_day USING (test_date, state, zipcode)
JOIN ul_total_samples_per_geo USING (test_date, state, zipcode)
ORDER BY test_date, state, zipcode, bucket_min, bucket_max