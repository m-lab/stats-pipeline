#standardSQL
# AIANNH - us_american_indian_alaska_native_areas_hawaiian_home_lands
WITH aiannh AS (
  SELECT
    NAME AS name,
    WKT,
    GEOID
  FROM
    `measurement-lab.geographies.us_aiannh_2018`
),
dl AS (
  SELECT
    test_date,
    aiannh.GEOID AS GEOID,
    client.IP AS clientIP,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM
    `measurement-lab.ndt.unified_downloads` tests, aiannh
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= '2020-01-01'
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), aiannh.WKT
    )
),
mlab_dl_perip_perday AS (
  SELECT
    test_date,
    GEOID,
    clientIP,
    MIN(mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(mbps) AS MAX_download_Mbps,
    APPROX_QUANTILES(CAST(MinRTT AS FLOAT64), 100) [ORDINAL(50)] as MED_DL_min_rtt
  FROM dl
  GROUP BY test_date, GEOID, clientIP
),
aiannh_stats_dl AS (
  SELECT
    FORMAT_DATE("%Y%m", test_date) AS time_period,
    GEOID,
    MIN(MIN_download_Mbps) AS MIN_download_Mbps,
    APPROX_QUANTILES(LOWER_QUART_download_Mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_download_Mbps,
    APPROX_QUANTILES(MED_download_Mbps, 100) [SAFE_ORDINAL(50)] AS MED_download_Mbps,
    AVG(MEAN_download_Mbps) AS MEAN_download_Mbps,
    APPROX_QUANTILES(UPPER_QUART_download_Mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_download_Mbps,
    MAX(MAX_download_Mbps) AS MAX_download_Mbps,
    APPROX_QUANTILES(CAST(MED_DL_min_rtt AS FLOAT64), 100) [ORDINAL(50)] as MED_DL_min_rtt
  FROM mlab_dl_perip_perday
  GROUP BY time_period, GEOID
),    
aiannh_dl_sample AS (
  SELECT 
    COUNT(*) AS aiannh_dl_sample_size,
    COUNT(DISTINCT clientIP) AS sample_dl_count_ips, 
    GEOID,
    FORMAT_DATE("%Y%m", test_date) AS time_period,
  FROM dl
  GROUP BY time_period, GEOID
),
ul AS (
  SELECT
    test_date,
    aiannh.GEOID AS GEOID,
    client.IP AS clientIP,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM
    `measurement-lab.ndt.unified_uploads` tests, aiannh
  WHERE
    client.Geo.country_name = "United States"
    AND test_date >= '2020-01-01'
    AND ST_WITHIN(
      ST_GeogPoint(
        client.Geo.longitude,
        client.Geo.latitude
      ), aiannh.WKT
    )
),
mlab_ul_perip_perday AS (
  SELECT
    test_date,
    GEOID,
    clientIP,
    MIN(mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(mbps) AS MAX_upload_Mbps
  FROM ul
  GROUP BY test_date, GEOID, clientIP
),
aiannh_stats_ul AS (
  SELECT
    FORMAT_DATE("%Y%m", test_date) AS time_period,
    GEOID,
    MIN(MIN_upload_Mbps) AS MIN_upload_Mbps,
    APPROX_QUANTILES(LOWER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(25)] AS LOWER_QUART_upload_Mbps,
    APPROX_QUANTILES(MED_upload_Mbps, 100) [SAFE_ORDINAL(50)] AS MED_upload_Mbps,
    AVG(MEAN_upload_Mbps) AS MEAN_upload_Mbps,
    APPROX_QUANTILES(UPPER_QUART_upload_Mbps, 100) [SAFE_ORDINAL(75)] AS UPPER_QUART_upload_Mbps,
    MAX(MAX_upload_Mbps) AS MAX_upload_Mbps
  FROM mlab_ul_perip_perday
  GROUP BY time_period, GEOID
),    
aiannh_ul_sample AS (
  SELECT 
    COUNT(*) AS aiannh_ul_sample_size,
    COUNT(DISTINCT clientIP) AS sample_ul_count_ips, 
    GEOID,
    FORMAT_DATE("%Y%m", test_date) AS time_period,
  FROM ul
  GROUP BY time_period, GEOID
),
DL_pct_levels AS (
  SELECT 
    FORMAT_DATE("%Y%m", test_date) AS time_period,
    dl.GEOID,
    COUNTIF(mbps < 1) / COUNT(*) AS pct_under_1mbpsDL,
    COUNTIF(mbps < 4) / COUNT(*) AS pct_under_4mbpsDL,
    COUNTIF(mbps < 7) / COUNT(*) AS pct_under_7mbpsDL,
    COUNTIF(mbps < 10) / COUNT(*) AS pct_under_10mbpsDL,
    COUNTIF(mbps < 15) / COUNT(*) AS pct_under_15mbpsDL,
    COUNTIF(mbps < 25) / COUNT(*) AS pct_under_25mbpsDL,
    COUNTIF(mbps < 30) / COUNT(*) AS pct_under_30mbpsDL,
    COUNTIF(mbps < 50) / COUNT(*) AS pct_under_50mbpsDL, 
    COUNTIF(mbps < 100) / COUNT(*) AS pct_under_100mbpsDL,
    COUNTIF(mbps < 150) / COUNT(*) AS pct_under_150mbpsDL, 
    COUNTIF(mbps < 200) / COUNT(*) AS pct_under_200mbpsDL,
    COUNTIF(mbps < 300) / COUNT(*) AS pct_under_300mbpsDL, 
    COUNTIF(mbps < 400) / COUNT(*) AS pct_under_400mbpsDL,
    COUNTIF(mbps < 500) / COUNT(*) AS pct_under_500mbpsDL,
    COUNTIF(mbps < 600) / COUNT(*) AS pct_under_600mbpsDL,
    COUNTIF(mbps < 700) / COUNT(*) AS pct_under_700mbpsDL, 
    COUNTIF(mbps < 800) / COUNT(*) AS pct_under_800mbpsDL,
    COUNTIF(mbps < 900) / COUNT(*) AS pct_under_900mbpsDL, 
    COUNTIF(mbps < 1000) / COUNT(*) AS pct_under_1000mbpsDL,
    COUNTIF(mbps < 1) AS cnt_under_1mbpsDL,
    COUNTIF(mbps < 3) AS cnt_under_3mbpsDL,
    COUNTIF(mbps < 7) AS cnt_under_7mbpsDL,
    COUNTIF(mbps < 10) AS cnt_under_10mbpsDL,
    COUNTIF(mbps < 15) AS cnt_under_15mbpsDL,
    COUNTIF(mbps < 25) AS cnt_under_25mbpsDL,
    COUNTIF(mbps < 30) AS cnt_under_30mbpsDL,
    COUNTIF(mbps < 50) AS cnt_under_50mbpsDL, 
    COUNTIF(mbps < 100) AS cnt_under_100mbpsDL,
    COUNTIF(mbps < 150) AS cnt_under_150mbpsDL, 
    COUNTIF(mbps < 200) AS cnt_under_200mbpsDL,
    COUNTIF(mbps < 300) AS cnt_under_300mbpsDL, 
    COUNTIF(mbps < 400) AS cnt_under_400mbpsDL,
    COUNTIF(mbps < 500) AS cnt_under_500mbpsDL,
    COUNTIF(mbps < 600) AS cnt_under_600mbpsDL,
    COUNTIF(mbps < 700) AS cnt_under_700mbpsDL, 
    COUNTIF(mbps < 800) AS cnt_under_800mbpsDL,
    COUNTIF(mbps < 900) AS cnt_under_900mbpsDL, 
    COUNTIF(mbps < 1000) AS cnt_under_1000mbpsDL
  FROM dl
  GROUP BY time_period, GEOID
),
UL_pct_levels AS (
  SELECT 
    FORMAT_DATE("%Y%m", test_date) AS time_period,
    ul.GEOID,
    COUNTIF(mbps < 1) / COUNT(*) AS pct_under_1mbpsUL,
    COUNTIF(mbps < 4) / COUNT(*) AS pct_under_4mbpsUL,
    COUNTIF(mbps < 7) / COUNT(*) AS pct_under_7mbpsUL,
    COUNTIF(mbps < 10) / COUNT(*) AS pct_under_10mbpsUL,
    COUNTIF(mbps < 15) / COUNT(*) AS pct_under_15mbpsUL,
    COUNTIF(mbps < 25) / COUNT(*) AS pct_under_25mbpsUL,
    COUNTIF(mbps < 30) / COUNT(*) AS pct_under_30mbpsUL,
    COUNTIF(mbps < 50) / COUNT(*) AS pct_under_50mbpsUL, 
    COUNTIF(mbps < 100) / COUNT(*) AS pct_under_100mbpsUL,
    COUNTIF(mbps < 150) / COUNT(*) AS pct_under_150mbpsUL, 
    COUNTIF(mbps < 200) / COUNT(*) AS pct_under_200mbpsUL,
    COUNTIF(mbps < 300) / COUNT(*) AS pct_under_300mbpsUL, 
    COUNTIF(mbps < 400) / COUNT(*) AS pct_under_400mbpsUL,
    COUNTIF(mbps < 500) / COUNT(*) AS pct_under_500mbpsUL,
    COUNTIF(mbps < 600) / COUNT(*) AS pct_under_600mbpsUL,
    COUNTIF(mbps < 700) / COUNT(*) AS pct_under_700mbpsUL, 
    COUNTIF(mbps < 800) / COUNT(*) AS pct_under_800mbpsUL,
    COUNTIF(mbps < 900) / COUNT(*) AS pct_under_900mbpsUL, 
    COUNTIF(mbps < 1000) / COUNT(*) AS pct_under_1000mbpsUL,
    COUNTIF(mbps < 1) AS cnt_under_1mbpsUL,
    COUNTIF(mbps < 3) AS cnt_under_3mbpsUL,
    COUNTIF(mbps < 7) AS cnt_under_7mbpsUL,
    COUNTIF(mbps < 10) AS cnt_under_10mbpsUL,
    COUNTIF(mbps < 15) AS cnt_under_15mbpsUL,
    COUNTIF(mbps < 25) AS cnt_under_25mbpsUL,
    COUNTIF(mbps < 30) AS cnt_under_30mbpsUL,
    COUNTIF(mbps < 50) AS cnt_under_50mbpsUL, 
    COUNTIF(mbps < 100) AS cnt_under_100mbpsUL,
    COUNTIF(mbps < 150) AS cnt_under_150mbpsUL, 
    COUNTIF(mbps < 200) AS cnt_under_200mbpsUL,
    COUNTIF(mbps < 300) AS cnt_under_300mbpsUL, 
    COUNTIF(mbps < 400) AS cnt_under_400mbpsUL,
    COUNTIF(mbps < 500) AS cnt_under_500mbpsUL,
    COUNTIF(mbps < 600) AS cnt_under_600mbpsUL,
    COUNTIF(mbps < 700) AS cnt_under_700mbpsUL, 
    COUNTIF(mbps < 800) AS cnt_under_800mbpsUL,
    COUNTIF(mbps < 900) AS cnt_under_900mbpsUL, 
    COUNTIF(mbps < 1000) AS cnt_under_1000mbpsUL    
  FROM ul
  GROUP BY time_period, GEOID
)
SELECT * FROM aiannh_stats_dl
JOIN aiannh_stats_ul USING (time_period, GEOID)
JOIN aiannh_dl_sample USING (time_period, GEOID)
JOIN aiannh_ul_sample USING (time_period, GEOID)
JOIN DL_pct_levels USING (time_period, GEOID)
JOIN UL_pct_levels USING (time_period, GEOID)
JOIN aiannh USING (GEOID)