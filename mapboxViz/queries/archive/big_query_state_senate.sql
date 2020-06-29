#standardSQL
WITH dl AS (
  SELECT
    COUNT(test_id) AS ml_dl_count_tests,
    COUNT(DISTINCT connection_spec.client_ip) as ml_dl_count_ips,
    APPROX_QUANTILES(
      8 * SAFE_DIVIDE(
        web100_log_entry.snap.HCThruOctetsAcked,
        (
          web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd
        )
      ),
      101 IGNORE NULLS
    ) [SAFE_ORDINAL(51)] AS ml_download_Mbps,
    APPROX_QUANTILES(CAST(web100_log_entry.snap.MinRTT AS FLOAT64), 101 IGNORE NULLS) [SAFE_ORDINAL(51)] AS ml_min_rtt,
    CASE
      WHEN partition_date BETWEEN '2014-07-01'
      AND '2014-12-31' THEN 'dec_2014'
      WHEN partition_date BETWEEN '2015-01-01'
      AND '2015-06-30' THEN 'jun_2015'
      WHEN partition_date BETWEEN '2015-07-01'
      AND '2015-12-31' THEN 'dec_2015'
      WHEN partition_date BETWEEN '2016-01-01'
      AND '2016-06-30' THEN 'jun_2016'
      WHEN partition_date BETWEEN '2016-07-01'
      AND '2016-12-31' THEN 'dec_2016'
      WHEN partition_date BETWEEN '2017-01-01'
      AND '2017-06-30' THEN 'jun_2017'
      WHEN partition_date BETWEEN '2017-07-01'
      AND '2017-12-31' THEN 'dec_2017'
      WHEN partition_date BETWEEN '2018-01-01'
      AND '2018-06-30' THEN 'jun_2018'
      WHEN partition_date BETWEEN '2018-07-01'
      AND '2018-12-31' THEN 'dec_2018'
    END AS time_period,
    GEOID
  FROM
    `measurement-lab.release.ndt_downloads` tests,
    `mlab-sandbox.usa_geo.cb_2016_state_senate_districts` districts
  WHERE
    connection_spec.server_geolocation.country_name = "United States"
    AND partition_date BETWEEN '2014-07-01'
    AND '2018-12-31'
    AND ST_WITHIN(
      ST_GeogPoint(
        connection_spec.client_geolocation.longitude,
        connection_spec.client_geolocation.latitude
      ),
      districts.state_senate_districts_polygons
    )
  GROUP BY
    GEOID,
    time_period
),
ul AS (
  SELECT
    COUNT(test_id) AS ml_ul_count_tests,
    COUNT(DISTINCT connection_spec.client_ip) AS ml_ul_count_ips,
    APPROX_QUANTILES(
      8 * SAFE_DIVIDE(
        web100_log_entry.snap.HCThruOctetsReceived,
        web100_log_entry.snap.Duration
      ),
      101 IGNORE NULLS
    ) [SAFE_ORDINAL(51)] AS ml_upload_Mbps,
    CASE
      WHEN partition_date BETWEEN '2014-07-01'
      AND '2014-12-31' THEN 'dec_2014'
      WHEN partition_date BETWEEN '2015-01-01'
      AND '2015-06-30' THEN 'jun_2015'
      WHEN partition_date BETWEEN '2015-07-01'
      AND '2015-12-31' THEN 'dec_2015'
      WHEN partition_date BETWEEN '2016-01-01'
      AND '2016-06-30' THEN 'jun_2016'
      WHEN partition_date BETWEEN '2016-07-01'
      AND '2016-12-31' THEN 'dec_2016'
      WHEN partition_date BETWEEN '2017-01-01'
      AND '2017-06-30' THEN 'jun_2017'
      WHEN partition_date BETWEEN '2017-07-01'
      AND '2017-12-31' THEN 'dec_2017'
      WHEN partition_date BETWEEN '2018-01-01'
      AND '2018-06-30' THEN 'jun_2018'
      WHEN partition_date BETWEEN '2018-07-01'
      AND '2018-12-31' THEN 'dec_2018'
    END AS time_period,
    GEOID
  FROM
    `measurement-lab.release.ndt_uploads` tests,
    `mlab-sandbox.usa_geo.cb_2016_state_senate_districts` districts
  WHERE
    connection_spec.server_geolocation.country_name = "United States"
    AND partition_date BETWEEN '2014-07-01'
    AND '2018-12-31'
    AND ST_WITHIN(
      ST_GeogPoint(
        connection_spec.client_geolocation.longitude,
        connection_spec.client_geolocation.latitude
      ),
      districts.state_senate_districts_polygons
    )
  GROUP BY
    GEOID,
    time_period
),
main AS (
  SELECT
    GEOID,
    time_period,
    ml_ul_count_tests,
    ml_ul_count_ips,
    ml_upload_Mbps,
    ml_dl_count_tests,
    ml_dl_count_ips,
    ml_download_Mbps,
    ml_min_rtt
  FROM
    dl
    JOIN ul USING (GEOID, time_period)
),
main_dec_2014 AS (
  SELECT
    GEOID,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_dec_2014,
    ml_ul_count_ips AS ml_ul_count_ips_dec_2014,
    ml_upload_Mbps AS ml_upload_Mbps_dec_2014,
    ml_dl_count_tests AS ml_dl_count_tests_dec_2014,
    ml_dl_count_ips AS ml_dl_count_ips_dec_2014,
    ml_download_Mbps AS ml_download_Mbps_dec_2014,
    ml_min_rtt AS ml_min_rtt_dec_2014
  FROM
    main
  WHERE
    time_period = 'dec_2014'
),
main_jun_2015 AS (
  SELECT
    GEOID,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_jun_2015,
    ml_ul_count_ips AS ml_ul_count_ips_jun_2015,
    ml_upload_Mbps AS ml_upload_Mbps_jun_2015,
    ml_dl_count_tests AS ml_dl_count_tests_jun_2015,
    ml_dl_count_ips AS ml_dl_count_ips_jun_2015,
    ml_download_Mbps AS ml_download_Mbps_jun_2015,
    ml_min_rtt AS ml_min_rtt_jun_2015
  FROM
    main
  WHERE
    time_period = 'jun_2015'
),
main_dec_2015 AS (
  SELECT
    GEOID,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_dec_2015,
    ml_ul_count_ips AS ml_ul_count_ips_dec_2015,
    ml_upload_Mbps AS ml_upload_Mbps_dec_2015,
    ml_dl_count_tests AS ml_dl_count_tests_dec_2015,
    ml_dl_count_ips AS ml_dl_count_ips_dec_2015,
    ml_download_Mbps AS ml_download_Mbps_dec_2015,
    ml_min_rtt AS ml_min_rtt_dec_2015
  FROM
    main
  WHERE
    time_period = 'dec_2015'
),
main_jun_2016 AS (
  SELECT
    GEOID,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_jun_2016,
    ml_ul_count_ips AS ml_ul_count_ips_jun_2016,
    ml_upload_Mbps AS ml_upload_Mbps_jun_2016,
    ml_dl_count_tests AS ml_dl_count_tests_jun_2016,
    ml_dl_count_ips AS ml_dl_count_ips_jun_2016,
    ml_download_Mbps AS ml_download_Mbps_jun_2016,
    ml_min_rtt AS ml_min_rtt_jun_2016
  FROM
    main
  WHERE
    time_period = 'jun_2016'
),
main_dec_2016 AS (
  SELECT
    GEOID,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_dec_2016,
    ml_ul_count_ips AS ml_ul_count_ips_dec_2016,
    ml_upload_Mbps AS ml_upload_Mbps_dec_2016,
    ml_dl_count_tests AS ml_dl_count_tests_dec_2016,
    ml_dl_count_ips AS ml_dl_count_ips_dec_2016,
    ml_download_Mbps AS ml_download_Mbps_dec_2016,
    ml_min_rtt AS ml_min_rtt_dec_2016
  FROM
    main
  WHERE
    time_period = 'dec_2016'
),
main_jun_2017 AS (
  SELECT
    GEOID,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_jun_2017,
    ml_ul_count_ips AS ml_ul_count_ips_jun_2017,
    ml_upload_Mbps AS ml_upload_Mbps_jun_2017,
    ml_dl_count_tests AS ml_dl_count_tests_jun_2017,
    ml_dl_count_ips AS ml_dl_count_ips_jun_2017,
    ml_download_Mbps AS ml_download_Mbps_jun_2017,
    ml_min_rtt AS ml_min_rtt_jun_2017
  FROM
    main
  WHERE
    time_period = 'jun_2017'
),
main_dec_2017 AS (
  SELECT
    GEOID,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_dec_2017,
    ml_ul_count_ips AS ml_ul_count_ips_dec_2017,
    ml_upload_Mbps AS ml_upload_Mbps_dec_2017,
    ml_dl_count_tests AS ml_dl_count_tests_dec_2017,
    ml_dl_count_ips AS ml_dl_count_ips_dec_2017,
    ml_download_Mbps AS ml_download_Mbps_dec_2017,
    ml_min_rtt AS ml_min_rtt_dec_2017
  FROM
    main
  WHERE
    time_period = 'dec_2017'
),
main_jun_2018 AS (
  SELECT
    GEOID,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_jun_2018,
    ml_ul_count_ips AS ml_ul_count_ips_jun_2018,
    ml_upload_Mbps AS ml_upload_Mbps_jun_2018,
    ml_dl_count_tests AS ml_dl_count_tests_jun_2018,
    ml_dl_count_ips AS ml_dl_count_ips_jun_2018,
    ml_download_Mbps AS ml_download_Mbps_jun_2018,
    ml_min_rtt AS ml_min_rtt_jun_2018
  FROM
    main
  WHERE
    time_period = 'jun_2018'
),
main_dec_2018 AS (
  SELECT
    GEOID,
    time_period,
    ml_ul_count_tests AS ml_ul_count_tests_dec_2018,
    ml_ul_count_ips AS ml_ul_count_ips_dec_2018,
    ml_upload_Mbps AS ml_upload_Mbps_dec_2018,
    ml_dl_count_tests AS ml_dl_count_tests_dec_2018,
    ml_dl_count_ips AS ml_dl_count_ips_dec_2018,
    ml_download_Mbps AS ml_download_Mbps_dec_2018,
    ml_min_rtt AS ml_min_rtt_dec_2018
  FROM
    main
  WHERE
    time_period = 'dec_2018'
)
SELECT
  ml_ul_count_tests_dec_2014,
  ml_ul_count_ips_dec_2014,
  ml_upload_Mbps_dec_2014,
  ml_dl_count_tests_dec_2014,
  ml_dl_count_ips_dec_2014,
  ml_download_Mbps_dec_2014,
  ml_min_rtt_dec_2014,
  ml_ul_count_tests_jun_2015,
  ml_ul_count_ips_jun_2015,
  ml_upload_Mbps_jun_2015,
  ml_dl_count_tests_jun_2015,
  ml_dl_count_ips_jun_2015,
  ml_download_Mbps_jun_2015,
  ml_min_rtt_jun_2015,
  ml_ul_count_tests_dec_2015,
  ml_ul_count_ips_dec_2015,
  ml_upload_Mbps_dec_2015,
  ml_dl_count_tests_dec_2015,
  ml_dl_count_ips_dec_2015,
  ml_download_Mbps_dec_2015,
  ml_min_rtt_dec_2015,
  ml_ul_count_tests_jun_2016,
  ml_ul_count_ips_jun_2016,
  ml_upload_Mbps_jun_2016,
  ml_dl_count_tests_jun_2016,
  ml_dl_count_ips_jun_2016,
  ml_download_Mbps_jun_2016,
  ml_min_rtt_jun_2016,
  ml_ul_count_tests_dec_2016,
  ml_ul_count_ips_dec_2016,
  ml_upload_Mbps_dec_2016,
  ml_dl_count_tests_dec_2016,
  ml_dl_count_ips_dec_2016,
  ml_download_Mbps_dec_2016,
  ml_min_rtt_dec_2016,
  ml_ul_count_tests_jun_2017,
  ml_ul_count_ips_jun_2017,
  ml_upload_Mbps_jun_2017,
  ml_dl_count_tests_jun_2017,
  ml_dl_count_ips_jun_2017,
  ml_download_Mbps_jun_2017,
  ml_min_rtt_jun_2017,
  ml_ul_count_tests_dec_2017,
  ml_ul_count_ips_dec_2017,
  ml_upload_Mbps_dec_2017,
  ml_dl_count_tests_dec_2017,
  ml_dl_count_ips_dec_2017,
  ml_download_Mbps_dec_2017,
  ml_min_rtt_dec_2017,
  ml_ul_count_tests_jun_2018,
  ml_ul_count_ips_jun_2018,
  ml_upload_Mbps_jun_2018,
  ml_dl_count_tests_jun_2018,
  ml_dl_count_ips_jun_2018,
  ml_download_Mbps_jun_2018,
  ml_min_rtt_jun_2018,
  ml_ul_count_tests_dec_2018,
  ml_ul_count_ips_dec_2018,
  ml_upload_Mbps_dec_2018,
  ml_dl_count_tests_dec_2018,
  ml_dl_count_ips_dec_2018,
  ml_download_Mbps_dec_2018,
  ml_min_rtt_dec_2018,
  districts.GEOID as GEOID,
  districts.NAME AS tract_name,
  districts.state_senate_districts_polygons AS WKT
FROM
  `mlab-sandbox.usa_geo.cb_2016_state_senate_districts` districts
  LEFT JOIN main_dec_2014 USING (GEOID)
  LEFT JOIN main_jun_2015 USING (GEOID)
  LEFT JOIN main_dec_2015 USING (GEOID)
  LEFT JOIN main_jun_2016 USING (GEOID)
  LEFT JOIN main_dec_2016 USING (GEOID)
  LEFT JOIN main_jun_2017 USING (GEOID)
  LEFT JOIN main_dec_2017 USING (GEOID)
  LEFT JOIN main_jun_2018 USING (GEOID)
  LEFT JOIN main_dec_2018 USING (GEOID);