#standardSQL
WITH counties AS (
  SELECT
    county_name AS name,
    county_geom AS geom,
    geo_id as geo_id
  FROM
    `mlab-sandbox.usa_geo.us_counties`
),

mlab_dl AS (
SELECT 
APPROX_QUANTILES(ml_download_Mbps, 101) [SAFE_ORDINAL(51)] AS med_dl_Mbps,
SUM(ml_dl_count_tests) as county_tests,
partition_date,
FORMAT(
      '%s_%d',
      ['jun', 'dec'] [ORDINAL(CAST(CEIL(EXTRACT(MONTH FROM partition_date) / 6) AS INT64))],
      EXTRACT(
        YEAR
        FROM
          partition_date
      )
) as time_period,
geo_id
FROM
(
  SELECT
  COUNT(test_id) AS ml_dl_count_tests,
  APPROX_QUANTILES(
    8 * SAFE_DIVIDE(
      web100_log_entry.snap.HCThruOctetsAcked,
      (
        web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd
      )
    ),
    101 IGNORE NULLS
  ) [SAFE_ORDINAL(51)] AS ml_download_Mbps,
  partition_date,
  geo_id
  
  FROM 
  `measurement-lab.release.ndt_downloads` tests,
  `mlab-sandbox.usa_geo.us_counties` counties
  WHERE
  connection_spec.server_geolocation.country_name = 'United States'
  AND ST_WITHIN(
    ST_GeogPoint(
      connection_spec.client_geolocation.longitude,
      connection_spec.client_geolocation.latitude
    ),
    counties.county_geom
  )
  GROUP BY
  connection_spec.client_ip,
  geo_id,
  partition_date
)
GROUP BY
geo_id,
partition_date
),

mlab_ul AS (

SELECT 
APPROX_QUANTILES(ml_upload_Mbps, 101) [SAFE_ORDINAL(51)] AS med_ul_Mbps,
partition_date,
geo_id
FROM
(
  SELECT
  COUNT(test_id) AS ml_ul_count_tests,
  APPROX_QUANTILES(
 8 * SAFE_DIVIDE(
        web100_log_entry.snap.HCThruOctetsReceived,
        web100_log_entry.snap.Duration
),
    101 IGNORE NULLS
  ) [SAFE_ORDINAL(51)] AS ml_upload_Mbps,
  partition_date,
  geo_id
  
  FROM 
  `measurement-lab.release.ndt_uploads` tests,
  `mlab-sandbox.usa_geo.us_counties` counties
  WHERE
  connection_spec.server_geolocation.country_name = 'United States'
  AND ST_WITHIN(
    ST_GeogPoint(
      connection_spec.client_geolocation.longitude,
      connection_spec.client_geolocation.latitude
    ),
    counties.county_geom
  )
  GROUP BY
  connection_spec.client_ip,
  geo_id,
  partition_date
)
GROUP BY
geo_id,
partition_date
),
fccdata AS (
  SELECT *, 'dec_2014' AS time_period FROM `mlab-sandbox.fcc.477_dec_2014`
  UNION ALL SELECT *, 'dec_2015' AS time_period FROM `mlab-sandbox.fcc.477_dec_2015`
  UNION ALL SELECT *, 'dec_2016' AS time_period FROM `mlab-sandbox.fcc.477_dec_2016`
  UNION ALL SELECT *, 'jun_2015' AS time_period FROM `mlab-sandbox.fcc.477_jun_2015`
  UNION ALL SELECT *, 'jun_2016' AS time_period FROM `mlab-sandbox.fcc.477_jun_2016`
  UNION ALL SELECT *, 'jun_2017' AS time_period FROM `mlab-sandbox.fcc.477_jun_2017`
  UNION ALL SELECT *, 'dec_2017' AS time_period FROM `mlab-sandbox.fcc.477_dec_2017`
),
fcc_providerMedians AS (
  SELECT
      time_period,
      APPROX_QUANTILES(CAST(Max_Advertised_Downstream_Speed__mbps_ AS FLOAT64), 101)[SAFE_ORDINAL(51)] AS advertised_down,
      APPROX_QUANTILES(CAST(Max_Advertised_Upstream_Speed__mbps_ AS FLOAT64), 101)[SAFE_ORDINAL(51)] AS advertised_up,
      MAX(CAST(Max_Advertised_Downstream_Speed__mbps_ AS FLOAT64)) AS advertised_down_max,
      SUBSTR(Census_Block_FIPS_Code, 0, 5) as geo_id,
      Census_Block_FIPS_Code AS FIPS
  FROM fccdata
  WHERE Consumer = '1'
  GROUP BY geo_id, time_period, FRN
),
fcc_groups AS (
  SELECT
      fccdata.time_period,
      COUNT(DISTINCT FRN) AS reg_provider_count, 
      APPROX_QUANTILES(fcc_providerMedians.advertised_down, 101)[SAFE_ORDINAL(51)] AS advertised_down,
      APPROX_QUANTILES(fcc_providerMedians.advertised_up, 101)[SAFE_ORDINAL(51)] AS advertised_up,
      MAX(CAST(Max_Advertised_Downstream_Speed__mbps_ AS FLOAT64)) AS advertised_down_max,
      SUBSTR(Census_Block_FIPS_Code, 0, 5) as geo_id,
      Census_Block_FIPS_Code AS FIPS
  FROM fccdata JOIN fcc_providerMedians 
  ON SUBSTR(fccdata.Census_Block_FIPS_Code, 0, 5) = fcc_providerMedians.geo_id AND fccdata.time_period = fcc_providerMedians.time_period 
  WHERE Consumer = '1'
  GROUP BY geo_id, time_period 
),
fcc_timeslices AS (
  SELECT 
    time_period,
    reg_provider_count,
    advertised_down,
    advertised_up,
    advertised_down_max,
    geo_id,
    FIPS
  FROM fcc_groups
)
SELECT
      partition_date, 
      time_period,
      geo_id,
      med_dl_Mbps,
      med_ul_Mbps,
      county_tests,
      reg_provider_count,
      advertised_down,
      advertised_up,
      advertised_down_max,
      FIPS
    
FROM
(mlab_dl JOIN mlab_ul USING (geo_id, partition_date) JOIN fcc_timeslices USING (geo_id, time_period))