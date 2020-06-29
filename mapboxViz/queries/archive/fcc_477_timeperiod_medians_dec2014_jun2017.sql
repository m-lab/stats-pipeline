#standardSQL
WITH main as (
    SELECT
      SUBSTR(Census_Block_FIPS_Code, 0, 5) as fips,
      Provider_ID as pid,
      CAST(Max_Advertised_Downstream_Speed__mbps_ AS FLOAT64) as down,
      CAST(Max_Advertised_Upstream_Speed__mbps_ AS FLOAT64) as up,
      time_period
    FROM `mlab-sandbox.georgia_usb.fcc_477_dec2014_jun_2017`
    WHERE Consumer = '1'),
  dec_2014 as (
    SELECT
      fips,
      COUNT(DISTINCT pid) as pid,
      APPROX_QUANTILES(down, 101)[SAFE_ORDINAL(51)] as down,
      APPROX_QUANTILES(up, 101)[SAFE_ORDINAL(51)] as up
    FROM main
    WHERE time_period = 'dec_2014'
    GROUP BY fips),
  jun_2015 as ( 
    SELECT
      fips,
      COUNT(DISTINCT pid) as pid,
      APPROX_QUANTILES(down, 101)[SAFE_ORDINAL(51)] as down,
      APPROX_QUANTILES(up, 101)[SAFE_ORDINAL(51)] as up
    FROM main
    WHERE time_period = 'jun_2015'
    GROUP BY fips),
  dec_2015 as (
    SELECT
      fips,
      COUNT(DISTINCT pid) as pid,
      APPROX_QUANTILES(down, 101)[SAFE_ORDINAL(51)] as down,
      APPROX_QUANTILES(up, 101)[SAFE_ORDINAL(51)] as up
    FROM main
    WHERE time_period = 'dec_2015'
    GROUP BY fips),
  jun_2016 as (
    SELECT
      fips,
      COUNT(DISTINCT pid) as pid,
      APPROX_QUANTILES(down, 101)[SAFE_ORDINAL(51)] as down,
      APPROX_QUANTILES(up, 101)[SAFE_ORDINAL(51)] as up
    FROM main
    WHERE time_period = 'jun_2016'
    GROUP BY fips),
  dec_2016 as (    
    SELECT
      fips,
      COUNT(DISTINCT pid) as pid,
      APPROX_QUANTILES(down, 101)[SAFE_ORDINAL(51)] as down,
      APPROX_QUANTILES(up, 101)[SAFE_ORDINAL(51)] as up
    FROM main
    WHERE time_period = 'dec_2016'
    GROUP BY fips),
  jun_2017 as (
    SELECT
      fips,
      COUNT(DISTINCT pid) as pid,
      APPROX_QUANTILES(down, 101)[SAFE_ORDINAL(51)] as down,
      APPROX_QUANTILES(up, 101)[SAFE_ORDINAL(51)] as up
    FROM main
    WHERE time_period = 'jun_2017'
    GROUP BY fips)

SELECT
  main.fips as county_fips,
  COUNT(DISTINCT main.pid) as provider_count,
  APPROX_QUANTILES(main.down, 101)[SAFE_ORDINAL(51)] as advertised_down,
  APPROX_QUANTILES(main.up, 101)[SAFE_ORDINAL(51)] as advertised_up,
  # At most one of any sliced aggregate will be non-null.
  # MAX is used here since COALESCE doesn't support resultset input. 
  MAX(dec_2014.pid) provider_count_dec_2014,
  MAX(dec_2014.down) down_dec_2014,
  MAX(dec_2014.up) up_dec_2014,
  MAX(jun_2015.pid) provider_count_jun_2015,
  MAX(jun_2015.down) down_jun_2015,
  MAX(jun_2015.up) up_jun_2015,
  MAX(dec_2015.pid) provider_count_dec_2015,
  MAX(dec_2015.down) down_dec_2015,
  MAX(dec_2015.up) up_dec_2015,
  MAX(jun_2016.pid) provider_count_jun_2016,
  MAX(jun_2016.down) down_jun_2016,
  MAX(jun_2016.up) up_jun_2016,
  MAX(dec_2016.pid) provider_count_dec_2016,
  MAX(dec_2016.down) down_dec_2016,
  MAX(dec_2016.up) up_dec_2016,
  MAX(jun_2017.pid) provider_count_jun_2017,
  MAX(jun_2017.down) down_jun_2017,
  MAX(jun_2017.up) up_jun_2017
FROM main
LEFT JOIN dec_2014 using (fips)
LEFT JOIN jun_2015 using (fips)
LEFT JOIN dec_2015 using (fips)
LEFT JOIN jun_2016 using (fips)
LEFT JOIN dec_2016 using (fips)
LEFT JOIN jun_2017 using (fips)
GROUP BY fips;