#standardSQL
select 
  SUBSTR(Census_Block_FIPS_Code, 0, 6) as county_fips, 
  COUNT(DISTINCT Provider_ID) AS provider_count, 
  APPROX_QUANTILES(CAST(Max_Advertised_Downstream_Speed__mbps_ AS FLOAT64), 101)[SAFE_ORDINAL(51)] AS advertised_down,
  APPROX_QUANTILES(CAST(Max_Advertised_Upstream_Speed__mbps_ AS FLOAT64), 101)[SAFE_ORDINAL(51)] AS advertised_up,
  #ARRAY_AGG(distinct Provider_Name) as providers,
  time_period
FROM `mlab-sandbox.georgia_usb.fcc_477_dec2014_jun_2017` 
where
  Consumer = '1'
GROUP BY county_fips, time_period