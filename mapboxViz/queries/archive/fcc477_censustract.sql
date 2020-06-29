#standardSQL
WITH fccdata AS (
	SELECT *, 'dec_2014' AS time_period FROM `mlab-sandbox.fcc.477_dec_2014`
	UNION ALL SELECT *, 'dec_2015' AS time_period FROM `mlab-sandbox.fcc.477_dec_2015`
	UNION ALL SELECT *, 'dec_2016' AS time_period FROM `mlab-sandbox.fcc.477_dec_2016`
	UNION ALL SELECT *, 'jun_2015' AS time_period FROM `mlab-sandbox.fcc.477_jun_2015`
	UNION ALL SELECT *, 'jun_2016' AS time_period FROM `mlab-sandbox.fcc.477_jun_2016`
	UNION ALL SELECT *, 'jun_2017' AS time_period FROM `mlab-sandbox.fcc.477_jun_2017`
),
districts AS (
  SELECT
    NAME,
    tract_polygons as WKT,
    FORMAT("%011d",GEOID) as geo_id
  FROM
    `mlab-sandbox.usa_geo.cb_2016_census_tracts`
),
fcc_groups AS (
  SELECT
      time_period,
      COUNT(DISTINCT Provider_ID) AS provider_count, 
  	  APPROX_QUANTILES(CAST(Max_Advertised_Downstream_Speed__mbps_ AS FLOAT64), 101)[SAFE_ORDINAL(51)] AS advertised_down,
  	  APPROX_QUANTILES(CAST(Max_Advertised_Upstream_Speed__mbps_ AS FLOAT64), 101)[SAFE_ORDINAL(51)] AS advertised_up,
      SUBSTR(Census_Block_FIPS_Code, 0, 11) as tract_id
  FROM fccdata
  WHERE Consumer = '1'
  GROUP BY tract_id, time_period 
),
fcc_timeslices AS (
  SELECT ARRAY_AGG(STRUCT(
    time_period,
    provider_count,
    advertised_down,
    advertised_up)) slice,
    tract_id
  FROM fcc_groups
  GROUP BY
  tract_id
)
SELECT fcc_timeslices.*, districts.* FROM fcc_timeslices JOIN districts ON (tract_id = geo_id);