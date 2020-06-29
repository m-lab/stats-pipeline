CREATE TEMP FUNCTION
  weighted_median(data ARRAY<STRUCT<measure FLOAT64, weight FLOAT64>>) AS ((
    WITH calculated AS (SELECT
      measure,
      (SUM(weight) OVER above) - weight AS weight_above,
      (SUM(weight) OVER below) - weight AS weight_below
    FROM
      UNNEST(data)
    WINDOW
      above AS (
        ORDER BY measure
        RANGE BETWEEN CURRENT ROW
        AND UNBOUNDED FOLLOWING),
      below AS (
        ORDER BY measure 
        RANGE BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW)),
    max_of_lower_half AS (
      SELECT MAX(measure) as measure FROM calculated WHERE weight_below <= (SELECT SUM(weight) / 2 FROM UNNEST(data))
    ),
    min_of_upper_half AS (
      SELECT MIN(measure) as measure FROM calculated WHERE weight_above <= (SELECT SUM(weight) / 2 FROM UNNEST(data))
    )
    SELECT AVG(measure) FROM (SELECT measure FROM max_of_lower_half UNION ALL SELECT measure FROM min_of_upper_half)
  ));

WITH all_fcc AS (
    SELECT *, 'dec_2014' AS time_period
    FROM `mlab-sandbox.fcc.477_dec_2014`
    UNION ALL SELECT *, 'jun_2015' AS time_period
    FROM `mlab-sandbox.fcc.477_jun_2015`
    UNION ALL SELECT *, 'dec_2015' AS time_period
    FROM `mlab-sandbox.fcc.477_dec_2015`
    UNION ALL SELECT *, 'jun_2016' AS time_period
    FROM `mlab-sandbox.fcc.477_jun_2016`
    UNION ALL SELECT *, 'dec_2016' AS time_period
    FROM `mlab-sandbox.fcc.477_dec_2016`
    UNION ALL SELECT *, 'jun_2017' AS time_period
    FROM `mlab-sandbox.fcc.477_jun_2017`
    UNION ALL SELECT *, 'dec_2017' AS time_period
    FROM `mlab-sandbox.fcc.477_dec_2017` ),
fcc AS (
    SELECT
        * EXCEPT (Max_Advertised_Downstream_Speed__mbps_, Max_Advertised_Upstream_Speed__mbps_),
        CAST(Max_Advertised_Downstream_Speed__mbps_ AS FLOAT64) as advertised_down,
        CAST(Max_Advertised_Upstream_Speed__mbps_ AS FLOAT64) as advertised_up
    FROM all_fcc
    WHERE Consumer = '1'),
geo AS (
    SELECT
        name,
        state_name AS state,
        zcta_geom AS geom
    FROM `mlab-sandbox.usa_geo.us_zip_codes`),
blocks AS (
    SELECT
        FORMAT('%015d', geoid10) AS block_id,
        st_area(geom) as area,
        geom
    FROM `mlab-sandbox.usa_geo.cb_2016_census_blocks`),
geo_blocks AS (
    SELECT
        g.name,
        cb.block_id,
        ST_Area(ST_Intersection(g.geom, cb.geom)) / cb.area weight,
        g.geom
    FROM geo g
    JOIN blocks cb
        ON (ST_INTERSECTS(g.geom, cb.geom))),
fcc_geo AS (
    SELECT
        fcc.FRN,
        fcc.advertised_down as download,
        fcc.advertised_up as upload,
        fcc.time_period,
        g.weight,
        g.name,
        g.geom,
        g.block_id
    FROM fcc
    JOIN geo_blocks g
        ON (fcc.Census_Block_FIPS_Code = g.block_id)),
fcc_geo_provider_medians AS (
    SELECT 
        time_period,
        name,
        block_id
        FRN,
        APPROX_QUANTILES(download, 101 IGNORE NULLS)[SAFE_ORDINAL(50)] download,
        APPROX_QUANTILES(upload, 101 IGNORE NULLS)[SAFE_ORDINAL(50)] upload,
        ANY_VALUE(weight) weight,
        ANY_VALUE(geom) geom
        FROM fcc_geo
        GROUP BY time_period, name, block_id, FRN),
fcc_geo_median_of_medians AS (
    SELECT
    COUNT(DISTINCT FRN) provider_count,
    weighted_median(ARRAY_AGG(STRUCT(download, weight))) advertised_down,
    weighted_median(ARRAY_AGG(STRUCT(upload, weight))) advertised_up,
    time_period,
    name,
    ANY_VALUE(geom) AS geom
    FROM fcc_geo_provider_medians GROUP BY name, time_period
),
fcc_time_chunks AS (
    SELECT
      ARRAY_AGG(STRUCT(
          time_period,
          advertised_down,
          advertised_up
      )) slice,
      name,
      ANY_VALUE(geom) AS geom
    FROM fcc_geo_median_of_medians
    GROUP BY name
)
SELECT
  * EXCEPT (geom), geom
FROM
  fcc_time_chunks;