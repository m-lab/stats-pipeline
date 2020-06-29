SELECT
FROM `mlab-interns-2020`.mlab_pipeline.fcc_june19_ws_bcstr d
INNER JOIN `bigquery-public-data.geo_census_blockgroups.blockgroups_STATENO`
ON ca.GEOID = SUBSTR(d.BlockCode, 0, 12)
