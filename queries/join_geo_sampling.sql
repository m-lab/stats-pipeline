SELECT d.*, g.geo_id, g.state_fips_code, g.state_name, g.county_fips_code, g.county_name, g.tract_ce, g.blockgroup_ce, g.lsad_name, g.mtfcc_feature_class_code, g.functional_status, g.area_land_meters, g.area_water_meters, g.blockgroup_geom AS WKT
FROM `mlab-interns-2020.mlab_pipeline.fcc_june19_ws_bcstr` d
INNER JOIN `bigquery-public-data.geo_census_blockgroups.us_blockgroups_national` g
ON g.geo_id = SUBSTR(d.BlockCode, 0, 12)
WHERE RAND() < FRACTION
