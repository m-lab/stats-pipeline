SELECT d.Provider_Id, d.DBAName, d.HoldingCompanyName, d.MaxAdDown, d.MaxAdUp, d.MaxCIRDown, d.MaxCIRUp, g.geo_id, g.area_land_meters, g.area_water_meters, g.blockgroup_geom AS WKT
FROM `mlab-interns-2020.mlab_pipeline.fcc_june19_ws_bcstr` d
INNER JOIN `bigquery-public-data.geo_census_blockgroups.us_blockgroups_national` g
ON g.geo_id = SUBSTR(d.BlockCode, 0, 12)
WHERE RAND() < FRACTION
