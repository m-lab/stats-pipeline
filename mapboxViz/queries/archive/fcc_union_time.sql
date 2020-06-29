#standardSQL
SELECT *, 'dec_2014' AS time_period FROM `mlab-sandbox.fcc.477_dec_2014`
UNION ALL SELECT *, 'dec_2015' AS time_period FROM `mlab-sandbox.fcc.477_dec_2015`
UNION ALL SELECT *, 'dec_2016' AS time_period FROM `mlab-sandbox.fcc.477_dec_2016`
UNION ALL SELECT *, 'jun_2015' AS time_period FROM `mlab-sandbox.fcc.477_jun_2015`
UNION ALL SELECT *, 'jun_2016' AS time_period FROM `mlab-sandbox.fcc.477_jun_2016`
UNION ALL SELECT *, 'jun_2017' AS time_period FROM `mlab-sandbox.fcc.477_jun_2017`