SELECT continent_code, country_code, ISO3166_2region1, EXTRACT(YEAR from date) as year, date, 
ANY_VALUE(download_MIN) as download_MIN, ANY_VALUE(download_Q25) as download_Q25,
ANY_VALUE(download_MED) as download_MED, ANY_VALUE(download_AVG) as download_AVG, ANY_VALUE(download_Q75) as download_Q75,
ANY_VALUE(download_MAX) as download_MAX, ANY_VALUE(upload_MIN) as upload_MIN, ANY_VALUE(upload_Q25) as upload_Q25,
ANY_VALUE(upload_MED) as upload_MED, ANY_VALUE(upload_AVG) as upload_AVG, ANY_VALUE(upload_Q75) as upload_Q75,
ANY_VALUE(upload_MAX) as upload_MAX
FROM `mlab-sandbox.statistics.regions`
GROUP BY continent_code, country_code, ISO3166_2region1, year, date
ORDER BY continent_code, country_code, ISO3166_2region1, date