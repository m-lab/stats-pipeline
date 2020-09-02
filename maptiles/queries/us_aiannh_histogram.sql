WITH
# AIANNH - us_american_indian_alaska_native_areas_hawaiian_home_lands
aiannh AS (
  SELECT
    NAME AS name,
    WKT,
    GEOID
  FROM
    `measurement-lab.geographies.us_aiannh_2018`
),
# Select the initial set of results
per_location AS (
  SELECT
    test_date,
    aiannh.GEOID AS GEOID,
    aiannh.name AS aiannh_name,
    a.MeanThroughputMbps AS mbps,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip
  FROM `measurement-lab.ndt.unified_downloads` tests, aiannh
  WHERE test_date = @startday
  AND ST_WITHIN(
    ST_GeogPoint(
      client.Geo.longitude,
      client.Geo.latitude
    ), aiannh.WKT
  )
),
# With good locations and valid IPs
per_location_cleaned AS (
  SELECT
    test_date,
    GEOID,
    aiannh_name,
    mbps,
    ip          
  FROM per_location
  WHERE 
    GEOID IS NOT NULL
    AND ip IS NOT NULL
),
# Descriptive statistics per IP, per day 
max_per_day_ip AS (
  SELECT 
    test_date,
    GEOID,
    aiannh_name,
    ip,
    MAX(mbps) AS mbps
  FROM per_location_cleaned
  GROUP BY 
    test_date,
    GEOID,
    aiannh_name,
    ip
),
# Count the samples
sample_counts AS (
  SELECT 
    test_date,
    GEOID,
    aiannh_name,
    COUNT(*) AS samples
  FROM max_per_day_ip
  GROUP BY
    test_date,
    GEOID,
    aiannh_name
),
# Generate equal sized buckets in log-space
buckets AS (
  SELECT POW(10,x) AS bucket_right, POW(10, x-.2) AS bucket_left
  FROM UNNEST(GENERATE_ARRAY(0, 3, .2)) AS x
),
# Count the samples that fall into each bucket
histogram_counts AS (
  SELECT 
    test_date,
    GEOID,
    aiannh_name,
    bucket_left AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(mbps < bucket_right AND mbps >= bucket_left) AS bucket_count
  FROM max_per_day_ip CROSS JOIN buckets
  GROUP BY 
    test_date,
    GEOID,
    aiannh_name,
    bucket_min,
    bucket_max
  ORDER BY 
    test_date,
    GEOID, 
    bucket_min,
    bucket_max
),
# Turn the counts into frequencies
histogram AS (
  SELECT 
    test_date,
    GEOID,
    aiannh_name,
    bucket_min,
    bucket_max,
    bucket_count / samples AS frac,
    samples
  FROM histogram_counts 
  JOIN sample_counts USING (test_date, GEOID, aiannh_name)
)
# Show the results
SELECT * FROM histogram
ORDER BY test_date, GEOID, aiannh_name, bucket_min, bucket_max
