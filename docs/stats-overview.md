# What Statistics are Provided by `stats-pipeline`, and How are They Calculated?
This service uses the M-Lab team's best, current understanding and recommended
techniques for aggregating data in the NDT dataset. Below we provide a general
text description of the approaches used by all queries, and snippets of one
query to illustrate how they are accomplished in BigQuery SQL.

The general approach currently used in the queries `stats-pipeline` uses to
generate statistics:

## Establish a set of LOG scale "buckets" within which measurement test results will be grouped
  
Bucketing or grouping results is fairly common. Think of the buckets used in
`stats-pipeline` as "speed" ranges, where all measurements used in the aggregation will
fall within one of the ranges. The fraction of measurements within each bucket
in a single day and geography make up the "histogram" for that day in that
geography.
  
The snippet of SQL below produces our histogram buckets:
```~sql
WITH
--Generate equal sized buckets in log-space between near 0 Mbps and ~1 Gbps+
buckets AS (
SELECT POW(10, x-.25) AS bucket_min, POW(10,x+.25) AS bucket_max
FROM UNNEST(GENERATE_ARRAY(0, 3.5, .5)) AS x
),```

returning 8 buckets with the following ranges:

```
**bucket_min**       **bucket_max**
0.56234132519034907  1.7782794100389228
1.7782794100389228   5.6234132519034912
5.6234132519034912   17.782794100389228
17.782794100389228   56.234132519034908
56.234132519034908   177.82794100389228
177.82794100389228   562.341325190349
562.341325190349     1778.2794100389228
1778.2794100389228   5623.4132519034911
```

## Select the initial set of tests and filter out those that may not be properly annotated.
Each query in `stats-pipeline` gathers test rows identified between two dates and within a geographic level.

```
--Select the initial set of tests
dl_per_location AS (
  SELECT
    date,
    client.Geo.ContinentCode AS continent_code,
    NET.SAFE_IP_FROM_STRING(Client.IP) AS ip,
    id,
    a.MeanThroughputMbps AS mbps,
    a.MinRTT AS MinRTT
  FROM `measurement-lab.ndt.unified_downloads`
  WHERE date BETWEEN @startdate AND @enddate
  AND a.MeanThroughputMbps != 0
),
--Filter for only tests With good locations and valid IPs
dl_per_location_cleaned AS (
  SELECT * FROM dl_per_location
  WHERE
    continent_code IS NOT NULL
    AND continent_code != ""
    AND ip IS NOT NULL
),
```

## Fingerprint all cleaned tests, and sort in an arbitrary, but repeatable order
By using the FARM_FINGERPRINT function, an arbitrary fingerprint is assigned to
each row. Sorting on the fingerprint, along with the random selection in the
next section effectively randomizes the set used to aggregate our statistics.

```
--Fingerprint all cleaned tests, in an arbitrary but repeatable order
dl_fingerprinted AS (
  SELECT
    date,
      continent_code,
      ip,
      ARRAY_AGG(STRUCT(ABS(FARM_FINGERPRINT(id)) AS ffid, mbps, MinRTT) ORDER BY ABS(FARM_FINGERPRINT(id))) AS members
  FROM dl_per_location_cleaned
  GROUP BY date, continent_code, ip
),
```

## Select two random rows for each IP using a prime number larger than the total number of tests

```
dl_random_ip_rows_perday AS (
  SELECT
    date,
    continent_code,
    ip,
    ARRAY_LENGTH(members) AS tests,
    members[SAFE_OFFSET(MOD(511232941,ARRAY_LENGTH(members)))] AS random1,
    members[SAFE_OFFSET(MOD(906686609,ARRAY_LENGTH(members)))] AS random2
  FROM dl_fingerprinted
),
```

## Calculate log averages and statistics per day from random samples 

```
dl_stats_per_day AS (
  SELECT
    date, continent_code,
    COUNT(*) AS dl_samples_day,
    ROUND(POW(10,AVG(Safe.LOG10(random1.mbps))),3) AS dl_LOG_AVG_rnd1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.mbps))),3) AS dl_LOG_AVG_rnd2,
    ROUND(POW(10,AVG(Safe.LOG10(random1.MinRtt))),3) AS dl_minRTT_LOG_AVG_rnd1,
    ROUND(POW(10,AVG(Safe.LOG10(random2.MinRtt))),3) AS dl_minRTT_LOG_AVG_rnd2,
    ROUND(MIN(random1.mbps),3) AS download_MIN,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(25)],3) AS download_Q25,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(50)],3) AS download_MED,
    ROUND(AVG(random1.mbps),3) AS download_AVG,
    ROUND(APPROX_QUANTILES(random1.mbps, 100) [SAFE_ORDINAL(75)],3) AS download_Q75,
    ROUND(MAX(random1.mbps),3) AS download_MAX,
    ROUND(APPROX_QUANTILES(random1.MinRTT, 100) [SAFE_ORDINAL(50)],3) AS download_minRTT_MED,
  FROM dl_random_ip_rows_perday
  GROUP BY continent_code, date
),
```

## Count the samples that fall into each bucket and get frequencies for the histogram

```
dl_histogram AS (
  SELECT
    date,
    continent_code,
    --Set the lowest bucket's min to zero, so all tests below the generated min of the lowest bin are included. 
    CASE WHEN bucket_left = 0.5623413251903491 THEN 0
    ELSE bucket_left END AS bucket_min,
    bucket_right AS bucket_max,
    COUNTIF(random1.mbps < bucket_right AND random1.mbps >= bucket_left) AS dl_samples_bucket,
    ROUND(COUNTIF(random1.mbps < bucket_right AND random1.mbps >= bucket_left) / COUNT(*), 3) AS dl_frac_bucket
  FROM dl_random_ip_rows_perday CROSS JOIN buckets
  GROUP BY
    date,
    continent_code,
    bucket_min,
    bucket_max
),
```
