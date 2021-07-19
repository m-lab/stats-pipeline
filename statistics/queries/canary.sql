CREATE OR REPLACE TABLE `mlab-sandbox.gfr.metro_stats_2021` 
PARTITION BY test_date
CLUSTER BY metro, site
AS

WITH
# This is the subset of data needed for the analysis.  It consists of about two
# weeks of data, with metro, machine, and site broken out.
primary AS (
SELECT date AS test_date,
REGEXP_EXTRACT(parser.ArchiveURL, ".*-(mlab[1-4])-.*") AS machine, 
REGEXP_EXTRACT(parser.ArchiveURL, ".*-mlab[1-4]-([a-z]{3}[0-9]{2}).*") AS site, 
REGEXP_EXTRACT(parser.ArchiveURL, ".*-mlab[1-4]-([a-z]{3})[0-9]{2}.*") AS metro, 
TIMESTAMP_DIFF(raw.Download.EndTime, raw.Download.StartTime, MICROSECOND)/1000000 AS duration,
raw.clientIP,
raw.Download.UUID, # Use this instead of id or a.UUID to ensure we are only using Downloads (TODO??)
# Consider adding client-server geo distance and country
a.* EXCEPT(UUID)
FROM `measurement-lab.ndt.ndt7`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY) # PARTITIONED on this (test_date)
AND raw.Download IS NOT NULL
),

hours_per_machine AS ( 
SELECT 
  test_date, 
  TIMESTAMP_TRUNC(TestTime, HOUR) AS hour, 
  COUNT(uuid) AS tests, metro, site, machine,
FROM primary
# Without this, the query costs goes up dramatically.
WHERE test_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY) 
GROUP BY test_date, hour, machine, site, metro ),

hours_per_day_per_machine AS (
SELECT 
  * EXCEPT(hour, tests),
  COUNT(hour) AS hours, SUM(tests) AS tests,  # TODO - sometimes see 25 hours!!
FROM hours_per_machine 
GROUP BY test_date, machine, site, metro 
), 

good_machines_per_metro AS (
SELECT 
  * EXCEPT(machine, hours, site, tests), 
  COUNT(machine) AS metro_machines, SUM(hours) AS metro_hours, 
  COUNTIF(hours = 24) AS good_machines, SUM(IF(hours = 24, hours, 0)) AS good_hours, 
  ARRAY_AGG( STRUCT( site, machine, tests, hours, hours = 24 AS good ) ORDER BY site, machine) AS machines, 
FROM hours_per_day_per_machine 
GROUP BY test_date, metro ), 

--------------------------------------------------------------------------- 

tests_per_client AS (
SELECT 
  test_date, TestTime, 
  metro, site, machine,
  clientIP AS client,
  UUID, # From Download
#  MeanThroughputMbps AS mbps, 
FROM primary 
WHERE duration BETWEEN 9 and 13
),

--------------------------------------------------------------------- 
# Count tests per machine, and join with hours per machine

machine_summary AS (
SELECT 
  tests_per_client.* EXCEPT(TestTime, uuid),
  --test_date, metro, site, machine, client, 
  COUNT(DISTINCT uuid) AS tests,
  machine_hours.* EXCEPT(metro, site, machine, test_date, tests) 
FROM tests_per_client LEFT JOIN hours_per_day_per_machine machine_hours
ON (tests_per_client.metro = machine_hours.metro AND 
    tests_per_client.site = machine_hours.site AND 
    tests_per_client.machine = machine_hours.machine AND 
    tests_per_client.test_date = machine_hours.test_date)
GROUP BY metro, site, machine, client, test_date, hours 
), 

# This will be very expensive. 
# This should create a lot of empty rows, for clients that appear in metro, but not on a machine. 
with_hours AS ( 
SELECT machine_summary.*, good_machines_per_metro.* EXCEPT(test_date, metro, machines) 
FROM good_machines_per_metro LEFT JOIN machine_summary 
ON (machine_summary.metro = good_machines_per_metro.metro AND machine_summary.test_date = good_machines_per_metro.test_date) 
),

# Not clear if this is actually useful as aggregate.
metro_summary AS (
SELECT 
  CURRENT_DATE() AS update_time, test_date, metro, client, 
  ARRAY_AGG( STRUCT( site, machine, tests ) ORDER BY site, machine) AS machines, 
  metro_machines, metro_hours, good_machines, good_hours, 
FROM with_hours 
GROUP BY metro, client, test_date, good_hours, metro_hours, good_machines, metro_machines
),

-------------------------------------------------------------- 

# All metros, 7 dates takes about 1 slot hour, produces 2M rows of good clients.
# CROSS JOIN produces about 150M rows.

# flatten the per metro data, so that we have a row for each machine/client/date
metro_machine_summary AS (
SELECT test_date, metro, client, CONCAT(site, ".", machine) AS machine, tests
FROM metro_summary JOIN UNNEST(metro_summary.machines)
GROUP BY test_date, metro, client, site, machine, tests
),

# extract complete list of machine per metro/date
machine_hours AS (
SELECT test_date, metro, machine
FROM metro_machine_summary
GROUP BY test_date, metro, machine
),

# extract complete list of clients per metro/date
clients AS (
SELECT test_date, metro, client
FROM metro_machine_summary
WHERE client != ""
GROUP BY test_date, metro, client
),

# create a complete list of machine/client pairs per metro/date
# This is quite large - about 100M pairs worldwide for a one week window.
machine_clients AS (
SELECT machine_hours.test_date, machine_hours.metro, machine_hours.machine, clients.client
FROM machine_hours CROSS JOIN clients
WHERE machine_hours.metro = clients.metro AND machine_hours.test_date = clients.test_date
),

# Now join the machine/client pairs with the original metro_machine_summaryed data.
# This produces a full complement of rows for each client/metro/date.
joined AS (
SELECT machine_clients.test_date, machine_clients.metro, machine_clients.machine, machine_clients.client, IF(metro_machine_summary.tests IS NULL, 0, metro_machine_summary.tests) AS tests
FROM machine_clients LEFT JOIN metro_machine_summary ON  machine_clients.test_date = metro_machine_summary.test_date AND machine_clients.metro = metro_machine_summary.metro AND machine_clients.client = metro_machine_summary.client AND machine_clients.machine = metro_machine_summary.machine
),

---------------------------------------------------------

# Now aggregate over the past week, to produce machine_summary complete distribution of tests
# per client across all machine_hours in each metro.
past_week AS (
SELECT
  test_date AS date, metro, machine, client,
  SUM(tests) OVER weekly_window AS weekly_tests,
  MIN(test_date) OVER weekly_window AS min_date,
FROM joined
GROUP BY date, metro, client, machine, tests
WINDOW weekly_window AS (
  PARTITION BY client, machine
  ORDER BY UNIX_DATE(test_date) RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
)
),

# Now summarize the data for each client/metro
weekly_summary AS (
SELECT
  date, metro, client,
  COUNTIF(weekly_tests > 0) AS test_machines,
  COUNT(machine) AS machines
,
  SUM(weekly_tests) total_tests, 
  MIN(min_date) AS min_date,
  # These count the number of machines with 0,1,2,3 or more tests
  # These are useful to determining whether the client is statistically well behaved
  MIN(weekly_tests) AS min,
  MAX(weekly_tests) AS max,
  COUNTIF(weekly_tests = 0) AS zeros,
  COUNTIF(weekly_tests = 1) AS ones,
  COUNTIF(weekly_tests = 2) AS twos,  
  COUNTIF(weekly_tests = 3) AS threes,
  COUNTIF(weekly_tests > 3) AS more,
FROM past_week
GROUP BY date, metro, client
),

-------------------------------------------------------------- 

# Metro stats
# Order:
#  2020-06-06 ndt5 per machine client stats, last 10 days
#  2020-06-07 Client stats
#  2020-06-16 Metro stats


# Ideally, this should be based on binomial distribution likelihood.
# However, for now Im using machine_summary simpler criteria that is sub-optimal.
# This could also be machine_summary sliding window partition if we want to compute multiple dates.
good_clients AS (
SELECT * FROM weekly_summary # mlab-sandbox.gfr.client_weekly_stats
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY)
AND client NOT IN
        ("45.56.98.222", "35.192.37.249", "35.225.75.192", "23.228.128.99",
        "2600:3c03::f03c:91ff:fe33:819",  "2605:a601:f1ff:fffe::99")
# Exclude clients more than twice as many tests as machines in the metro
--AND total_tests < 2*machines
# Good clients will have similar counts across all machines
AND max <= min + SQRT(machines)  -- up to 3 machines -> max = 1, 4 machines -> max = 2, ALL 15 machines -> spread < 4
# If there are fewer tests than machines, we expect lots of singletons
--AND (total_tests > machines OR ones >= twos)
ORDER BY machines DESC, metro
),
--------------------------------------------------------------------------- 

downloads AS (
SELECT 
  test_date, TestTime, metro, site, machine,
  clientIP, uuid, #id AS uuid, #uuid
  CongestionControl AS cc,  #TODO
  #result.Control.MessageProtocol AS protocol,
  MeanThroughputMbps AS mbps, 
  #raw.Download[x].TCPInfo.Retransmits,  # empty/NULL
  MinRTT,       # empty/NULL
  #result.S2C.MinRTT AS appMinRTT,
  #result.S2C.SumRTT, result.S2C.CountRTT,  # empty/NULL
  #result.S2C.MaxRTT AS appMaxRTT,          # empty/NULL
  duration, #  TIMESTAMP_DIFF(result.S2C.EndTime, result.S2C.StartTime, MICROSECOND)/1000000 AS test_duration,
  #result.S2C.Error != "" AS error,
  MeanThroughputMbps <= 0.1 AS slow,
  duration BETWEEN 9 AND 13 AS complete,
FROM primary 
),

-------------------------------------------------------------- 

# Good downloads should include only those clients that meet the good_client criteria.
good_downloads AS (
SELECT D.*
FROM downloads D JOIN good_clients G ON D.clientIP = G.client AND D.metro = G.metro AND D.test_date = G.date
),

stats AS (
SELECT test_date, metro, site, machine, complete, slow, count(uuid) AS tests, 
ROUND(EXP(AVG(IF(mbps > 0, LN(mbps), NULL))),2) AS log_mean_speed, 
ROUND(SAFE_DIVIDE(COUNTIF(MinRTT < 10000000), COUNT(uuid)),3) AS rttUnder10,  # Using MinRTT instead of appMinRTT here and below
ROUND(APPROX_QUANTILES(MinRTT, 101)[OFFSET(50)]/1000000,3) AS medianMinRTT,
ROUND(AVG(MinRTT)/1000000,3) AS meanMinRTT,
ROUND(EXP(AVG(IF(MinRTT > 0, LN(MinRTT/1000000), 0))),3) AS logMeanMinRTT,
# AVG(Retransmits) AS avgRetransmits,
# Pearson correlation between ln(minRTT) and ln(bandwidth).  Ln produces much higher correlation (.5 vs .3)
# suggesting that the long tail of high speed / low RTT undermines the correlation without the LOG.
ROUND(CORR(IF(MinRTT > 0, LN(1/MinRTT), NULL) , IF(mbps > 0, LN(mbps), NULL)), 3) AS pearson,
--ROUND(AVG(SAFE_DIVIDE(SumRTT,CountRTT))/1000000,2) AS meanAppAvgRTT,
ROUND(APPROX_QUANTILES(mbps, 101)[OFFSET(10)],2) AS q10,
ROUND(APPROX_QUANTILES(mbps, 101)[OFFSET(25)],2) AS q25,
ROUND(APPROX_QUANTILES(mbps, 101)[OFFSET(50)],2) AS q50,
ROUND(APPROX_QUANTILES(mbps, 101)[OFFSET(75)],2) AS q75,
ROUND(APPROX_QUANTILES(mbps, 101)[OFFSET(90)],2) AS q90,
ROUND(MAX(mbps),2) AS max,
ROUND(SAFE_DIVIDE(COUNTIF(mbps < 1), COUNT(uuid)),3) AS under_1,
ROUND(SAFE_DIVIDE(COUNTIF(mbps BETWEEN 1 AND 3), COUNT(uuid)),3) AS _1_3,
ROUND(SAFE_DIVIDE(COUNTIF(mbps BETWEEN 3 AND 10), COUNT(uuid)),3) AS _3_10,
ROUND(SAFE_DIVIDE(COUNTIF(mbps BETWEEN 10 AND 30), COUNT(uuid)),3) AS _10_30,
ROUND(SAFE_DIVIDE(COUNTIF(mbps BETWEEN 30 AND 100), COUNT(uuid)),3) AS _30_100,
ROUND(SAFE_DIVIDE(COUNTIF(mbps BETWEEN 100 AND 300), COUNT(uuid)),3) AS _100_300,
ROUND(SAFE_DIVIDE(COUNTIF(mbps > 300), COUNT(uuid)),3) AS over_300,
COUNTIF(MinRTT > 50000000) AS far,
ROUND(EXP(AVG(IF(MinRTT > 50000000 AND mbps > 0, LN(mbps), NULL))),3) AS logMeanFarMbps,
FROM good_downloads
GROUP BY metro, test_date, machine, site, complete, slow
)

SELECT * FROM stats

