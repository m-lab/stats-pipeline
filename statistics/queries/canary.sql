# This query computes client
# To invoke from command line:
# bq query --use_legacy_sql=false --parameter='startdate:DATE:2021-07-15' --parameter='enddate:DATE:2021-09-13' --destination_table='mlab-sandbox:statistics.gfr_stats2' --time_partitioning_type=DAY --time_partitioning_field=test_date --replace  < statistics/queries/canary_incorp_numbered.sql

# TODO - identify slow and far clients, instead of individual tests.
# Otherwise, we are filtering out potentially important outlier tests, instead of weird clients.

------------------------------------------------------------------------
# NOTE: This block (all_tests and numbered_ndt7) will soon be broken out into a public view
# and documented in a blog post.  This query will be updated at that time to use the public view.
#
# This decorates all NDT7 tests with additional fields:
#  1. A synthetic ClientID
#  2. Top level ServerMeasurements (either Upload or Download)
#  3. NDTVersion, isDownload
#  4. Additional fields in the client struct: Name, OS, Arch, Version, Library, LibraryVersion
#. 4. Top level lastSnapshot (which may be NULL if there are no ServerMeasurements)
#  5. Synthetic sequence numbers and counts per machine (machineSample/machineCount), site, and metro, to simplify client de-biasing.
#  6. Top level elapsedTime based on last ServerMeasurement TCPInfo.ElapseTime value (or NULL if ServerMeasurements is empty).

WITH all_tests AS (
  SELECT ID, date, parser, 
  (SELECT AS STRUCT server.*, LEFT(server.Site,3) AS Metro) AS server,
  (SELECT AS STRUCT client.*, 
    raw.clientIP IN
      ("45.56.98.222", "35.192.37.249", "35.225.75.192", "23.228.128.99",
      "2600:3c03::f03c:91ff:fe33:819",  "2605:a601:f1ff:fffe::99") AS isMonitoring,
    (SELECT Value
     FROM UNNEST(IFNULL(raw.Download.ClientMetadata, raw.Upload.ClientMetadata))
     WHERE Name='client_name') AS Name,
    (SELECT Value
     FROM UNNEST(IFNULL(raw.Download.ClientMetadata, raw.Upload.ClientMetadata))
     WHERE Name='client_os') AS OS,
    (SELECT Value
     FROM UNNEST(IFNULL(raw.Download.ClientMetadata, raw.Upload.ClientMetadata))
     WHERE Name='client_arch') AS Arch,
    (SELECT Value
     FROM UNNEST(IFNULL(raw.Download.ClientMetadata, raw.Upload.ClientMetadata))
     WHERE Name='client_library') AS Library,
    (SELECT Value
     FROM UNNEST(IFNULL(raw.Download.ClientMetadata, raw.Upload.ClientMetadata))
     WHERE Name='client_version') AS Version,
    (SELECT Value
     FROM UNNEST(IFNULL(raw.Download.ClientMetadata, raw.Upload.ClientMetadata))
     WHERE Name='client_library_version') AS LibraryVersion,
    IFNULL(IFNULL(raw.Download.ServerMeasurements, raw.Upload.ServerMeasurements)[SAFE_OFFSET(0)].TCPInfo.wscale,-1) AS WScale,
  ) AS client,
  * EXCEPT(ID, date, parser, server, client),
  
  IF(raw.Download IS NULL, false, true) AS isDownload,
  IFNULL(raw.Download.ServerMeasurements, raw.Upload.ServerMeasurements) AS ServerMeasurements,
  REGEXP_EXTRACT(ID, "(ndt-?.*)-.*") AS NDTVersion,
  FROM `measurement-lab.ndt.ndt7`
),

numbered_ndt7 AS (
SELECT *,
  # This is an experimental ClientID, intended for exploration of how well it functions as a substitute for ClientIP.
  FORMAT("%16X",FARM_FINGERPRINT(FORMAT("%s_%s_%s_%s_%s_%s_%s_%02x",
    IFNULL(raw.ClientIP,"none"), IFNULL(client.Name,"none"), IFNULL(client.OS,"none"),
    IFNULL(client.Arch, "none"), IFNULL(client.Version, "none"), IFNULL(client.Library, "none"), IFNULL(client.LibraryVersion, "none"),
    IFNULL(ServerMeasurements[SAFE_OFFSET(0)].TCPInfo.wscale,-1)))) AS ClientID,
  # This is a copy of the last snapshot, to make it more easily accessible, independent of whether it is an Upload or Download
  ServerMeasurements[SAFE_ORDINAL(ARRAY_LENGTH(ServerMeasurements))] AS lastSnapshot,
  # This may be a more useful elapsedTime measurement, since it reflects the available measurements.
  ServerMeasurements[SAFE_ORDINAL(ARRAY_LENGTH(ServerMeasurements))].TCPInfo.ElapsedTime/1000000 AS elapsedTime,

  # This assigns sample sequence numbers to each sample for a client on a machine.
  # The xxxCount can be used in a MOD function to choose samples other than Sample = 1
  ROW_NUMBER() OVER machineOrder AS machineSample, 
  COUNT(*) OVER machinePart AS machineCount, #(PARTITION BY date, server.Site, server.Machine, raw.ClientIP, client.Name, client.OS, client.Version, client.Arch, client.Library, client.LibraryVersion, ServerMeasurements[SAFE_OFFSET(0)].TCPInfo.wscale, NDTVersion, isDownload) AS machineCount,
  ROW_NUMBER() OVER siteOrder AS siteSample,
  COUNT(*) OVER sitePart AS siteCount, #(PARTITION BY date, server.Site, raw.ClientIP, client.Name, client.OS, client.Version, client.Arch, client.Library, client.LibraryVersion, ServerMeasurements[SAFE_OFFSET(0)].TCPInfo.wscale, NDTVersion, isDownload) AS siteCount,
  ROW_NUMBER() OVER metroOrder AS metroSample, 
  COUNT(*) OVER metroPart AS metroCount, #(PARTITION BY date, server.Metro, raw.ClientIP, client.Name, client.OS, client.Version, client.Arch, client.Library, client.LibraryVersion, ServerMeasurements[SAFE_OFFSET(0)].TCPInfo.wscale, NDTVersion, isDownload) AS metroCount,
FROM all_tests
WINDOW
  # Add random test numbers, per client/day, for uploads and downloads separately, and separately for each NDTVersion
  # These are ordered by ARRAY_LENGTH(ServerMeasurements) DESC to prefer longer tests.
  machinePart AS (PARTITION BY date, server.Site, server.Machine, raw.ClientIP, client.Name, client.OS, client.Version, client.Arch, client.Library, client.LibraryVersion, ServerMeasurements[SAFE_OFFSET(0)].TCPInfo.wscale, NDTVersion, isDownload),
  machineOrder AS (machinePart ORDER BY ARRAY_LENGTH(ServerMeasurements) DESC, FARM_FINGERPRINT(ID)),
  sitePart AS (PARTITION BY date, server.Site, raw.ClientIP, client.Name, client.OS, client.Version, client.Arch, client.Library, client.LibraryVersion, ServerMeasurements[SAFE_OFFSET(0)].TCPInfo.wscale, NDTVersion, isDownload),
  siteOrder AS (sitePart ORDER BY ARRAY_LENGTH(ServerMeasurements) DESC, FARM_FINGERPRINT(ID)),
  metroPart AS (PARTITION BY date, server.Metro, raw.ClientIP, client.Name, client.OS, client.Version, client.Arch, client.Library, client.LibraryVersion, ServerMeasurements[SAFE_OFFSET(0)].TCPInfo.wscale, NDTVersion, isDownload),
  metroOrder AS (metroPart ORDER BY ARRAY_LENGTH(ServerMeasurements) DESC, FARM_FINGERPRINT(ID))
),
------------------------------------------------------------------------

tests AS (
  SELECT * EXCEPT(a), a.* FROM numbered_ndt7
  WHERE NOT client.isMonitoring
),

subset AS (
SELECT
  date AS test_date, TestTime, server.metro, server.site, server.machine, Client, Server,
  ClientID,
  NDTVersion,
  UUID,
  CongestionControl AS cc,
  MeanThroughputMbps AS mbps,
  lastSnapshot.TCPInfo.Retransmits,
  MinRTT, # in msec
  LossRate,
  elapsedTime,
  MeanThroughputMbps <= 0.1 AS slow,
  elapsedTime BETWEEN 7 AND 10.1 AS complete, # Only filters out slower tests Using elapsedTime from tcpinfo snapshot
  IF(NDTVersion LIKE "%canary%", (siteCount > 3 OR metroCount > 9), (siteCount > 6 OR metroCount > 18)) AS isHot,
FROM tests
WHERE isDownload
  AND (machineSample = 1 # only allow one sample per day per client/machine.
    OR (machineSample = 2 AND client.WScale = 0x78)) # Allow two samples per machine from 0x78 clients.
),

--------------------------------------------------------------

stats AS (
SELECT test_date, CURRENT_DATE() AS compute_date, 
Client.Geo.countryName,
Client.Network.ASName,
metro, site, machine, 
client.Name AS clientName,
client.OS AS clientOS,
isHot AS clientIsHot,
FORMAT("%02X", client.WScale) AS clientWScale,
NDTVersion, complete, slow, COUNT(DISTINCT ClientID) AS clients, count(uuid) AS tests, 
ROUND(EXP(AVG(IF(mbps > 0, LN(mbps), NULL))),2) AS log_mean_speed, 
# ndt7 has only TCPINFO MinRTT, and reports in microseconds??  Using MinRTT instead of appMinRTT here and below
# ndt5 was reporting in nanoseconds??
ROUND(SAFE_DIVIDE(COUNTIF(MinRTT < 10), COUNT(uuid)),3) AS rttUnder10msec,
ROUND(APPROX_QUANTILES(MinRTT, 101)[OFFSET(50)],3) AS medianMinRTT,
ROUND(AVG(IF(MinRTT<10000,MinRTT,NULL)),3) AS meanMinRTT, # Ignore insane values over 10 seconds
ROUND(EXP(AVG(IF(MinRTT > 0, LN(MinRTT), 0))),3) AS logMeanMinRTT,
AVG(LossRate) AS AvgLossRate,
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
COUNTIF(MinRTT > 50) AS far,
ROUND(EXP(AVG(IF(MinRTT > 50 AND mbps > 0, LN(mbps), NULL))),3) AS logMeanFarMbps,
FROM subset
GROUP BY test_date, countryName, ASName, metro, site, machine, NDTVersion, 
client.Name, client.OS, client.WScale, isHot,
complete, slow
)

SELECT * FROM stats WHERE test_date BETWEEN @startdate AND @enddate
