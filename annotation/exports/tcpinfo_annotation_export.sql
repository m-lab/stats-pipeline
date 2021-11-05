-- Generate a synthetic UUID annotation from the base_tables.tcpinfo.
--
-- NOTES
--
-- For a single UUID, there may be multiple Timestamps per day in tcpinfo due to
-- some long lived connections (probably not actual NDT measurements). The
-- export query groups on the UUID and year_month_day to guarantee a single
-- UUID per day. The query uses any Server and Client annotation from that day
-- and the first (minimum) Timestamp.
--
-- This export query may be safely run multiple times *IF* the previously
-- generated annotations have been copied to the ndt/annotation GCS location and
-- parsed into the raw_ndt.annotation tables.
--
-- The query uses the "Left Excluding JOIN" pattern to select only rows from
-- TCPINFO *without* corresponding rows in the annotation table (i.e.
-- "annotation.id IS NULL"). Both tcpinfo and annotation tables are filtered by
-- partition dates.
WITH annotations AS (
    SELECT *
    FROM `{{ .project }}.raw_ndt.annotation`
    WHERE date BETWEEN
          DATE_SUB(DATE('{{ .partitionID }}'), INTERVAL 1 DAY)
      AND DATE_ADD(DATE('{{ .partitionID }}'), INTERVAL 1 DAY)
), tcpinfos AS (
    SELECT *
    FROM `{{ .project }}.base_tables.tcpinfo`
    WHERE DATE(TestTime) = DATE('{{ .partitionID }}')
      AND DATE('{{ .partitionID }}') BETWEEN
          DATE_SUB(DATE(_PARTITIONTIME), INTERVAL 1 DAY)
      AND DATE_ADD(DATE(_PARTITIONTIME), INTERVAL 1 DAY)
)

SELECT
    tcpinfo.UUID,
    MIN(tcpinfo.TestTime) AS Timestamp,
    ANY_VALUE(tcpinfo.ServerX) AS Server,
    ANY_VALUE(tcpinfo.ClientX) AS Client,
    REPLACE(CAST(DATE(tcpinfo.TestTime) AS STRING), "-", "/") AS year_month_day,
FROM
    tcpinfos AS tcpinfo
LEFT OUTER JOIN
    annotations AS annotation
ON
        tcpinfo.UUID = annotation.id
    AND DATE(tcpinfo.TestTime) = annotation.date
WHERE
        annotation.id IS NULL
    AND tcpinfo.UUID != ""
    AND tcpinfo.UUID IS NOT NULL
    AND tcpinfo.ServerX.Site != ""
    AND tcpinfo.ServerX.Geo IS NOT NULL
GROUP BY
    UUID,
    year_month_day
