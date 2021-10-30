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
-- "annotation.id IS NULL").
--
-- TODO(soltesz): Allows the query to filter on annotation date partitions
-- to make the query more efficient than a global search.
SELECT
    tcpinfo.UUID,
    MIN(tcpinfo.TestTime) as Timestamp,
    ANY_VALUE(tcpinfo.ServerX) as Server,
    ANY_VALUE(tcpinfo.ClientX) as Client,
    REPLACE(CAST(DATE(tcpinfo.TestTime) AS STRING), "-", "/") as year_month_day,
FROM
    `mlab-oti.base_tables.tcpinfo` AS tcpinfo
LEFT OUTER JOIN
    `mlab-oti.raw_ndt.annotation` AS annotation
ON
        tcpinfo.UUID = annotation.id
    AND DATE(tcpinfo.TestTime) = annotation.date
{{ .whereClause }}
    AND annotation.id IS NULL
    AND tcpinfo.UUID != ""
    AND tcpinfo.UUID is not NULL
    AND tcpinfo.ServerX.Site != ""
    AND tcpinfo.ServerX.Geo is not NULL
GROUP BY
    UUID,
    year_month_day
