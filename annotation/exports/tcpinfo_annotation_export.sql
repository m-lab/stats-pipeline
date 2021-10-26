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
SELECT
    UUID,
    MIN(TestTime) as Timestamp,
    ANY_VALUE(ServerX) as Server,
    ANY_VALUE(ClientX) as Client,
    REPLACE(CAST(DATE(TestTime) AS STRING), "-", "/") as year_month_day,
FROM `mlab-oti.base_tables.tcpinfo`
{{ .whereClause }}
    AND UUID != "" AND UUID is not NULL AND UUID NOT IN (
        SELECT id FROM `mlab-oti.raw_ndt.annotation`
    )
    AND ServerX.Site != ""
    AND ServerX.Geo is not NULL
GROUP BY
    UUID,
    year_month_day
