-- Generate a synthetic hopannotation1 annotation from base_tables.traceroute.

WITH annotations AS (
    SELECT *
    FROM `{{ .project }}.raw_ndt.hopannotation1`
    WHERE date BETWEEN
        DATE_SUB(DATE('{{ .partitionID }}'), INTERVAL 1 DAY)
      AND DATE_ADD(DATE('{{ .partitionID }}'), INTERVAL 1 DAY)
), traceroutes AS (
    SELECT *
    FROM `{{ .project }}.base_tables.traceroute`
    WHERE DATE(TestTime) = DATE('{{ .partitionID }}')
        AND DATE('{{ .partitionID }}') BETWEEN
            DATE_SUB(DATE(_PARTITIONTIME), INTERVAL 1 DAY)
        AND DATE_ADD(DATE(_PARTITIONTIME), INTERVAL 1 DAY)
)

SELECT
-- The below fields make up the content of the hopannotation1 file and are written to disk.
    hop.Source.hopannotation1.ID,
    FORMAT_TIMESTAMP("%FT%TZ", MIN(hop.Source.hopannotation1.Timestamp)) AS Timestamp,
    ANY_VALUE(hop.Source.hopannotation1.Annotations) AS Annotations,
-- The below fields are used to construct the file path and name.
    REPLACE(CAST(DATE(hop.Source.hopannotation1.Timestamp) AS STRING), "-", "/") AS Date,
    FORMAT_TIMESTAMP("%Y%m%dT000000Z", MIN(hop.Source.hopannotation1.Timestamp)) AS FilenameTimestamp,
    REGEXP_EXTRACT(hop.Source.hopannotation1.ID, r".+_(.+)_.+") AS Hostname,
    ANY_VALUE(hop.Source.IP) AS IP
FROM
    traceroutes AS traceroute, UNNEST(Hop) AS hop
LEFT OUTER JOIN
   annotations AS annotation
ON
    hop.Source.hopannotation1.ID = annotation.id
WHERE annotation.id IS NULL
    AND hop.Source.hopannotation1.ID != ""
    AND hop.Source.hopannotation1.ID IS NOT NULL
    AND hop.Source.hopannotation1.Annotations IS NOT NULL
GROUP BY hop.Source.hopannotation1.ID, Date
