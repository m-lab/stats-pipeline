#standardSQL
WITH districts AS (
  SELECT
    state_name,
    legal_area_name,
    district_geom,
    congress_district_code as geo_id
  FROM
    `mlab-sandbox.usa_geo.116_congress_district`
),
dl AS (
  SELECT
    tests.*,
    FORMAT(
      '%s_%d',
      ['jun', 'dec'] [ORDINAL(CAST(CEIL(EXTRACT(MONTH FROM partition_date) / 6) AS INT64))],
      EXTRACT(
        YEAR
        FROM
          partition_date
      )
    ) as time_period,
    districts.geo_id AS geo_id
  FROM
    `measurement-lab.release.ndt_downloads` tests
    JOIN districts ON ST_WITHIN(
      ST_GeogPoint(
        connection_spec.client_geolocation.longitude,
        connection_spec.client_geolocation.latitude
      ),
      district_geom
    )
  WHERE
    connection_spec.server_geolocation.country_name = "United States"
    AND (
      partition_date BETWEEN '2014-07-01'
      AND '2018-12-31'
    )
),
ul AS (
  SELECT
    tests.*,
    FORMAT(
      '%s_%d',
      ['jun', 'dec'] [ORDINAL(CAST(CEIL(EXTRACT(MONTH FROM partition_date) / 6) AS INT64))],
      EXTRACT(
        YEAR
        FROM
          partition_date
      )
    ) as time_period,
    districts.geo_id AS geo_id
  FROM
    `measurement-lab.release.ndt_uploads` tests
    JOIN districts ON ST_WITHIN(
      ST_GeogPoint(
        connection_spec.client_geolocation.longitude,
        connection_spec.client_geolocation.latitude
      ),
      district_geom
    )
  WHERE
    connection_spec.server_geolocation.country_name = "United States"
    AND (
      partition_date BETWEEN '2014-07-01'
      AND '2018-12-31'
    )
)
SELECT
  ARRAY(
    SELECT AS STRUCT
      time_period,
      COUNT(test_id) AS test_count,
      COUNT(DISTINCT connection_spec.client_ip) AS ip_count,
      APPROX_QUANTILES(
        8 * SAFE_DIVIDE(
          web100_log_entry.snap.HCThruOctetsAcked,
          (
            web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd
          )
        ),
        101
      ) [SAFE_ORDINAL(51)] AS tx_Mbps,
      APPROX_QUANTILES(
        CAST(web100_log_entry.snap.MinRTT AS FLOAT64),
        101
      ) [ORDINAL(51)] as min_rtt
    FROM
      dl
    WHERE dl.geo_id = districts.geo_id
    GROUP BY
      time_period
  ) dl,
  ARRAY(
    SELECT AS STRUCT
      time_period,
      COUNT(test_id) AS test_count,
      COUNT(DISTINCT connection_spec.client_ip) AS ip_count,
      APPROX_QUANTILES(
        8 * SAFE_DIVIDE(
          web100_log_entry.snap.HCThruOctetsReceived,
          (
            web100_log_entry.snap.Duration
          )
        ),
        101
      ) [SAFE_ORDINAL(51)] AS tx_Mbps
    FROM
      ul
    WHERE
      ul.geo_id = districts.geo_id
    GROUP BY
      time_period
  ) AS ul,
  districts.geo_id,
  districts.state_name,
  districts.legal_area_name,
  districts.district_geom
FROM
  districts;