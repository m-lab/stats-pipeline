Notes for USBB Process

## Overview
1. Use the geo functions in BigQuery to aggregate the data to geographic areas 
2. Export that to geojson (or csv)
3. Use ogr2ogr + [tippecanoe](https://github.com/mapbox/tippecanoe) to generate MBTiles of the results, and 
4. use Mapbox or openmaptiles to serve them up to the application. Locally can use `klokantech/tileserver-gl` to serve your mbtiles file. Openmaptiles docker container sets up an account and downloads tiles from their service. Stuart shared this command: `docker run --rm -it -v $(pwd):/data -p 8080:80 klokantech/tileserver-gl` or you can use Kitematic/Docker to run the docker container.


## Data Sets

- M-Lab Data (ndt)
- FCC Form 477 Data
- Geo Data
	- Census Tracts
	- Counties
	- State House
	- State Senate
	- Congress
	- Zip

## Time Periods, as defined by the FCC data sets

1. Dec 2014 = BETWEEN '2014-07-01' AND '2014-12-31'
2. Jun 2015 = BETWEEN '2015-01-01' AND '2015-06-30'
3. Dec 2015 = BETWEEN '2015-07-01' AND '2015-12-31'
4. Jun 2016 = BETWEEN '2016-01-01' AND '2016-06-30'
5. Dec 2016 = BETWEEN '2016-07-01' AND '2016-12-31'
6. Jun 2017 = BETWEEN '2017-01-01' AND '2017-06-30'

Available in M-Lab, but not in the FCC data:

7. Coming soon: Dec 2017 = BETWEEN '2017-07-01' AND '2017-12-31'
8. Coming soon: Jun 2018 = BETWEEN '2018-01-01' AND '2018-06-30'
9. Coming soon: Dec 2018 = BETWEEN '2018-07-01' AND '2018-12-31'

## M-Lab Data

### Structure

- M-Lab Data (ndt)
    - download speed
    - upload speed
    - asn (currently not added, but later maybe)
    - lat, long
    - rtt
    - date

### Counts of tests and IPs
```sql
count(test_id) as count_tests
```

```sql
count(distinct connection_spec.client_ip) as count_ips
```
 
### Download speed aggregate medians
```sql
APPROX_QUANTILES(8 * SAFE_DIVIDE(web100_log_entry.snap.HCThruOctetsAcked,
    (web100_log_entry.snap.SndLimTimeRwin +
    web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd)), 101)[SAFE_ORDINAL(51)] AS download_Mbps
```
  
### Upload speed aggregate medians
```sql
APPROX_QUANTILES(8 * SAFE_DIVIDE(web100_log_entry.snap.HCThruOctetsReceived,web100_log_entry.snap.Duration), 101)[SAFE_ORDINAL(51)] AS upload_Mbps
```
  
### Min RTT aggregate medians
```sql
APPROX_QUANTILES(web100_log_entry.snap.MinRTT, 101)[SAFE_ORDINAL(51)] AS min_rtt
```

### Full example query
```sql
#standardSQL
SELECT
  'census_tract' AS geo,
  count(test_id) as ml_count_tests,
  count(distinct connection_spec.client_ip) as ml_count_ips,
  APPROX_QUANTILES(8 * SAFE_DIVIDE(web100_log_entry.snap.HCThruOctetsAcked,
      (web100_log_entry.snap.SndLimTimeRwin +
        web100_log_entry.snap.SndLimTimeCwnd +
        web100_log_entry.snap.SndLimTimeSnd)), 101)[SAFE_ORDINAL(51)] AS ml_download_Mbps,
  APPROX_QUANTILES(8 * SAFE_DIVIDE(web100_log_entry.snap.HCThruOctetsReceived,web100_log_entry.snap.Duration), 101)[SAFE_ORDINAL(51)] AS ml_upload_Mbps,
  APPROX_QUANTILES(web100_log_entry.snap.MinRTT, 101)[SAFE_ORDINAL(51)] AS ml_min_rtt,
  GEOID,
  AFFGEOID,
  CASE
    WHEN partition_date BETWEEN '2014-07-01' AND '2014-12-31' THEN 'dec_2014'
    WHEN partition_date BETWEEN '2015-01-01' AND '2015-06-30' THEN 'jun_2015'
    WHEN partition_date BETWEEN '2015-07-01' AND '2015-12-31' THEN 'dec_2015'
    WHEN partition_date BETWEEN '2016-01-01' AND '2016-06-30' THEN 'jun_2016'
    WHEN partition_date BETWEEN '2016-07-01' AND '2016-12-31' THEN 'dec_2016'
    WHEN partition_date BETWEEN '2017-01-01' AND '2017-06-30' THEN 'jun_2017'
    WHEN partition_date BETWEEN '2017-07-01' AND '2017-12-31' THEN 'dec_2017'
    WHEN partition_date BETWEEN '2018-01-01' AND '2018-06-30' THEN 'jun_2018'
    WHEN partition_date BETWEEN '2018-07-01' AND '2018-12-31' THEN 'dec_2018'
    END AS time_period
FROM
  `measurement-lab.release.ndt_all`,
  `mlab-sandbox.usa_geo.cb_2016_census_tracts`
WHERE
  connection_spec.server_geolocation.country_name = "United States"
  AND partition_date BETWEEN '2014-07-01' AND '2018-12-31'
  AND ST_WITHIN(ST_GeogPoint(connection_spec.client_geolocation.longitude , connection_spec.client_geolocation.latitude ), tract_polygons)
GROUP BY
  GEOID,
  time_period,
  AFFGEOID
```

## FCC Data

### Structure

- FCC Form 477 Data
    - FRN
    - Provider_Name
    - DBA_Name
    - Holding_Company_Number    
    - Holding_Company_Final
    - Census_Block_FIPS_Code
    - State
    - Technology_Code
    - Max_Advertised_Downstream_Speed__mbps_
    - Max_Advertised_Upstream_Speed__mbps_
    - Business [0,1] - Boolean?
    - Consumer [0,1] - Boolean?
    - Max_CIR_Downstream_Speed__mbps_
    - Max_CIR_Upstream_Speed__mbps_
    - time_period

### Goal 
`location, median dl time_period, median up time_period, count_isps time_period, max advertized down time_period, max advertized up, time_period`

### How to go from Census Block to other geo
location Census FIPS > County, Tract, State House, State Senate

Census Block Code: 06|067|001101|1085
Corresponds to: 06 - State| 067 - County| 001101 - Tract| 1085 - block

do we think this is accurate? [https://i.stack.imgur.com/sF4tS.png](https://i.stack.imgur.com/sF4tS.png)
from here: [https://gis.stackexchange.com/questions/55239/which-census-geography-boundaries-do-congressional-districts-preserve](https://gis.stackexchange.com/questions/55239/which-census-geography-boundaries-do-congressional-districts-preserve)


## Steps

### M-Lab Data

1. Query M-Lab data, case by time period, spatial joined to geometry in BQ
2. Save to table
3. Export table as CSV
4. ogr2ogr to tippecanoe to create mbtiles from geojson
6. Repeat for each geometry

### FCC Data

1. Export csv of FCC data with Census Tract GEOID
2. download all of them from GCP `gsutil cp gs://bucket/path/* ./`
3. tbd.


## Making Tiles: tippecanoe+ogr2ogr from csv to mbtiles

### Make sure the CSV has the right field types

By default, ogr2ogr will treat all csv columns as text fields.
You can provide a schema file with the same name as the .csv, but with the .csvt extension to fix this; this format is documented in the [ogr2ogr docs](https://www.gdal.org/drv_csv.html).
I used a CSV processing tool called [xsv](https://github.com/burntsushi/xsv) to generate a .csvt semi-automatically.

```sh
$ xsv select '!WKT' mlab_county_dec2014_dec2018_429.csv | \
  xsv stats | \
  xsv select type | \
  tail -n +2 | \
  sed 's/.*/"&"/' | \
  sed 's/Unicode/String/g' | \
  sed 's/Float/Real/g' | \
  tr '\n' , > mlab_county_dec2014_dec2018_429.csvt
$ echo '"WKT"' >> mlab_county_dec2014_dec2018_429.csvt
```

### Use ogr2ogr to make the tiles

By specifying `/dev/stdout` as the output file for ogr2ogr and specifying `/dev/stdin` as the input file for tippecanoe both can be part of a Unix pipeline.

`-oo KEEP_GEOM_COLUMNS` avoids ogr2ogr including the WKT-encoded geometry in the output; it's a waste to keep it because we have the GeoJSON geometry instead.
By default, ogr2ogr looks for WKT geometry in a column literally named `WKT`.

```sh
$ ogr2ogr -f GeoJSON /dev/stdout \
  -oo KEEP_GEOM_COLUMNS=no \
  mlab_county_dec2014_dec2018_429.csv | \
  tippecanoe -o mlab_county_dec2014_dec2018_429.mbtiles \
    -l mlab_county_dec2014_dec2018 /dev/stdin -zg
```

### Join csvs using xsv
```sh
$ xsv join --help
...
Usage:
    xsv join [options] <columns1> <input1> <columns2> <input2>
...
```

```sh
$ xsv join --full \
  GEOID fcc_477_county_2014_2017_0515.csv \
  GEOID mlab_county_dec2014_dec2018_429.csv | \
  xsv select '!NAME,GEOID,WKT' > fcc_mlab_county_dec2014_dec2018_429.csv
```

```sh
$ (
  echo 'GEOID'; \
  xsv select 'GEOID' mlab_census_tracts_2014_2018_0430.csv | \
  tail -n +2 | \
  xargs printf '%011d\n' \
  ) | \
  xsv cat columns - <(xsv select '!GEOID' mlab_census_tracts_2014_2018_0430.csv) > ~/mlab_adjusted_census_tracts.csv
$ xsv join --full GEOID fcc477_ct_json.csv GEOID \
  ~/mlab_adjusted_census_tracts.csv | xsv select '!GEOID,WKT' > fcc_mlab_census_tract.csv
```