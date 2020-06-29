##Notes for USBB Process

more mapping notes

### ogr2ogr to go from shapedata to geojson

#### shp > geojson in ogr2ogr
`ogr2ogr -f GeoJSON tl_2016_us_county.geojson tl_2016_us_county.shp`

#### GEOID join on shp > geojson in ogr2ogr
```sh
ogr2ogr -f GeoJSON mlab_county.geojson USBB-2/data/county_mlab_2015_2018.csv -sql "SELECT * FROM county_mlab_2015_2018 c JOIN 'USBB-2/shapefiles/tl_2016_us_county/tl_2016_us_county.shp'.tl_2016_us_county s on c.GEOID = s.GEOID"
```

### tippecanoe from geojson > mbtiles

`tippecanoe -zg -o tl_2016_us_county.mbtiles --drop-densest-as-needed --extend-zooms-if-still-dropping tl_2016_us_county.geojson`


### tippecanoe+ogr2ogr from csv to mbtiles
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

