#!/bin/bash

GCS_GEO_BUCKET="county_geo_by_state"
# Create GCS bucket for geo CSVs
gsutil mb gs://${GCS_GEO_BUCKET}

# for .shp file in current directory:
for shpfile in *.shp; do

  # Convert shape file to CSV, to be uploaded to GCS/BigQuery
  ogr2ogr -f CSV -dialect sqlite -sql "SELECT AsGeoJSON(geometry) AS WKT, * FROM ${shpfile%.*}" ${shpfile%.*}.csv ${shpfile}

done

# Upload CSV to GCS bucket
gsutil cp *.csv gs://${GCS_GEO_BUCKET}
