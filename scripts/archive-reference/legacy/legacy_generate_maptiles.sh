#!/bin/bash

set -eux

# TABLE="maptiles_temp.temp"
PUB_LOC="fcc-maptiles"

# declare -a query_jobs=( "" \
#   )
# 
# for val in ${query_jobs[@]}; do
#   RESULT_NAME="$val"
#   QUERY="${RESULT_NAME}.sql"
#   GCS_STORAGE="${RESULT_NAME}_temp"

  # Run bq query with generous row limit. Write results to temp table created above.
  # By default, bq fetches the query results to display in the shell, consuming a lot of memory.
  # Use --nosync to "fire-and-forget", then implement our own wait loop to defer the next command
  # until the table is populated.
  gcloud config set project mlab-interns-2020

#   JOB_ID=$(bq --nosync --project_id measurement-lab query \
#     --allow_large_results --destination_table "mlab_statistics.${RESULT_NAME}" \
#     --replace --use_legacy_sql=false --max_rows=4000000 \
#     "$(cat "queries/${QUERY}")")
# 
#   JOB_ID="${JOB_ID#Successfully started query }"
# 
#   until [ DONE == $(bq --format json show --job "${JOB_ID}" | jq -r '.status.state') ]
#   do
#     sleep 30
#   done

  # create a temprary GCS Storage Bucket
  gsutil mb gs://${GCS_STORAGE}

  # Generate CSV files; expected to include geometry info in WKT format.
  # bq extract --destination_format CSV "mlab_statistics.${RESULT_NAME}" \
  #     gs://${GCS_STORAGE}/${RESULT_NAME}_*.csv

  bq extract --destination_format CSV "mlab-interns-2020." \
      gs://${GCS_STORAGE}/${RESULT_NAME}_*.csv

  # Fetch the CSV files that were just exported.
  gsutil -m cp gs://${GCS_STORAGE}/${RESULT_NAME}_*.csv ./

  # Cleanup the files on GCS because we don't need them there anymore.
  gsutil rm -r gs://${GCS_STORAGE}

  # # ogr2ogr requires a schema file to know which csv column represents
  # # the geometry. We pass all filenames to the inference script, but
  # # it only reads the first one, since the schema should be consistent
  # # for all of them.
  # scripts/infer_csvt_schema.sh ${RESULT_NAME}_*.csv > schema.csvt

#   # Use xargs to convert all the csv files to geojson individually, in
#   # parallel. We will aggregate them in the next step.  See csv_to_geojson
#   # script for ogr2ogr args.
#   echo ${RESULT_NAME}_*.csv | xargs -n1 -P4 scripts/csv_to_geojson.sh 

  # (OPTIONAL:) Truncate CSV. Do this either here or in BQ.
  # FPAT is regex parsing the csv input (either pure comma
  # separation, or surrounded by double quotes), OFS is the output delimiter, 
  # and the main expression takes the tenth entry (Block Code) and replaces it with its first
  # 12 characters (Block Group Code) for all rows after the first. 
  # awk -vFPAT='([^,]*)|("[^"]+")' -vOFS=, 'NR>1{$"10"=substr($"10",1,12)}1' ${}.csv > ${}.csv

  # Convert shape files to geojson
  # ogr2ogr -f GeoJSON ${}.geojson ${}.shp

  # Convert shape files to CSV, to be uploaded to GCS/BigQuery
  # ogr2ogr -f CSV -dialect sqlite -sql "SELECT AsGeoJSON(geometry) AS WKT, * FROM cb_2018_06_bg_500k" ca_for_bq.csv cb_2018_06_bg_500k.shp
  # THE ABOVE STEP IS TAKEN CARE OF HERE
  # ./convert_and_upload_geo.sh

  # Upload ca_for_bq.csv to BQ, JOIN with truncated FCC data, download as
  # ca_bq_join.csv
  # for geo_csv in gs://${GCS_GEO_BUCKET}/*.csv; do
  #   bq mk --external_table_definition=../schema/shapefile_csv_schema.json@CSV=gs://${GCS_GEO_BUCKET}/${}.csv mlab_pipeline.${}
  # done
  
  # Just need to do this once!!!!
  # bq --project_id mlab-interns-2020 query \
  #   --allow_large_results \
  #   --destination_table "mlab_pipeline.fcc_june19_with_geo" \
  #   --replace \
  #   --use_legacy_sql=false \
  #   "$(cat "../queries/join_geo.sql")"

  bq extract --destination_format CSV "mlab_pipeline.${RESULT_NAME}" \
      gs://${GCS_STORAGE}/${RESULT_NAME}_*.csv

  # Convert joined CSV to GeoJSON:
  gsutil -m cp gs://${GCS_STORAGE}/${RESULT_NAME}_*.csv ./
  echo ${RESULT_NAME}_*.csv | xargs -n1 -P4 scripts/csv_to_geojson.sh 
  ogr2ogr -f GeoJSON -oo KEEP_GEOM_COLUMNS=NO ca_bq.geojson ca_bq_join.csv

  # (OLD STUFF - DO THE JOIN IN BQ!!) Join with (truncated) CSV
  # ogr2ogr -f GeoJSON california_retry.geojson truncated.csv -sql "SELECT * FROM trun
# cated c JOIN 'california_shape.geojson'.cb_2018_06_bg_500k s ON c.BlockCode = s.GEOID"

  # Let tippecanoe read all the geojson files into one layer.
  if [ $RESULT_NAME = "us_zipcode"]; then 
    tippecanoe -e ./maptiles/${RESULT_NAME} -f -l ${RESULT_NAME} \
      *.geojson -Z4 -z10 -d8 -D8 -m4 \
      --simplification=10 \
      --detect-shared-borders \
      --drop-densest-as-needed \
      --coalesce-densest-as-needed \
      --no-tile-compression
  else 
    tippecanoe -e ./maptiles/${RESULT_NAME} -f -l ${RESULT_NAME} \
      *.geojson -zg \
      --simplification=10 \
      --detect-shared-borders \
      --drop-densest-as-needed \
      --coalesce-densest-as-needed \
      --no-tile-compression
  fi

  # Define the GCS path based on the RESULT NAME.
  PATHSTRING="$(echo ${RESULT_NAME//_//})"

  # # Switch projects
  # gcloud config set project mlab-oti

  # Upload generated tile set to cloud storage publishing location
  gsutil -m -h 'Cache-Control:private, max-age=0, no-transform' \
    cp -r ./maptiles/${RESULT_NAME}/* gs://${PUB_LOC}/${PATHSTRING}/

  # Upload csv source data
  gsutil -m -h 'Cache-Control:private, max-age=0, no-transform' \
    cp -r ./${RESULT_NAME}_*.csv gs://${PUB_LOC}/${PATHSTRING}/csv/

  # Cleanup local files 
  rm -r schema.csvt ${RESULT_NAME}_* maptiles/*

  # gsutil -m -h 'Cache-Control:private, max-age=0, no-transform' \
  # cp -r ./maptiles/example.html gs://${PUB_LOC}/${RESULT_NAME}/index.html

  # maptiles.mlab-sandbox.measurementlab.net
  # NOTE: if the html and tiles are served from different domains we'll need to
  # apply a CORS policy to GCS.

# done
