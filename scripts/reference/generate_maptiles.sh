#!/bin/bash

set -eux

TABLE="maptiles_temp.temp"
PUB_LOC="maptiles.measurementlab.net"

declare -a query_jobs=( "us_county_ndt_month" \
                    "us_county_ndt_week"    \
                    "us_state_ndt_month"    \
                    "us_state_ndt_week"     \
                    "us_congress_ndt_month" \
                    "us_congress_ndt_week"  \
                    "us_aiannh_ndt_month"   \
                    "us_aiannh_ndt_week"    
  )

for val in ${query_jobs[@]}; do
  RESULT_NAME="$val"
  QUERY="${RESULT_NAME}.sql"
  GCS_STORAGE="${RESULT_NAME}_temp"

  # Run bq query with generous row limit. Write results to temp table created above.
  # By default, bq fetches the query results to display in the shell, consuming a lot of memory.
  # Use --nosync to "fire-and-forget", then implement our own wait loop to defer the next command
  # until the table is populated.
  gcloud config set project measurement-lab

  JOB_ID=$(bq --nosync --project_id measurement-lab query \
    --allow_large_results --destination_table "mlab_statistics.${RESULT_NAME}" \
    --replace --use_legacy_sql=false --max_rows=4000000 \
    "$(cat "queries/${QUERY}")")

  JOB_ID="${JOB_ID#Successfully started query }"

  until [ DONE == $(bq --format json show --job "${JOB_ID}" | jq -r '.status.state') ]
  do
    sleep 30
  done

  # create a temprary GCS Storage Bucket
  gsutil mb gs://${GCS_STORAGE}

  # Generate CSV files; expected to include geometry info in WKT format.
  bq extract --destination_format CSV "mlab_statistics.${RESULT_NAME}" \
      gs://${GCS_STORAGE}/${RESULT_NAME}_*.csv

  # Fetch the CSV files that were just exported.
  gsutil -m cp gs://${GCS_STORAGE}/${RESULT_NAME}_*.csv ./

  # Cleanup the files on GCS because we don't need them there anymore.
  gsutil rm -r gs://${GCS_STORAGE}

  # ogr2ogr requires a schema file to know which csv column represents
  # the geometry. We pass all filenames to the inference script, but
  # it only reads the first one, since the schema should be consistent
  # for all of them.
  scripts/infer_csvt_schema.sh ${RESULT_NAME}_*.csv > schema.csvt

  # Use xargs to convert all the csv files to geojson individually, in
  # parallel. We will aggregate them in the next step.  See csv_to_geojson
  # script for ogr2ogr args.
  echo ${RESULT_NAME}_*.csv | xargs -n1 -P4 scripts/csv_to_geojson.sh 

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

  # Switch projects
  gcloud config set project mlab-oti

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

done
