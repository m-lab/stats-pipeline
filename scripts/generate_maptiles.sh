#!/bin/bash

set -eux

FRACTION=${1?Please enter desired fraction of FCC data.}
QUERYFILE=${2?Please enter one of ../queries/(concise)_join_geo_sampling.sql.}
PUB_LOC="fcc-maptiles/june19"
GCS_STORAGE="fcc-june19-shards"
# Define paths based on FRACTION queried.
PATHSTRING="${FRACTION#.}"
PATHSTRING=$([[ ${QUERYFILE} == *"concise"* ]] && echo "${PATHSTRING}concise" || echo "${PATHSTRING}")

gcloud config set project mlab-interns-2020

# Run bq query WITH NO ROW LIMIT. Instead, FRACTION determines the fraction of
# rows returned via random sampling.

# Don't understand the following comment from legacy script:
# By default, bq fetches the query results to display in the shell, consuming a lot of memory.
# Use --nosync to "fire-and-forget", then implement our own wait loop to defer the next command
# until the table is populated.

# Join FCC data with public geo information.
# We retain the tables in BQ, so in theory this does not need to be rerun if the
# given FRACTION has already been queried for.
bq --project_id mlab-interns-2020 query \
  --allow_large_results \
  --destination_table "mlab_pipeline.fcc_june19_with_geo_${PATHSTRING}" \
  --replace \
  --use_legacy_sql=false \
  "$(cat "${QUERYFILE}" | sed "s/FRACTION/${FRACTION}/")"
#   "$(cat "../queries/join_geo.sql")"

# create a (temporary?) GCS Storage Bucket
gsutil mb gs://${GCS_STORAGE}-${PATHSTRING}

# Generate CSV files; expected to include geometry info in WKT format.
bq extract --destination_format CSV "mlab_pipeline.fcc_june19_with_geo_${PATHSTRING}" \
  gs://${GCS_STORAGE}-${PATHSTRING}/*.csv

# Create directories if needed.
[ -d ../shards ] || mkdir ../shards
[ -d ../shards/${PATHSTRING} ] || mkdir ../shards/${PATHSTRING}
[ -d ../maptiles ] || mkdir ../maptiles
[ -d ../maptiles/${PATHSTRING} ] || mkdir ../maptiles/${PATHSTRING}

# Fetch the CSV files that were just exported.
gsutil -m cp gs://${GCS_STORAGE}-${PATHSTRING}/*.csv ../shards/${PATHSTRING}

# Clean up the files on GCS because we don't need them there anymore.
# gsutil rm -r gs://${GCS_STORAGE}-${PATHSTRING}

# Use xargs to convert all the csv files to geojson individually, in
# parallel. We will aggregate them in the next step. See csv_to_geojson
# script for ogr2ogr args.
echo ../shards/${PATHSTRING}/*.csv | xargs -n1 -P4 ./csv_to_geojson.sh 

# # Let tippecanoe read all the geojson files into one layer.
# if [ SOME CONDITION ]; then 
#   sudo /usr/local/bin/tippecanoe -e ../maptiles/${PATHSTRING} -f -l fcc-june19 \
#     ../shards/${PATHSTRING}/*.geojson -Z4 -z10 -d8 -D8 -m4 \
#     --simplification=10 \
#     --detect-shared-borders \
#     --drop-densest-as-needed \
#     --coalesce-densest-as-needed \
#     --no-tile-compression
# else 
sudo /usr/local/bin/tippecanoe -e ../maptiles/${PATHSTRING} -f -l fcc-june19 \
  ../shards/${PATHSTRING}/*.geojson -zg \
  --simplification=10 \
  --detect-shared-borders \
  --drop-densest-as-needed \
  --coalesce-densest-as-needed \
  --no-tile-compression
# fi

# Upload generated tile set to cloud storage publishing location
gsutil -m -h 'Cache-Control:private, max-age=0, no-transform' \
  cp -r ../maptiles/${PATHSTRING}/* gs://${PUB_LOC}/${PATHSTRING}/

# # Upload csv source data
# gsutil -m -h 'Cache-Control:private, max-age=0, no-transform' \
#   cp -r ./${RESULT_NAME}_*.csv gs://${PUB_LOC}/${PATHSTRING}/csv/

# Cleanup local files 
sudo rm -rf ../shards/${PATHSTRING}/* ../maptiles/${PATHSTRING}/*
