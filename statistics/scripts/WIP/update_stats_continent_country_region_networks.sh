#!/bin/bash

# The purpose of this script is to run queries to provide static statistics by:
#   * test_geography_timePeriod - for example: `ndt_us_aiannh_month`
#                                     - NDT test results
#                                     - US American Indian, Alaska Native Corps, & Hawaiian Homelands
#                                     - aggregated by month
#
# This script will run the queries and save the results in:
#   `measurement-lab.mlab_statistics.<table name>`
#
# Use this script for jobs that do not result in maptiles, only statistics.
#
set -eux

PROJECT="measurement-lab"
USERNAME="critzo"
PUB_LOC="api.measurementlab.net"

# Initially set the project to measurement-lab.
gcloud config set project measurement-lab

declare -a query_jobs=("continent_country_region_networks")

# TO DO: when this is run via a scheduled job, should query for the last day in the dataset
#     and use that for startday/endday values
startday=2020-01-01
endday=2020-06-29

for val in ${query_jobs[@]}; do
  RESULT_NAME="$val"
  QUERY="${RESULT_NAME}.sql"
  QUALIFIED_TABLE="${PROJECT}:mlab_statistics.${RESULT_NAME}"

  # Run bq query with generous row limit. Write results to temp table created above.
  # By default, bq fetches the query results to display in the shell, consuming a lot of memory.
  # Use --nosync to "fire-and-forget", then implement our own wait loop to defer the next command
  # until the table is populated.

  while [ "$startday" != "$endday" ]; do
    JOB_ID=$(bq --nosync --project_id "${PROJECT}" query \
      --parameter=startday::$startday --allow_large_results --destination_table "${QUALIFIED_TABLE}" \
      --append_table --use_legacy_sql=false --max_rows=4000000 \
      "$(cat "queries/${QUERY}")")

    JOB_ID="${JOB_ID#Successfully started query }"

    until [ DONE == $(bq --format json show --job "${JOB_ID}" | jq -r '.status.state') ]
    do
      sleep 30
    done

    startday=$(date -I -d "$startday + 1 day")
  done

  # Automate stats and outputs by continent, country, region, etc. using query params.
  #   Get all combinations of continent, country, and region codes & save to a local csv.

## TODO: need to output daily counts by geo by YEAR. 
##       probably should make /YYYY/ the final GCS path.
  declare -a location_combos_query=("get_continent_country_region_codes_sample")

  for v in ${location_combos_query[@]}; do
    RESULT2_NAME="$v"
    QUERY2="${RESULT2_NAME}.sql"

    JOB_ID2=$(bq --format=csv --project_id "${PROJECT}" query \
    --use_legacy_sql=false --max_rows=4000000 \
    "$(cat "queries/${QUERY2}")" > codes.csv ) 
  done

  # bq exports csvs with a header. remove the header.
  sed -i '1d' codes.csv

  # Make a temporary GCS bucket to store results.
  gsutil mb gs://temp_generate_stats

  # Loop through the csv lines, using three values as query parameters for a series of queries.
  while IFS=, read -r continent country region;
  do  
    #QUERY3="export_continent_country_region_stats.sql"

    iso_region="$country-$region"

    JOB_ID3=$(bq --nosync query \
    --use_legacy_sql=false --max_rows=4000000 --allow_large_results \
    --destination_table "mlab_statistics.temp_continent_country_region_stats" \
    --replace "SELECT * FROM \`mlab_statistics.continent_country_region_networks\` WHERE continent_code = \"${continent}\" AND country_code = \"${country}\" AND ISO3166_2region1 = \"${iso_region}\" ORDER BY test_date, continent_code, county_code, country_name, ISO3166_2region1, bucket_min, bucket_max")

    JOB_ID3="${JOB_ID3#Successfully started query }"

    until [ DONE == $(bq --format json show --job "${JOB_ID3}" | jq -r '.status.state') ]
    do
      sleep 30
    done

    # Extract the rows to JSON and/or other output formats      
    bq extract --destination_format NEWLINE_DELIMITED_JSON \
      mlab_statistics.temp_continent_country_region_stats \
      gs://temp_generate_stats/${continent}/${country}/${region}/networks_daily_stats.json

  done < codes.csv

  # Copy the full list of generated stats from measurement-lab project temp GCS bucket
  gsutil -m cp -r gs://temp_generate_stats ./tmp/

  # Change to production project and copy generated stats to the public bucket.
  gcloud config set project mlab-oti

  # Convert all new line json files to json array format
  find ./tmp/ -type f -exec sed -i '1s/^/[/; $!s/$/,/; $s/$/]/' {} +

  # Publish the json array files to public GCS bucket
  gsutil -m cp -r ./tmp/* gs://${PUB_LOC}/

  # Change back to the measurement-lab project for the next iteration.
  gcloud config set project measurement-lab

done

# Cleanup 
## Remove the temporary GCS bucket.
gsutil rm -r gs://temp_generate_stats

## Remove local copies.
rm -r ./tmp/*
