#!/bin/bash
set -eux

PROJECT="mlab-sandbox"
USERNAME="critzo"
PUB_LOC="test-critzo-statistics"

# Initially set the project to measurement-lab.
gcloud config set project mlab-sandbox

declare -a query_jobs=("continent_country_region_histogram")


startday=2020-01-01
endday=2020-01-01

#########################
endday=$(TZ=GMT date -I -d "$endday + 1 day")
yeararray=($(echo $startday | tr "-" "\n"))
year=${yeararray[0]}

for val in ${query_jobs[@]}; do
  RESULT_NAME="$val"
  QUERY="${RESULT_NAME}.sql"
  QUALIFIED_TABLE="${PROJECT}:test_critzo_statistics.${RESULT_NAME}"
  QUALIFIED_TABLE_IN_QUERY="${PROJECT}.test_critzo_statistics.${RESULT_NAME}"
  DATASET="test_critzo_statistics"
  TEMP_TABLE="temp_continent_country_region_stats"
  TEMP_STATS="${PROJECT}:${DATASET}.${TEMP_TABLE}"

  # Run bq query with generous row limit. Write results to temp table created above.
  # By default, bq fetches the query results to display in the shell, consuming a lot of memory.
  # Use --nosync to "fire-and-forget", then implement our own wait loop to defer the next command
  # until the table is populated.

  # TODO: add a check to see if this table exists already, and create it if not.

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

  declare -a location_combos_query=("get_continent_country_region_codes_sample")

  for v in ${location_combos_query[@]}; do
    RESULT2_NAME="$v"
    QUERY2="${RESULT2_NAME}.sql"

    JOB_ID2=$(bq --format=csv --project_id "${PROJECT}" query \
    --use_legacy_sql=false --max_rows=4000000 \
    "$(cat "queries/${QUERY2}")" > continent_country_region_codes.csv )
  done

  # bq exports csvs with a header. remove the header.
  sed -i '1d' continent_country_region_codes.csv

  # Make a temporary GCS bucket to store results.
  gsutil mb gs://${USERNAME}_temp_stats_continent_country_region

  # Loop through the csv lines, using three values as query parameters for a series of queries.
  while IFS=, read -r continent country region;
  do
    iso_region="$country-$region"

    JOB_ID3=$(bq --nosync query \
    --use_legacy_sql=false \
    --max_rows=4000000 \
    --project_id "${PROJECT}" \
    --allow_large_results --destination_table "${TEMP_STATS}" \
    --replace "SELECT * FROM ${QUALIFIED_TABLE_IN_QUERY} WHERE continent_code = \"${continent}\" AND country_code = \"${country}\" AND ISO3166_2region1 = \"${iso_region}\" ORDER BY test_date, continent_code, country_code, country_name, ISO3166_2region1, bucket_min, bucket_max")

    JOB_ID3="${JOB_ID3#Successfully started query }"

    until [ DONE == $(bq --format json show --job "${JOB_ID3}" | jq -r '.status.state') ]
    do
      sleep 30
    done

    # Extract the rows to JSON and/or other output formats
    bq extract --destination_format NEWLINE_DELIMITED_JSON \
      ${QUALIFIED_TABLE} \
      gs://${USERNAME}_temp_stats_continent_country_region/${continent}/${country}/${region}/${year}/histogram_daily_stats.json

  done < continent_country_region_codes.csv

  # Copy the full list of generated stats from measurement-lab project temp GCS bucket
  gsutil -m cp -r gs://${USERNAME}_temp_stats_continent_country_region/* tmp/

  # Convert all new line json files to json array format
  find ./tmp/ -type f -exec sed -i '1s/^/[/; $!s/$/,/; $s/$/]/' {} +

  # Publish the json array files to public GCS bucket
  gsutil -m cp -r tmp/* gs://${PUB_LOC}/

done

# Cleanup
## Remove the temporary GCS bucket.
gsutil rm -r gs://${USERNAME}_temp_stats_continent_country_region

## Remove local copies.
rm -r ./tmp/*
rm continent_country_region_codes.csv
