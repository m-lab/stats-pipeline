#!/bin/bash
set -eux

PROJECT="measurement-lab"
USERNAME="critzo"
PUB_LOC="api.measurementlab.net"

# Initially set the project to measurement-lab.
gcloud config set project measurement-lab

declare -a query_jobs=("us_county_histogram")

##########################
# Set date parameters for this run.
# Two options: - Check the exising table for the last date in the table
#              - Set specific start & end dates
#
#   Comment out the option you don't want to use

### option 1
#today=($(TZ=GMT date +"%Y-%m-%d"))
#sixmonths=$(TZ=GMT date -I -d "$today - 6 month")

# Get the last date in the appropriate stats table from the last run.
#JOB_ID0=$(bq --format=json --nosync --project_id "${PROJECT}" query \
#  "SELECT test_date FROM \`measurement-lab.mlab_statistics.continent_country_histogram\` WHERE test_date >= \"${sixmonths}\" ORDER BY test_date DESC LIMIT 1") > lastdate.json

#JOB_ID0="${JOB_ID0#Successfully started query }"

#until [ DONE == $(bq --format json show --job "${JOB_ID0}" | jq -r '.status.state') ]
#do
#  sleep 30
#done

#lastday=($(jq .[].test_date lastdate.json))

# Set startday to -2 days from last date.
#   This ensures we've got all data from that day, as previous runs may have missed
#   tests that hadn't been pushed or were reprocessed since the last time.
#
#startday=$(TZ=GMT date -I -d "$lastday - 2 day")

# Set end day to -4 days from today.
#   This ensures we're processing days where ETL has likely published most test data already.
#
#endday=$(TZ=GMT date -I -d "$today - 4 day")

## When running option 1, automatic date selection:
#     - first we'll delete any data between startday and endday
#     - this ensures we are reprocessing all recent days to account for test rows
#       that might have been added since the last run by pusher or re-processed by gardener

#JOB_ID1=$(bq --nosync --project_id "${PROJECT}" query \
#  --use_legacy_sql=false "DELETE FROM \`measurement-lab.mlab_statistics.continent_country_histogram\` WHERE test_date BETWEEN \"${startday}\" AND \"${endday}\"")

#JOB_ID1="${JOB_ID1#Successfully started query }"

#until [ DONE == $(bq --format json show --job "${JOB_ID1}" | jq -r '.status.state') ]
#do
#  sleep 30
#done

### option 2
startday=2020-01-01
endday=2020-07-01
#########################

# Set the start and end year so we can group output by year
startarray=($(echo $startday | tr "-" "\n"))
startyear=${startarray[0]}
endarray=($(echo $endday | tr "-" "\n"))
endyear=${endarray[0]}
endyear=$((endyear+1))

year_range=()
year_range+=(${startyear})

while [ "$startyear" != "$endyear" ]; do
  startyear=$((startyear+1))
  year_range+=(${startyear})
done

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

  # Automate stats and outputs by continent, country, etc. using query params.
  #   Get all combinations of continent and country codes & save to a local csv.
  declare -a location_combos_query=("get_continent_country_codes")

  for v in ${location_combos_query[@]}; do
    RESULT2_NAME="$v"
    QUERY2="${RESULT2_NAME}.sql"

    JOB_ID2=$(bq --format=csv --project_id "${PROJECT}" query \
    --use_legacy_sql=false --max_rows=4000000 \
    "$(cat "queries/${QUERY2}")" > continent_country_codes.csv )
  done

  # bq exports csvs with a header. remove the header.
  sed -i '1d' continent_country_codes.csv

  # Make a temporary GCS bucket to store results.
  gsutil mb gs://temp_stats_continent_country

  # Grab the stats generated in bulk above, by year
  for year in "${year_range[@]}"; do

    # Loop through the csv lines, using three values as query parameters for a series of queries.
    while IFS=, read -r continent country;
    do
      JOB_ID3=$(bq --nosync query \
      --use_legacy_sql=false --max_rows=4000000 --allow_large_results \
      --destination_table "mlab_statistics.temp_continent_country_stats" \
      --replace "SELECT * FROM \`mlab_statistics.continent_country_histogram\` WHERE continent_code = \"${continent}\" AND country_code = \"${country}\" ORDER BY test_date, continent_code, county_code, country_name, bucket_min, bucket_max")

      JOB_ID3="${JOB_ID3#Successfully started query }"

      until [ DONE == $(bq --format json show --job "${JOB_ID3}" | jq -r '.status.state') ]
      do
        sleep 30
      done

      # Extract the rows to JSON and/or other output formats
      bq extract --destination_format NEWLINE_DELIMITED_JSON \
        mlab_statistics.temp_continent_country_stats \
        gs://temp_generate_stats/${continent}/${country}/${year}/histogram_daily_stats.json

    done < continent_country_codes.csv

  done

  # Copy the full list of generated stats from measurement-lab project temp GCS bucket
  gsutil -m cp -r gs://temp_stats_continent_country/* ./tmp/

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
gsutil rm -r gs://temp_stats_continent_country

## Remove local copies.
rm -r ./tmp/*
rm continent_country_codes.csv
