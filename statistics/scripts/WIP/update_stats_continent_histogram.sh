#!/bin/bash
set -eux

PROJECT="measurement-lab"
USERNAME="critzo"
PUB_LOC="api.measurementlab.net"

# Initially set the project to measurement-lab.
gcloud config set project measurement-lab

declare -a query_jobs=("continent_histogram")

#########################
# Set date parameters for this run.  
# Two options: - Check the exising table for the last date in the table
#              - Set specific start & end dates
#
#   Comment out the option you don't want to use

### option 1
today=($(TZ=GMT date +"%Y-%m-%d"))
sixmonths=$(TZ=GMT date -I -d "$today - 6 month")

# Get the last date in the appropriate stats table from the last run.

JOB_ID0=$(bq --format=json --project_id "${PROJECT}" query \
  --use_legacy_sql=false "SELECT test_date FROM \`measurement-lab.mlab_statistics.continent_histogram\` ORDER BY test_date DESC LIMIT 1" > lastdate.json )

lastday=($(jq -r .[].test_date lastdate.json))

# Set startday to -2 days from last date.
#   This ensures we've got all data from that day, as previous runs may have missed 
#   tests that hadn't been pushed or were reprocessed since the last time.
#
startday=$(TZ=GMT date -I -d "$lastday - 2 day")

# Set end day to -4 days from today. 
#   This ensures we're processing days where ETL has likely published most test data already.
#
endday=$(TZ=GMT date -I -d "$today - 4 day")

## When running option 1, automatic date selection:
#     - first we'll delete any data between startday and endday
#     - this ensures we are reprocessing all recent days to account for test rows 
#       that might have been added since the last run by pusher or re-processed by gardener

JOB_ID1=$(bq --nosync --project_id "${PROJECT}" query \
  --use_legacy_sql=false "DELETE FROM \`measurement-lab.mlab_statistics.continent_histogram\` WHERE test_date BETWEEN \"${startday}\" AND \"${endday}\"")

JOB_ID1="${JOB_ID1#Successfully started query }"

until [ DONE == $(bq --format json show --job "${JOB_ID1}" | jq -r '.status.state') ]
do
  sleep 30
done

### option 2
#startday=2019-12-31
#endday=2020-01-01
#########################

# Set the start and end year so we can group output by year
startarray=($(echo $startday | tr "-" "\n"))
startyear=${startarray[0]}
endarray=($(echo $endday | tr "-" "\n"))
endyear=${endarray[0]}

# TODO : year range as implemented needs work. it will work correctly if we're doing a year at a time, 
#         but if we define a range across a year boundary, but not the entire year on either end,
#         the resulting year based JSON file in GCS will only be for the partial year.
#
#   source data by day in the stats table will be good, but perhaps a second script is needed to parse results 
#      into JSON in GCS by year AFTER new data is pulled into _stats, and not at the same time.

#   other possible solution- use option 2 for pulling stats into the API for past results, 
#                            automated dates only for the current year up to "today"
#                            a run of this script regenerates the whole year for the current year

year_range=()
year_range+=(${startyear})
while [ "$startyear" != "$endyear" ]; do
  startyear=$((startyear+1))
  year_range+=(${startyear})
done
year_range+=(${endyear})

for val in ${query_jobs[@]}; do
  RESULT_NAME="$val"
  QUERY="${RESULT_NAME}.sql"
  QUALIFIED_TABLE="${PROJECT}:mlab_statistics.${RESULT_NAME}"

  # Run bq query for each day in the selected range with generous row limit. 
  # Appends data to a table of the same name as the query in `measurement-lab.mlab_statistics.`

  while [ "$startday" != "$endday" ]; do
    JOB_ID=$(bq --nosync --project_id "${PROJECT}" query \
      --parameter=startday::$startday --allow_large_results --destination_table "${QUALIFIED_TABLE}" \
      --append_table --use_legacy_sql=false --max_rows=4000000 \
      "$(cat "queries/${QUERY}")")

    JOB_ID="${JOB_ID#Successfully started query }"

    # Strip the prefix text from the job_id
    until [ DONE == $(bq --format json show --job "${JOB_ID}" | jq -r '.status.state') ]
    do
      sleep 30
    done

    startday=$(TZ=GMT date -I -d "$startday + 1 day")
  done

  # Automate stats and outputs by continent, country, region, etc. using query params.
  #   Get all combinations of continent, country, and region codes & save to a local csv.

  declare -a location_combos_query=("get_continent_codes")

  for v in ${location_combos_query[@]}; do
    RESULT2_NAME="$v"
    QUERY2="${RESULT2_NAME}.sql"

    JOB_ID2=$(bq --format=csv --project_id "${PROJECT}" query \
    --use_legacy_sql=false --max_rows=4000000 \
    "$(cat "queries/${QUERY2}")" > continent_codes.csv ) 
  done

  # bq exports csvs with a header. remove the header.
  sed -i '1d' continent_codes.csv

  # Make a temporary GCS bucket to store results.
  gsutil mb gs://temp_generate_stats_continent_histogram

  # Grab the stats generated in bulk above, by year
  for year in "${year_range[@]}"; do

    # Loop through the csv lines, using three values as query parameters for a series of queries.
    while IFS=, read -r continent;
    do  
      JOB_ID3=$(bq --nosync query \
      --use_legacy_sql=false --max_rows=4000000 --allow_large_results \
      --destination_table "mlab_statistics.temp_continent_stats" \
      --replace "SELECT * FROM \`mlab_statistics.continent_histogram\` WHERE continent_code = \"${continent}\" AND CAST(test_date AS STRING) LIKE \"${year}%\" ORDER BY test_date, continent_code, bucket_min, bucket_max")

      JOB_ID3="${JOB_ID3#Successfully started query }"

      until [ DONE == $(bq --format json show --job "${JOB_ID3}" | jq -r '.status.state') ]
      do
        sleep 30
      done

      # Extract the rows to JSON and/or other output formats      
      bq extract --destination_format NEWLINE_DELIMITED_JSON \
        mlab_statistics.temp_continent_country_region_stats \
        gs://temp_generate_stats_continent_histogram/${continent}/${year}/histogram_daily_stats.json

   done < continent_codes.csv

  done

  # Copy the full list of generated stats from measurement-lab project temp GCS bucket
  gsutil -m cp -r gs://temp_generate_stats_continent_histogram/* tmp/

  # Change to production project and copy generated stats to the public bucket.
  gcloud config set project mlab-oti

  # Convert all new line json files to json array format
  find ./tmp/ -type f -exec sed -i '1s/^/[/; $!s/$/,/; $s/$/]/' {} +

  # Publish the json array files to public GCS bucket
  gsutil -m cp -r tmp/* gs://${PUB_LOC}/

  # Change back to the measurement-lab project for the next iteration.
  gcloud config set project measurement-lab

done

# Cleanup 
## Remove the temporary GCS bucket.
gsutil rm -r gs://temp_generate_stats_continent_histogram

## Remove local copies.
rm -r ./tmp/*
rm continent_codes.csv
