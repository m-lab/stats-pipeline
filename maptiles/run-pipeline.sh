#!/bin/bash
#
# run-pipeline.sh starts stats-pipeline for the current year and then
# generates updated maptiles.

set -euxo pipefail
ENDPOINT=${1?"Please provide the endpoint (hostname + port). Usage: $0 <endpoint>"}

# Start the pipeline for the past 2 days.
start=$(date -d "@$(( $(date +%s) - 86400 * 2 ))" +%Y-%m-%d)
end=$(date +%Y-%m-%d)

if ! curl -X POST "http://$ENDPOINT/v0/pipeline?start=${start}&end=${end}&step=all"; then
    echo "Running the pipeline failed, please check the container logs."
    exit 1
fi

echo "The pipeline completed successfully"
# Note: this is disabled until the maptiles generation can run on multiple
# years. Currently, 2020 is hardcoded and it would be pointless to regenerate
# the maptiles every time the stats-pipeline runs.
#export GCS_BUCKET=maptiles-${PROJECT}
#make piecewise
