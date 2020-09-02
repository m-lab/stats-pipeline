#!/bin/bash

CSV=${1?Please specify a csv file name}
CSVT="${CSV/csv/csvt}"
GEOJSON="${CSV/csv/geojson}"
SCHEMA=schema.csvt

if ! [ -f "${SCHEMA}" ] 
then 
  echo "`${SCHEMA}` is required"
  exit 1
fi

# ogr2ogr requires that the schema file have the same base name as
# the main csv file, with .csvt extension.
cp "${SCHEMA}" "${CSVT}"

ogr2ogr -f GeoJSON "${GEOJSON}" -oo KEEP_GEOM_COLUMNS=no "${CSV}"