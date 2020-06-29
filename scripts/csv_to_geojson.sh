#!/bin/bash

ogr2ogr -f GeoJSON "${1%*.csv}.geojson" -oo KEEP_GEOM_COLUMNS=no "$1"
