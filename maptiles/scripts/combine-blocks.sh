#!/bin/bash

# mapshaper blocks/**/*.shp -o blocks/ format=geojson
# GEOJSON=$(ls blocks/tl_2019_06_tabblock10.json)
# for FILE in $GEOJSON;
# do
#   BASENAME=$(basename $FILE .json)
#   tippecanoe --force -l blocks -o blocks/$BASENAME.mbtiles $FILE
# done

# MBTILES=$(ls blocks/*.mbtiles)
MBTILES="geographies/blocks/tl_2019_06_tabblock10.mbtiles"
tile-join --no-tile-size-limit -o blocks.mbtiles $MBTILES