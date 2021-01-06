
# download the block level shapefiles from the census
# they serve them as a single zip per state so we have
# to download them, unzip them, & convert them to geojson
geographies/blocks/.downloaded:
	mkdir -p geographies/blocks
	node scripts/download-blocks.js
	./scripts/unzip-blocks.sh
	mapshaper geographies/blocks/*.shp -o geographies/blocks/ format=geojson

	# clean up a bunch of the intermediary files because they are big!
	rm -f geographies/blocks/*.cpg
	rm -f geographies/blocks/*.dbf
	rm -f geographies/blocks/*.prj
	rm -f geographies/blocks/*.shp
	rm -f geographies/blocks/*.shx
	rm -f geographies/blocks/*.xml
	rm -f geographies/blocks/*.zip

	touch geographies/blocks/.downloaded

# merge the block level geojson files into tract level files
geographies/tracts: geographies/blocks/.downloaded
	mkdir -p geographies/tracts
	mapshaper -i geographies/blocks/tl*.json -dissolve TRACTCE10 copy-fields STATEFP10,COUNTYFP10 -o geographies/tracts/ format=geojson

# and merge all of the tract level shapefiles into one national file
geographies/tracts-geo.json: geographies/tracts
	mapshaper -i geographies/tracts/*.json merge-files -each 'fips = STATEFP10 + "" + COUNTYFP10 + TRACTCE10' -o geographies/tracts-geo.json format=geojson

fcc/477-without-satellite-june-2019.csv:
	mkdir -p fcc
	wget -c --output-document=fcc/477-without-satellite-june-2019.csv https://opendata.fcc.gov/api/views/sgz3-kiqt/rows.csv?accessType=DOWNLOAD&sorting=true

# this is just to make this file smaller and easier to deal
# with. the source file is some 29M rows
fcc/test.csv: fcc/477-without-satellite-june-2019.csv
	head -n 5000 fcc/477-without-satellite-june-2019.csv > fcc/test.csv

# load fcc 477 into sqlite so that we can easily
# query it by different geography types
fcc-477.sqlite: fcc/477-without-satellite-june-2019.csv
	rm -f fcc-477.sqlite
	node scripts/load-fcc-477-to-db.js fcc/477-without-satellite-june-2019.csv

# summarize 477 data by block
fcc-block.json: fcc-477.sqlite
	node scripts/process-fcc.js block

# summarize 477 data by county
fcc-county.json: fcc-477.sqlite
	node scripts/process-fcc.js county

# summarize 477 data by tract
fcc-tract.json: fcc-477.sqlite
	node scripts/process-fcc.js tract

# download mlab statistics from gcs
mlab/counties/.downloaded:
	mkdir -p mlab/counties
	node scripts/download-mlab.js counties
	touch mlab/counties/.downloaded

mlab/tracts/.downloaded:
	mkdir -p mlab/tracts
	node scripts/download-mlab.js tracts
	touch mlab/tracts/.downloaded

# and then summarize the mlab statistics	
mlab-counties.json: mlab/counties/.downloaded
	node scripts/process-mlab.js counties

mlab-tracts.json: mlab/tracts/.downloaded
	node scripts/process-mlab.js tracts

# merge county data with census demographic information
counties-geo-with-demographics.json: geographies/counties-geo.json census/county-demographics-acs-5.csv
	mapshaper geographies/counties-geo.json \
		-join census/county-demographics-acs-5.csv keys=fips,geo_id field-types=fips:str,geo_id:str \
		-each 'white_pct=white_pop / total_pop' \
		-each 'black_pct=black_pop / total_pop' \
		-each 'asian_pct=asian_pop / total_pop' \
		-each 'hispanic_pct=hispanic_pop / total_pop' \
		-each 'amerindian_pct=amerindian_pop / total_pop' \
		-o format=geojson \
		counties-geo-with-demographics.json

# merge the above county file with acs internet info
counties-geo-with-census.json: counties-geo-with-demographics.json census/county-internet-acs-5.csv
	mapshaper counties-geo-with-demographics.json \
		-join census/county-internet-acs-5.csv keys=fips,fips field-types=fips:str,fips:str \
		-o format=geojson \
		counties-geo-with-census.json

# merge the above county file with fcc 477 data
counties-geo-with-census-fcc.json: counties-geo-with-census.json fcc-county.json
	mapshaper counties-geo-with-census.json \
		-join fcc-county.json keys=fips,geo_id field-types=fips:str,geo_id:str \
		-o format=geojson \
		counties-geo-with-census-fcc.json

# and finally, merge in county level mlab data. this file will
# have census demographics, acs internet info, fcc 477 data,
# and mlab statistics
counties-geo-with-census-fcc-mlab.json: counties-geo-with-census-fcc.json mlab-counties.json
	mapshaper counties-geo-with-census-fcc.json \
		-join mlab-counties.json keys=fips,geo_id field-types=fips:str,geo_id:str \
		-o format=geojson \
		counties-geo-with-census-fcc-mlab.json

# and do the same thing for tracts

# merge tract data with census demographic information
tracts-geo-with-demographics.json: geographies/tracts-geo.json census/tract-demographics-acs-5.csv
	mapshaper geographies/tracts-geo.json \
		-join census/tract-demographics-acs-5.csv keys=fips,geo_id field-types=fips:str,geo_id:str \
		-each 'white_pct=white_pop / total_pop' \
		-each 'black_pct=black_pop / total_pop' \
		-each 'asian_pct=asian_pop / total_pop' \
		-each 'hispanic_pct=hispanic_pop / total_pop' \
		-each 'amerindian_pct=amerindian_pop / total_pop' \
		-o format=geojson \
		tracts-geo-with-demographics.json

tracts-geo-with-census.json: tracts-geo-with-demographics.json census/tract-internet-acs-5.csv
	mapshaper tracts-geo-with-demographics.json \
		-join census/tract-internet-acs-5.csv keys=fips,fips field-types=fips:str,fips:str \
		-o format=geojson \
		tracts-geo-with-census.json

# and now with FCC
tracts-geo-with-fcc.json: tracts-geo-with-census.json fcc-tract.json
	mapshaper tracts-geo-with-census.json \
		-join fcc-tract.json keys=fips,geo_id field-types=fips:str,geo_id:str \
		-o format=geojson \
		tracts-geo-with-fcc.json

tracts-geo-with-fcc-mlab.json: tracts-geo-with-fcc.json mlab-tracts.json
	mapshaper tracts-geo-with-fcc.json \
		-join mlab-tracts.json keys=fips,geo_id field-types=fips:str,geo_id:str \
		-o format=geojson \
		tracts-geo-with-fcc-mlab.json

# blocks.mbtiles: geographies/blocks/.downloaded
# 	./scripts/combine-blocks.sh

counties.mbtiles: counties-geo-with-census-fcc-mlab.json
	tippecanoe -z12 -f -o counties.mbtiles -l counties counties-geo-with-census-fcc-mlab.json

states.mbtiles:
	tippecanoe -z12 -f -o states.mbtiles -l states geographies/states-geo.json

tracts.mbtiles: tracts-geo-with-fcc-mlab.json
	tippecanoe -z12 -f -o tracts.mbtiles -l tracts --no-tile-size-limit -x STATEFP10 -x COUNTYFP10 -x TRACTCE10 tracts-geo-with-fcc-mlab.json

piecewise: counties.mbtiles states.mbtiles tracts.mbtiles
	tile-join -pk -pC -z10 -f -e piecewise counties.mbtiles states.mbtiles tracts.mbtiles
	gsutil -m cp -r piecewise gs://maptiles-mlab-sandbox
