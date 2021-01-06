# Vectortiles

This pipeline generates a vector tileset for the map tab on the admin interface. It pulls data from the Census, the FCC, and Measurement Lab, creates the tileset, and uploads it to Google Cloud Storage. It runs to completion with `make piecewise`.


## Required tools
In addition to the things you normally need to run the `piecewise` project, you'll need the following things installed on your `PATH`. Installation instructions are in parenthesis.

1. `mapshaper` (`npm install -g mapshaper`)
1. `tippecanoe` (`brew install tippecanoe` or [other os instructions](https://github.com/mapbox/tippecanoe#installation))

It is recommended to use the same version of Node that `piecewise` uses. Some of this workflow's dependencies are installed through the top level `package.json`.

## Generated tileset

The `piecewise` tileset has three layers:

1. `states`
1. `counties`
1. `tracts`

Features in the `states` layer have the following properties:

* `fips` (Source: Census)
* `name` (Source: Census)

Features in both the `counties` and `tracts` layers have the following properties:

* `amerindian_pct` (Source: Census)
* `amerindian_pop` (Source: Census)
* `asian_pct` (Source: Census)
* `asian_pop` (Source: Census)
* `black_pct` (Source: Census)
* `black_pop` (Source: Census)
* `fips` (Source: Census)
* `hispanic_pct` (Source: Census)
* `hispanic_pop` (Source: Census)
* `households_with_broadband_moe` (Source: Census)
* `households_with_broadband_pct` (Source: Census)
* `households_without_internet_moe` (Source: Census)
* `households_without_internet_pct` (Source: Census)
* `median_income` (Source: Census)
* `name` (Source: Census)
* `total_pop` (Source: Census)
* `white_pct` (Source: Census)
* `white_pop` (Source: Census)
* `mean_max_ad_down` (Source: FCC)
* `mean_max_ad_up` (Source: FCC)
* `provider_count` (Source: FCC)
* `source_rows` (Source: FCC)
* `2020_jan_jun_median_dl` (Source: Measurement Lab)
* `2020_july_dec_median_dl` (Source: Measurement Lab)
* `2020_jan_jun_median_ul` (Source: Measurement Lab)
* `2020_july_dec_median_ul` (Source: Measurement Lab)
* `2020_jan_jun_percent_over_audio_threshold` (Source: Measurement Lab)
* `2020_july_dec_percent_over_audio_threshold` (Source: Measurement Lab)
* `2020_jan_jun_percent_over_video_threshold` (Source: Measurement Lab)
* `2020_july_dec_percent_over_video_threshold` (Source: Measurement Lab)
* `2020_jan_jun_total_dl_samples` (Source: Measurement Lab)
* `2020_july_dec_total_dl_samples` (Source: Measurement Lab)
* `2020_jan_jun_total_ul_samples` (Source: Measurement Lab)
* `2020_july_dec_total_ul_samples` (Source: Measurement Lab)