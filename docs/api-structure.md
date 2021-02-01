# Statistics API URL Structure, Available Geographies & Aggregations
The statistics API provides aggregations of NDT data, accessible using a well
defined URL structure provided at https://statistics.measurementlab.net/

## Versioning
At the top level, a version number is used to provide incremental releases of
the statistics API as new features are added. The current version of `stats-api`
is **v0**, for example: `https://statistics.measurementlab.net/**v0**/`

## Available Geographies and ASN Aggregations
The URL structure below defines the geographies available in `stats-api`. At
each level, aggregates by year and ASN are provided: 

### Global
* /v0/asn/<AS#####>/

At the global geographic level, we aggregate by [Autonomous System Number][asn]

[asn]: https://en.wikipedia.org/wiki/Autonomous_system_%28Internet%29

### Continent
* /v0/<continent_code>/<year>/
* /v0/<continent_code>/asn/<AS#####>/<year>/

Continents are represented by the two character continent code.

### Country
* /v0/<continent_code>/<country_code>/<year>/
* /v0/<continent_code>/<country_code>/asn/<AS#####>/<year>/

Countries are identified by their two character country code.

### ISO 3166-2 region level 1
* /v0/<continent_code>/<country_code>/<region_code>/<year>/
* /v0/<continent_code>/<country_code>/<region_code>/asn/<AS#####>/<year>/

The [ISO 3166-2 standard][iso-3166] is used to identify subdivisions with countries. This
code begins with the two character country code, appended with a hypen and up to
three alphanumeric characters.

[iso-3166]: https://en.wikipedia.org/wiki/ISO_3166-2

### United States County
* /v0/NA/US/counties/<GEOID>/<year>/
* /v0/NA/US/counties/<GEOID>/asn/<AS#####>/<year>/

United States Counties are identified using the shapefile polygons that define
them, obtained through the US Census Bureau. The `GEOID` of each test is found
by looking up the polygon that contains the test's annotated latitude and longitude.

### City
* /v0/<continent_code>/<country_code>/<region_code>/<city_name>/<year>/
* /v0/<continent_code>/<country_code>/<region_code>/<city_name>/asn/<AS#####>/<year>/

Cities are identified from the IP address annotations present in NDT data after
it is published.

## Accessing Statistics Using the stats-api
Using the API will depend largely on how you develop your application, but
accessing the statistics is a matter of knowing the geography and year of
interest, and using the appropriate URL pattern to access daily statistics.

For example, to get statistics for Maryland in 2020, we would use this URL: 
`https://statistics.measurementlab.net/v0/NA/US/US-MD/2020/histogram_daily_stats.json`

## Additional Geographies Provided for Advisory / Comparison Use Only
As mentioned in [Geographic Precision in `stats-pipeline`][geo-precision], NDT
data may be aggregated by any geography, but the precision of individual test
location annotations is limited to the precision of IP address geolocation. In
geographies that are quite small, aggregate data should be compared with other
datasets and used only in and advisory capacity.

One example in the US is the Census Tract. The geographies of tracts are quite
small, and to achieve address level precision would require the collection of
new NDT test data using a third party integration of the test that requests
location from the user in some way. A variety of [community-driven initiatives][community-tools]
are doing this, but these more accurately located tests are maintained by those
initiatives. However, seeing NDT data aggregated by census tract can be
generally useful as a point of comparison with other datasets. As such we
provide aggregation by US Census Tract for this type of use case.

### United States Census tracts
* /v0/NA/tracts/<GEOID>/<year>/
* /v0/NA/tracts/<GEOID>/asn/<AS#####>/<year>/

[geo-precision]: geo-precision.md
[community-tools]: https://www.measurementlab.net/data/tools/#community
