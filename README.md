# Statistics Pipeline Service
This repository contains code that processes NDT data and provides aggregate
metrics by day for standard global, and some national geographies. The resulting
aggregations are made available in JSON format, for use by other applications.

The `stats-pipeline` service is written in Go, runs on GKE, and generates and
updates daily aggregate statistics. Access is provided in public BigQuery tables
and in per-year JSON formatted files hosted on GCS.

## Documentation Provided for the Statistics Pipeline Service
* (This document) Overview of the `stats-pipeline` service, fields provided
  (schema), output formats, available geographies, and API URL structure.
* [What Statistics are Provided by stats-pipeline, and How are They Calculated?][stats-overview]
* [Geographic Precision in stats-pipeline][geo-precision]
* [Statistics Output Format, Schema, and Field Descriptions][format-schema]
* [Statistics API URL Structure, Available Geographies & Aggregations][api-structure]

[stats-overview]: docs/stats-overview.md
[geo-precision]: docs/geo-precision.md
[format-schema]: docs/format-schema.md
[api-structure]: docs/api-structure.md

## General Recommendations for All Aggregations of NDT data
In general, [our recommendations][recommendations] for research aggregating NDT data are:

* Don't oversimplify
* Aggregate by ASN in addition to time/date and location
* Be aware and illustrate multimodal distributions
* Use histogram and logarithmic scales
* Take into account, and compensate for, client bias and population drift

[recommendations]: upcoming-blog-post

## Roadmap
Below we list additional features, methods, geographies, etc. which may be
considered for future versioned releases of `stats-pipeline`.

### Geographies
* US Zip Codes, US Congressional Districts, Block Groups, Blocks

### Output Formats
* histogram_daily_stats.csv - Same data as the JSON, but in CSV. Useful for importing into a spreadsheet.
* histogram_daily_stats.sql - A SQL query which returns the same rows in the corresponding .json and .csv. Useful for verifying the exported data against the source and to tweak the query as needed by different use cases. 
