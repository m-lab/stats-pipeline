# Geographic Precision in `stats-pipeline`
Understanding how location in NDT test results are identified is important when
looking at aggregations of that data by geography.

* NDT tests are conducted between a person or device testing (client) to an
  available M-Lab server.
* The NDT measurements, and the IP address of the client are collected on the
  server, and pushed to our central archives.
* Along the way, the results are annotated using the IP address as a lookup key,
  in publicly available datasets like Maxmind.
* Location fields in NDT data represent the locations of ISP's equipment that
  hands out IP addresses, not the address or GPS location of the client.

In general, aggregate NDT data should be considered advisory for geographic
areas smaller than the ISO 3166-2 second level subdivisions within a country.
For example in the US, the first level in the ISO 3166-2 standard corresponds to
US states. The US does not identify second level subdivisions in this standard,
therefore M-Lab recommends that US geographic aggregations at the county level
as the smallest level of geography appropriate for this dataset, given current
understanding.
