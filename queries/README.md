join_geo_sampling.sql is the query used in scripts/generate_maptiles.sh.
FRACTION must be replaced with a numeric fraction, which is done using sed.

It is the same as join_geo.sql, which returns all records in the query, except
with the addition of a filter to sample a fraction of the rows.

join_state_geo.sql is an example query which joins the data for a single state,
given by STATENO. This too must be replaced, if used, by a two-digit number
representing a state (in alphabetical order).
