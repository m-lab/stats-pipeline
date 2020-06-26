#!/bin/bash 

INPUT=${1:-/dev/stdin}

## Use xsv to guess the value types for the columns, then adjust the
## output format with shell text utilities to produce the .csvt format
## used in ogr2ogr to define column types for csv.

# xsv stats generates a report in csv of fieldname and type, among other things
xsv stats "${INPUT}" |
# select only the statistics fields we care about
xsv select 'field,type' |
# drop the headers (open the csv file with headers disabled, slices starting from row 1)
xsv slice -n -s 1 |
# Force WKT field type as "WKT", reword other types from WKT naming to CSVT naming. Float->Real,Unicode->String
sed -e 's/WKT,.*$/WKT,WKT/' \
    -e 's/Float$/Real/' \
    -e 's/Unicode$/String/' \
    -e 's/.*,//' |
# Drop the trailing newline
head -c -1 |
# Replace all newlines with commas
tr '\n' ',';
# Add back the newline
echo