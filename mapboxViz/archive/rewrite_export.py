#!/usr/bin/env python3

import csv
import json
import sys
from functools import reduce
from os import path

input_file = sys.argv[1]
(base, ext) = path.splitext(input_file)
assert ext == '.json', "Expected json file but extension is {}".format(ext)

output_file = "{}.csv".format(base)

replace_keys = {
    'district_geom': 'WKT',
    'dl_ip_count': 'dl_count_ips',
    'ul_ip_count': 'ul_count_ips',
    'dl_test_count': 'dl_count_tests',
    'ul_test_count': 'ul_count_tests',
    'dl_tx_Mbps': 'download_Mbps',
    'ul_tx_Mbps': 'upload_Mbps',
    'dl_min_rtt': 'min_rtt',
    'legal_area_name': 'name',
    'geo_id': 'GEOID'
}

def rewrite_row(json_row):
    "Flattens a json row into a dict with scalar values"
    row = dict()
    for k, v in json_row.items():
        if k in ('dl', 'ul', 'slice'):
            for time_slice in v:
                time_period = time_slice.get('time_period')
                if not time_period:
                    continue
                for k2, v2 in time_slice.items():
                    if k2 != 'time_period':
                        name = '{}_{}'.format(k,k2)
                        name = replace_keys.get(name,name)
                        row["ml_{}_{}".format(name, time_period)] = v2
        else:
            k = replace_keys.get(k, k)
            row[k] = v
    return row

with open(input_file) as results_file:
    results = [json.loads(line) for line in results_file.readlines()]

fields = reduce(lambda a, b: a | b, (set(rewrite_row(r)) for r in results))
fields = list(fields)
if 'WKT' in fields:
    fields.remove('WKT') # remove, then append to ensure WKT is last column.
    fields.sort()
    fields.append('WKT')

with open(output_file, 'w') as output:
    writer = csv.DictWriter(output, fields)
    writer.writeheader()
    writer.writerows(rewrite_row(r) for r in results)

print(fields)