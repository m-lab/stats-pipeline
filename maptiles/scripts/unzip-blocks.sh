#!/bin/bash

for FILE in $(ls geographies/blocks/*.zip);
do
  unzip -o -d geographies/blocks $FILE
done