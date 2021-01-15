# This Dockerfile builds a minimal container with nodejs and the dependencies
# to run the maptiles generation.

# Build tippecanoe since it isn't included in the Alpine repo.
FROM alpine:3.12 AS build-tippecanoe

ARG TIPPECANOE_RELEASE="1.30.0"

RUN mkdir -p /tmp/tippecanoe-src
WORKDIR /tmp/tippecanoe-src

RUN apk add --no-cache git make g++ libgcc libstdc++ sqlite-libs sqlite-dev zlib-dev bash

RUN git clone https://github.com/mapbox/tippecanoe.git tippecanoe \
 && cd tippecanoe \
 && git checkout tags/$TIPPECANOE_RELEASE \ 
 && make -j$(nproc) \
 && make install

# The actual container is based on the official node base image.
FROM google/cloud-sdk:alpine

# Copy the only binary we need from the tippecanoe container.
COPY --from=build-tippecanoe /usr/local/bin/tippecanoe /usr/local/bin/tippecanoe

# Make sure we have make, nodejs/npm and tippecanoe's dependencies.
RUN apk add --no-cache libstdc++ sqlite-libs make nodejs npm ca-certificates

COPY ./ /home/node
WORKDIR /home/node

# These modules are required by the maptiles generation scripts.
RUN npm install -g mapshaper
RUN npm install