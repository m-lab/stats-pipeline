#!/bin/bash
#
# create_statistics_api.sh creates all GCP resources needed to serve the statistics
# API from a GCS bucket.

set -euxo pipefail
PROJECT=${1:?Please provide project}

# Create statistics GCS bucket.
statistics_bucket="statistics-${PROJECT}"
if ! gsutil acl get "gs://${statistics_bucket}" &> /dev/null ; then
  gsutil mb -p ${PROJECT} -l us-central1 "gs://${statistics_bucket}"
  gsutil defacl set public-read "gs://${statistics_bucket}"
fi

# Apply CORS settings to the statistics bucket.
gsutil cors set cors-settings.json gs://${statistics_bucket}

# Lookup or create loadbalancer IP.
lb_ip=$(
  gcloud --project ${PROJECT} compute addresses describe \
    statistics-lb-ip --global --format="value(address)" || :
)
if [[ -z "${lb_ip}" ]] ; then
  lb_ip=$(
    gcloud --project ${PROJECT} compute addresses create \
      statistics-lb-ip --ip-version=IPV4 --global --format="value(address)"
  )
fi

# Lookup or create the backend bucket for the statistics data bucket.
statistics_backend_name=$(
  gcloud --project ${PROJECT} compute backend-buckets describe \
    statistics-bucket --format='value(name)' || :
)
if [[ -z "${statistics_backend_name}" ]] ; then
  statistics_backend_name=$(
    gcloud --project ${PROJECT} compute backend-buckets create \
      statistics-bucket \
      --gcs-bucket-name ${statistics_bucket} --format='value(name)'
  )
fi

# Create url-map.
urlmap_name=$(
  gcloud --project ${PROJECT} compute url-maps describe \
    statistics-url-map --format='value(name)' || :
)
if [[ -z "${urlmap_name}" ]] ; then
  urlmap_name=$(
    gcloud --project ${PROJECT} compute url-maps create \
      statistics-url-map \
      --default-backend-bucket=${statistics_backend_name} \
      --format='value(name)'
  )
fi

# Setup DNS for statistics.<project>.measurementlab.net.
current_ip=$(
  gcloud dns record-sets list --zone "${PROJECT}-measurementlab-net" \
    --name "statistics.${PROJECT}.measurementlab.net." \
    --format "value(rrdatas[0])" --project ${PROJECT} || : )
if [[ "${current_ip}" != "${lb_ip}" ]] ; then
  # Add the record, deleting the existing one first.
  gcloud dns record-sets transaction start \
    --zone "${PROJECT}-measurementlab-net" \
    --project ${PROJECT}
  # Allow remove to fail when CURRENT_IP is empty.
  gcloud dns record-sets transaction remove \
    --zone "${PROJECT}-measurementlab-net" \
    --name "statistics.${PROJECT}.measurementlab.net." \
    --type A \
    --ttl 300 \
    "${current_ip}" --project ${PROJECT} || :
  gcloud dns record-sets transaction add \
    --zone "${PROJECT}-measurementlab-net" \
    --name "statistics.${PROJECT}.measurementlab.net." \
    --type A \
    --ttl 300 \
    "${lb_ip}" \
    --project ${PROJECT}
  gcloud dns record-sets transaction execute \
    --zone "${PROJECT}-measurementlab-net" \
    --project ${PROJECT}
fi

# Create managed TLS certificates.
certificate_name=$(
  gcloud --project ${PROJECT} beta compute ssl-certificates describe \
    statistics-certificate --format='value(name)' || :
)
if [[ -z "${certificate_name}" ]] ; then
  certificate_name=$(
    gcloud --project ${PROJECT} beta compute ssl-certificates create \
      statistics-certificate \
      --domains statistics.${PROJECT}.measurementlab.net --format='value(name)'
  )
fi

# Create the HTTPS target proxy connecting the url-map and managed certificate.
proxy_name=$(
  gcloud --project ${PROJECT} compute target-https-proxies describe \
    statistics-lb-proxy --format='value(name)' || :
)
if [[ -z "${proxy_name}" ]] ; then
  proxy_name=$(
    gcloud --project ${PROJECT} compute target-https-proxies create \
      statistics-lb-proxy \
      --url-map ${urlmap_name} --ssl-certificates ${certificate_name} \
      --format='value(name)'
  )
fi

# Create the forwarding rule connecting our loadbalancer IP to the target proxy.
forwarder_name=$(
  gcloud --project ${PROJECT} compute forwarding-rules describe \
    statistics-forwarder --global --format='value(name)' || :
)
if [[ -z "${forwarder_name}" ]] ; then
  gcloud --project ${PROJECT} compute forwarding-rules create \
    statistics-forwarder \
    --address ${lb_ip} --global \
    --target-https-proxy ${proxy_name} \
    --ports 443
fi
