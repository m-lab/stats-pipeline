# Annotation Export

Directions for running the stats-pipeline for annotation export using the
alternate `config-annotation-export.json`.

## Local development

The `compose-annotation-export.yaml` specifies a docker compose configuration
for running the stats-pipeline with an instance of pusher. Both services are
able to use your local gcloud application default credentials.

There are three "volumes" of interest:

- `shared:/var/spool/ndt` - this is shared between the two containers,
  stats-pipeline writes files, and the pusher archives, uploads, and removes them.
- `/Users/soltesz/.config/gcloud/:/root/.config/gcloud` - this provides access
  to your gcloud credentials. You must update the directory to your local home
  path.
- `/Users/soltesz/src/github.com/m-lab/stats-pipeline:/config` - this provides
  access to the configuration and BigQuery SQL files in this repo.

After updating the docker compose file to refer to your local home directories,
you may run a local instance of the annotation export stats pipeline using
docker-compose:

```sh
docker-compose -f compose-annotation-export.yaml build
docker-compose -f compose-annotation-export.yaml up
```

NOTE: this will upload sample archives to the configured GCS bucket in
mlab-sandbox. Those files don't matter b/c it's sandbox, but be careful to
distinguish between new files created from your run and archives from previous
runs.

## Kubernetes

TODO(soltesz): add notes for kubernetes configuration/deployment.
