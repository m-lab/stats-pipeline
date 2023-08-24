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
- `$HOME/.config/gcloud/:/root/.config/gcloud` - this provides access
  to your gcloud credentials. You must update the directory to your local home
  path.
- `./:/config` - this provides access to the configuration and BigQuery SQL
  files in this repo.

NOTE: Depending on your version of docker-compose, you may need to replace
`$HOME` with your actual local directory name.

You may run a local instance of the annotation export stats pipeline using
docker-compose:

```sh
docker-compose -f compose-annotation-export.yaml build
docker-compose -f compose-annotation-export.yaml up
```

NOTE: this will upload sample archives to the configured GCS bucket in
mlab-sandbox. Those files don't matter b/c it's sandbox, but be careful to
distinguish between new files created from your run and archives from previous
runs.

You may trigger the export process using:

```sh
curl -XPOST --data {} 'http://localhost:8080/v0/pipeline?step=exports&year=1'
```

Only the 'export' step is supported for annotation export, and the year is
required but ignored.

## Kubernetes

Only the hopannotation1 export process is available on Kubernetes. The export process is started via a CronJob which by default is scheduled to
never run.

To start an annotation export manually, run the following command on the
`data-pipeline` cluster:

```sh
kubectl create job --from=cronjob/hopannotation1-export-cronjob hopannotation1-export-manual
```
