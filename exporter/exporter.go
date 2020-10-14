package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"text/template"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/go/uploader"
	"google.golang.org/api/iterator"
)

// JSONExporter is a JSON exporter for histogram data on BigQuery.
type JSONExporter struct {
	bqClient      bqiface.Client
	storageClient stiface.Client

	bucket string
}

// New generates a new JSONExporter.
func New(bqClient bqiface.Client, storageClient stiface.Client, bucket string) *JSONExporter {
	return &JSONExporter{
		bqClient:      bqClient,
		storageClient: storageClient,
		bucket:        bucket,
	}
}

// Export runs the provided SQL query and, for each row in the result, uploads
// a file to the provided outputPath on GCS. This file contains the JSON
// representation of the "histograms" field, which must be present on each row.
// The outputPath is a template whose parameters are provided by the BigQuery
// row's fields.
// e.g. if outputPath is "{{ .Year }}/output.json" and we have a row per year,
// the histograms will be uploaded to:
// - 2010/output.json
// - 2020/output.json
// - etc.
//
// If any of the steps (running the query, reading the result, marshalling,
// uploading) fails, this function returns the corresponding error.
func (gen *JSONExporter) Export(ctx context.Context,
	selectQuery string, outputPath *template.Template) error {
	// Run the SELECT query to get histogram data
	q := gen.bqClient.Query(selectQuery)
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}
	// Iterate over the returned rows and upload results to GCS.
	type bqRow = map[string]bigquery.Value
	for {
		var row bqRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		j, err := json.Marshal(row["histograms"])
		if err != nil {
			return err
		}
		buf := new(bytes.Buffer)
		err = outputPath.Execute(buf, row)
		if err != nil {
			log.Printf("Cannot generate path (tpl:%s): %v",
				outputPath.Root.String(), err)
			return err
		}
		log.Printf("Uploading %s...", buf.String())
		up := uploader.New(gen.storageClient, gen.bucket)
		_, err = up.Upload(ctx, buf.String(), j)
		if err != nil {
			return err
		}
	}
	return nil
}
