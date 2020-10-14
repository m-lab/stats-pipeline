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

type JSONExporter struct {
	bqClient      bqiface.Client
	storageClient stiface.Client

	bucket string
}

// New generates a new JSONExporter
func New(bqClient bqiface.Client, storageClient stiface.Client, bucket string) *JSONExporter {
	return &JSONExporter{
		bqClient:      bqClient,
		storageClient: storageClient,
		bucket:        bucket,
	}
}

func (gen *JSONExporter) Export(ctx context.Context,
	selectQuery *template.Template, params map[string]string,
	outputPath *template.Template) error {
	// Interpolate query template
	buf := new(bytes.Buffer)
	err := selectQuery.Execute(buf, params)
	if err != nil {
		return err
	}
	log.Printf("DEBUG: select query: %s", buf.String())
	// Run the SELECT query to get histogram data
	q := gen.bqClient.Query(buf.String())
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
