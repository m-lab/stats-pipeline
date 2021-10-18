package types

import (
	"context"
	"errors"
	"text/template"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/stats-pipeline/config"
)

// Writer defines the interface for saving files to GCS or locally.
type Writer interface {
	Write(ctx context.Context, path string, content []byte) error
}

// JSONExporter is a JSON exporter for histogram data on BigQuery.
type JSONExporter struct{}

// New creates a new JSONExporter.
func New(bqClient bqiface.Client, projectID string, output Writer) *JSONExporter {
	return &JSONExporter{}
}

// Export executes the given query template based on the given config.
func (exporter *JSONExporter) Export(ctx context.Context, config config.Config, queryTpl *template.Template, year string) error {
	return errors.New("not implemented")
}
