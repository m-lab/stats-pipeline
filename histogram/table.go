package histogram

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/api/googleapi"
)

var (
	queryBytesProcessMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stats_pipeline_histograms_bytes_processed",
		Help: "Bytes processed by the histogram query",
	}, []string{
		"table",
	})
)

const (
	dateFormat    = "2006-01-02"
	deleteRowsTpl = "DELETE FROM {{.Table}} WHERE {{.DateField}} BETWEEN \"{{.Start}}\" AND \"{{.End}}\""
)

const (
	// TimePartitioning represents date-based partitioning.
	TimePartitioning = "date"

	// RangePartitioning represents range-based partitioning.
	RangePartitioning = "range"
)

type QueryConfig struct {
	// Query is the SQL Query to run.
	Query string

	// DateField is the field to use to determine which rows must be deleted
	// on a table update. It can be the same as partitionField, or different.
	DateField string

	// PartitionField is the field to use for date or range partitioning.
	PartitionField string

	// PartitionType is the type of partitioning to use (date or range).
	PartitionType string
}

// Table represents a bigquery table containing histogram data.
// It embeds bigquery.Table and extends it with an UpdateHistogram method.
type Table struct {
	bqiface.Table

	// config is the configuration for the query generating this table.
	config QueryConfig

	// client is the bigquery client used to execute the query.
	client bqiface.Client
}

// NewTable returns a new Table with the specified destination table, query
// and BQ client.
func NewTable(name string, ds string, config QueryConfig,
	client bqiface.Client) *Table {
	return &Table{
		Table:  client.Dataset(ds).Table(name),
		config: config,
		client: client,
	}
}

func (t *Table) queryConfig(query string) bqiface.QueryConfig {
	qc := bqiface.QueryConfig{}
	qc.Q = query
	return qc
}

// deleteRows removes rows where dateField is within the provided range.
func (t *Table) deleteRows(ctx context.Context, start, end time.Time) error {
	tpl := template.Must(template.New("query").Parse(deleteRowsTpl))
	q := &bytes.Buffer{}
	err := tpl.Execute(q, map[string]string{
		"Table":     t.DatasetID() + "." + t.TableID(),
		"DateField": t.config.DateField,
		"Start":     start.Format(dateFormat),
		"End":       end.Format(dateFormat),
	})
	if err != nil {
		return err
	}
	// Check that table exists.
	_, err = t.client.Dataset(t.DatasetID()).Table(t.TableID()).Metadata(ctx)
	if e, ok := err.(*googleapi.Error); ok && e.Code == http.StatusNotFound {
		// deleting rows from a table that does not exist is a no-op. So, return
		// without error.
		return nil
	}
	log.Printf("Deleting existing histogram rows: %s\n", q.String())
	query := t.client.Query(q.String())
	_, err = query.Read(ctx)
	if err != nil {
		log.Printf("Warning: cannot remove previous rows (%v)", err)
	}
	return err
}

// UpdateHistogram generates the histogram data for the specified time range.
// If any data for this time range exists already, it's overwritten.
func (t *Table) UpdateHistogram(ctx context.Context, start, end time.Time) error {
	log.Printf("Updating table %s\n", t.TableID())

	// Make sure there aren't multiple histograms for this date range by
	// removing any previously inserted rows.
	err := t.deleteRows(ctx, start, end)
	if err != nil {
		return err
	}

	// Configure the histogram generation query.
	qc := t.queryConfig(t.config.Query)
	switch t.config.PartitionField {
	case RangePartitioning:
		qc.RangePartitioning = &bigquery.RangePartitioning{
			Field: t.config.PartitionField,
			Range: &bigquery.RangePartitioningRange{
				Start:    0,
				End:      3999,
				Interval: 1,
			},
		}
	case TimePartitioning:
		qc.TimePartitioning = &bigquery.TimePartitioning{
			Field: t.config.PartitionField,
		}
	}

	qc.Dst = t.Table
	qc.WriteDisposition = bigquery.WriteAppend
	qc.Parameters = []bigquery.QueryParameter{
		{
			Name:  "startdate",
			Value: start.Format(dateFormat),
		},
		{
			Name:  "enddate",
			Value: end.Format(dateFormat),
		},
	}
	query := t.client.Query(t.config.Query)
	query.SetQueryConfig(qc)

	// Run the histogram generation query.
	log.Printf("Generating histogram data for table %s\n", t.TableID())
	bqJob, err := query.Run(ctx)
	if err != nil {
		return err
	}
	status, err := bqJob.Wait(ctx)
	if err != nil {
		return err
	}
	// Get bytes processed by the current query.
	queryBytesProcessMetric.WithLabelValues(t.Table.FullyQualifiedName()).
		Add(float64(status.Statistics.TotalBytesProcessed))
	if status.Err() != nil {
		return status.Err()
	}
	return nil
}
