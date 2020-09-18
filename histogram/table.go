package histogram

import (
	"bytes"
	"context"
	"log"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
)

const (
	dateFormat    = "2006-01-02"
	deleteRowsTpl = "DELETE FROM {{.Table}} WHERE test_date BETWEEN \"{{.Start}}\" AND \"{{.End}}\""
)

// Table represents a bigquery table containing histogram data.
// It embeds bigquery.Table and extends it with an UpdateHistogram method.
type Table struct {
	bqiface.Table
	// Query is the generating query for this Table
	Query string

	// Client is the BigQuery client to use.
	Client bqiface.Client
}

// NewTable returns a new Table with the specified destination table, query
// and BQ client.
func NewTable(name string, ds string, query string, client bqiface.Client) *Table {
	return &Table{
		Table:  client.Dataset(ds).Table(name),
		Query:  query,
		Client: client,
	}
}

func (t *Table) queryConfig(query string) bqiface.QueryConfig {
	qc := bqiface.QueryConfig{}
	qc.Q = query
	return qc
}

// deleteRows removes rows where test_date is within the provided range.
func (t *Table) deleteRows(ctx context.Context, start, end time.Time) error {
	// TODO: partition table and delete per-day partitions, which is likely
	// much more efficient.
	tpl := template.Must(template.New("query").Parse(deleteRowsTpl))
	q := &bytes.Buffer{}
	err := tpl.Execute(q, map[string]string{
		"Table": t.DatasetID() + "." + t.TableID(),
		"Start": start.Format(dateFormat),
		"End":   end.Format(dateFormat),
	})
	if err != nil {
		return err
	}
	log.Printf("Deleting existing histogram rows: %s\n", q.String())
	query := t.Client.Query(q.String())
	_, err = query.Read(ctx)
	if err != nil {
		return err
	}

	return nil
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
	qc := t.queryConfig(t.Query)
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
	query := t.Client.Query(t.Query)
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
	if status.Err() != nil {
		return status.Err()
	}
	return nil
}
