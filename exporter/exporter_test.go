package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"reflect"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/m-lab/stats-pipeline/formatter"

	"github.com/m-lab/stats-pipeline/output"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/go/cloudtest/bqfake"
	"github.com/m-lab/go/cloudtest/gcsfake"
	"github.com/m-lab/go/prometheusx/promtest"
	"github.com/m-lab/go/testingx"
	"github.com/m-lab/go/uploader"
	"google.golang.org/api/iterator"
)

// BigQuery mocks to allow for testing with fake data.
// ***** mockClient *****
type mockClient struct {
	bqiface.Client

	// queryReadMustFail controls whether the Read() method will fail for
	// queries created by this client object.
	queryReadMustFail bool

	// queries stores every query run through this client so it can be
	// checked later in tests.
	queries []string

	// iterator is the fake iterator every query will return.
	// Allows to provide fake query results.
	iterator bqiface.RowIterator
}

func (c *mockClient) Dataset(name string) bqiface.Dataset {
	return &mockDataset{
		name: name,
	}
}

func (c *mockClient) Query(query string) bqiface.Query {
	return &mockQuery{
		client:       c,
		q:            query,
		readMustFail: c.queryReadMustFail,
		iterator:     c.iterator,
	}
}

// ***** mockDataset *****
type mockDataset struct {
	bqiface.Dataset
	name string
}

func (ds *mockDataset) Table(name string) bqiface.Table {
	return &mockTable{
		ds:   ds.name,
		name: name,
	}
}

// ***** mockTable *****
type mockTable struct {
	bqiface.Table
	ds   string
	name string
}

func (t *mockTable) DatasetID() string {
	return t.ds
}

func (t *mockTable) TableID() string {
	return t.name
}

// ********** mockQuery **********
type mockQuery struct {
	bqiface.Query
	client       *mockClient
	q            string
	qc           bqiface.QueryConfig
	readMustFail bool
	runMustFail  bool
	iterator     bqiface.RowIterator
}

func (q *mockQuery) Read(context.Context) (bqiface.RowIterator, error) {
	if q.readMustFail {
		return nil, errors.New("Read() failed")
	}
	// Store the query's content into the client so it can be checked later.
	q.client.queries = append(q.client.queries, q.q)
	return q.iterator, nil
}

func (q *mockQuery) SetQueryConfig(qc bqiface.QueryConfig) {
	q.qc = qc
}

// ***** mockRowIterator *****
type mockRowIterator struct {
	bqiface.RowIterator

	iterErr error
	rows    []map[string]bigquery.Value
	index   int
}

func (it *mockRowIterator) PageInfo() *iterator.PageInfo {
	return &iterator.PageInfo{}
}

func (it *mockRowIterator) Next(dst interface{}) error {
	// Check config for an error.
	if it.iterErr != nil {
		return it.iterErr
	}
	// Allow an empty config to return Done.
	if it.index >= len(it.rows) {
		return iterator.Done
	}
	v := dst.(*map[string]bigquery.Value)
	*v = it.rows[it.index]
	it.index++
	return nil
}

func (it *mockRowIterator) Reset() {
	it.index = 0
}

func TestNew(t *testing.T) {
	bq, err := bqfake.NewClient(context.Background(), "test")
	testingx.Must(t, err, "cannot init bq client")
	gcs := &gcsfake.GCSClient{}
	wr := output.NewGCSWriter(uploader.New(gcs, "test-bucket"))
	f := formatter.NewStatsQueryFormatter()
	exporter := New(bq, "project", wr, f)
	if exporter == nil {
		t.Fatalf("New() returned nil.")
	}
	if exporter.bqClient != bq || exporter.output != wr ||
		exporter.projectID != "project" {
		t.Errorf("New() didn't return the expected exporter instance")
	}
}

func TestJSONExporter_marshalAndUpload(t *testing.T) {
	exporter := &JSONExporter{
		format: formatter.NewStatsQueryFormatter(),
	}
	jobs := make(chan *UploadJob)
	fakeRow := bqRow{
		"field": "value",
	}
	rows := []bqRow{fakeRow}
	go func() {
		err := exporter.marshalAndUpload("tablename", "test", rows, jobs)
		if err != nil {
			t.Errorf("marshalAndUpload() returned err: %v", err)
		}
		close(jobs)
	}()
	// Read from the channel. If the channel is closed and nothing has been
	// sent, the function failed.
	if job, ok := <-jobs; ok {
		if job.objName != "test" || len(job.content) == 0 {
			t.Errorf("marshalAndUpload() didn't send the expected value")
		}
	} else {
		t.Errorf("marshalAndUpload() didn't send a value")
	}

	// Call marshalAndUpload with a row that cannot be marshalled.
	unmarshallableRow := bqRow{
		"this-will-fail": make(chan int),
	}
	rows = append(rows, unmarshallableRow)
	jobs = make(chan *UploadJob)
	go func() {
		err := exporter.marshalAndUpload("tablename", "this-will-fail", rows, jobs)
		if err == nil {
			t.Errorf("marshalAndUpload(): expected error, got nil")
		}
		close(jobs)
	}()
	// We don't expect anything on the channel.
	if _, ok := <-jobs; ok {
		t.Errorf("marshalAndUpload() sent an unexpected job after an error")
	}
}

func Test_printStats(t *testing.T) {
	out := new(bytes.Buffer)
	log.SetOutput(out)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bq, err := bqfake.NewClient(context.Background(), "test")
	testingx.Must(t, err, "cannot init bq client")
	gcs := &gcsfake.GCSClient{}
	wr := output.NewGCSWriter(uploader.New(gcs, "test-bucket"))
	f := formatter.NewStatsQueryFormatter()
	exporter := New(bq, "project", wr, f)

	go exporter.printStats(ctx, 1)
	// Send a successful upload and an error, then check the output after a
	// second.
	exporter.results <- UploadResult{
		objName: "test",
	}
	exporter.results <- UploadResult{
		objName: "failed",
		err:     errors.New("upload failed"),
	}

	time.Sleep(2 * time.Second)
	if !strings.Contains(out.String(), "uploaded: 1") ||
		!strings.Contains(out.String(), "1 errors") {
		t.Errorf("printStats() didn't print the expected output: %v", out.String())
	}
}

func Test_getFieldsFromPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    []string
		wantErr bool
	}{
		{
			name: "ok",
			path: "{{ .foo }}/{{.bar}}",
			want: []string{"foo", "bar"},
		},
		{
			name:    "no-matches",
			path:    "{{foo}}/{{bar}}",
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getFieldsFromPath(tt.path)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getFieldsFromPath() = %v, want %v", got, tt.want)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("exporter.getFieldsFromPath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_removeFieldsFromRow(t *testing.T) {
	fakeRow := bqRow{
		"test":   "foo",
		"remove": "this",
	}
	tests := []struct {
		name   string
		row    bqRow
		fields []string
		want   bqRow
	}{
		{
			name:   "ok-field-removed",
			row:    fakeRow,
			fields: []string{"remove"},
			want: bqRow{
				"test": "foo",
			},
		},
		{
			name:   "not-found-return-original-row",
			row:    fakeRow,
			fields: []string{"non-existing-field"},
			want:   fakeRow,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := removeFieldsFromRow(tt.row, tt.fields); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("removeFieldsFromRow() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPrometheusMetrics ensures that all the metrics pass the linter.
func TestPrometheusMetrics(t *testing.T) {
	bytesProcessedMetric.WithLabelValues("x")
	cacheHitMetric.WithLabelValues("x")
	uploadedBytesMetric.WithLabelValues("x")
	queryTotalMetric.WithLabelValues("x")
	queryProcessedMetric.WithLabelValues("x")
	inFlightUploadsHistogram.WithLabelValues("x")
	uploadQueueSizeHistogram.WithLabelValues("x")

	promtest.LintMetrics(t)
}

func TestJSONExporter_processQueryResults(t *testing.T) {
	exporter := &JSONExporter{
		uploadJobs: make(chan *UploadJob),
		format:     formatter.NewStatsQueryFormatter(),
	}
	// Create an iterator returning a fake row.
	it := &mockRowIterator{
		rows: []map[string]bigquery.Value{
			{
				"date":  "2020-01-01",
				"year":  2020,
				"shard": int64(1),
			},
		},
	}
	outputPathTpl := template.Must(template.New("path").Parse(
		"{{.year}}/output.json",
	))
	qJob := &QueryJob{
		name:       "test",
		query:      "SELECT * FROM test_table",
		fields:     []string{"year"},
		outputPath: outputPathTpl,
	}
	go exporter.processQueryResults(it, qJob)
	// Read the job sent on the uploadJobs channel and check its content.
	ul := <-exporter.uploadJobs
	var rows []map[string]json.RawMessage
	err := json.Unmarshal(ul.content, &rows)
	if err != nil {
		t.Errorf("Cannot unmarshal JSON: %v", err)
	}

	if len(rows) == 0 {
		t.Fatal("Output JSON is empty")
	}
	row := rows[0]
	if _, ok := row["shard"]; ok {
		t.Errorf("the 'shard' field has not been removed from the output")
	}
	if string(row["date"]) != `"2020-01-01"` {
		t.Errorf("wrong value for the date field: %v", string(row["date"]))
	}
	if string(row["year"]) != `2020` {
		t.Errorf("wrong value for the year field: %v", string(row["year"]))
	}
}
