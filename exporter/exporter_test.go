package exporter

import (
	"context"
	"errors"
	"io/ioutil"
	"strings"
	"testing"
	"text/template"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/go/cloudtest/bqfake"
	"github.com/m-lab/go/cloudtest/gcsfake"
	"github.com/m-lab/go/testingx"
	"google.golang.org/api/iterator"
)

// BigQuery mocks to allow for testing with fake data.
// ***** mockClient *****
type mockClient struct {
	bqiface.Client

	// queryReadMustFail controls whether the Read() method will fail for
	// queries created by this client object.
	queryReadMustFail bool
	// queryRunMustFail controler whether the Run() method will fail for
	// queries created by this client object.
	queryRunMustFail bool

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
		runMustFail:  c.queryRunMustFail,
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

func TestNew(t *testing.T) {
	bq, err := bqfake.NewClient(context.Background(), "test")
	testingx.Must(t, err, "cannot init bq client")
	gcs := &gcsfake.GCSClient{}
	exporter := New(bq, gcs, "test-bucket")
	if exporter == nil {
		t.Fatalf("New() returned nil.")
	}
	if exporter.bqClient != bq || exporter.storageClient != gcs ||
		exporter.bucket != "test-bucket" {
		t.Errorf("New() didn't return the expected exporter instance")
	}
}

func TestJSONExporter_Export(t *testing.T) {
	// Create an iterator returning a fake row.
	it := &mockRowIterator{
		rows: []map[string]bigquery.Value{
			{
				"date":  "2020-01-01",
				"field": "test",
				"year":  2020,
				"histograms": []map[string]bigquery.Value{
					{"bucket_min": "0"},
				},
			},
		},
	}
	// Queries run via this BQ client always return the fake iterator created
	// above.
	bq := &mockClient{
		iterator: it,
	}
	// A fake GCS client and bucket to export data to.
	gcs := &gcsfake.GCSClient{}
	fakeBucket := gcsfake.NewBucketHandle()
	gcs.AddTestBucket("test", fakeBucket)

	queryTpl := template.Must(template.New("queryTpl").
		Parse("SELECT * FROM {{ .sourceTable }} {{ .whereClause }}"))
	outputPathTpl := template.Must(template.New("outputPathTpl").
		Parse("v0/{{ .field }}/{{ .year }}/histogram_daily_stats.json"))

	gen := &JSONExporter{
		bqClient:      bq,
		storageClient: gcs,
		bucket:        "test",
	}
	err := gen.Export(context.Background(), queryTpl, map[string]string{
		"sourceTable": "test",
		"whereClause": "",
	}, outputPathTpl)
	if err != nil {
		t.Fatalf("Export() returned an error: %v", err)
	}
	reader, err := fakeBucket.Object(
		"v0/test/2020/histogram_daily_stats.json").NewReader(
		context.Background())
	content, err := ioutil.ReadAll(reader)
	testingx.Must(t, err, "cannot read from GCS object")
	if string(content) != `[{"bucket_min":"0"}]` {
		t.Errorf("Export() wrote unexpected data on GCS: %v", string(content))
	}
	it.index = 0

	// If template execution fails, Export() should return the error.
	failingTpl := template.Must(template.New("fail").Parse("{{nil}}"))
	err = gen.Export(context.Background(), failingTpl, nil, outputPathTpl)
	if err == nil || !strings.Contains(err.Error(), "not a command") {
		t.Errorf("Export() didn't return the expected error: %v", err)
	}
	// Make the iterator return an error.
	it.iterErr = errors.New("iterator error")
	err = gen.Export(context.Background(), queryTpl, nil, outputPathTpl)
	if !errors.Is(err, it.iterErr) {
		t.Errorf("Export() didn't return the expected error: %v", err)
	}
	it.iterErr = nil
	// Replace client with one whose queries always fail.
	gen.bqClient = &mockClient{
		queryReadMustFail: true,
	}
	err = gen.Export(context.Background(), queryTpl, nil, outputPathTpl)
	if err == nil || !strings.Contains(err.Error(), "Read() failed") {
		t.Errorf("Export() didn't return the expected error: %v", err)
	}
	gen.bqClient = bq
	// Feed json.Marshal a row that cannot be marshalled.
	// Note: this is not something that can really happen when data comes
	// from BigQuery, I think.
	badDataIt := &mockRowIterator{
		rows: []map[string]bigquery.Value{
			{
				"date":  "2020-01-01",
				"field": "test",
				"year":  2020,
				"histograms": []map[string]bigquery.Value{
					{"this-will-fail": make(chan int)},
				},
			},
		},
	}
	bq.iterator = badDataIt
	err = gen.Export(context.Background(), queryTpl, nil, outputPathTpl)
	if !strings.Contains(err.Error(), "unsupported type") {
		t.Errorf("Export() didn't return the expected error: %v", err)
	}
	bq.iterator = it
	// Bad output template.
	failingOutputPathTpl := template.Must(template.New("fail").Parse("{{nil}}"))
	err = gen.Export(context.Background(), queryTpl, nil, failingOutputPathTpl)
	if err == nil || !strings.Contains(err.Error(), "not a command") {
		t.Errorf("Export() didn't return the expected error: %v", err)
	}
	it.index = 0
	// Make writes to the output path fail.
	fakeBucket.Object(
		"v0/test/2020/histogram_daily_stats.json").(*gcsfake.ObjectHandle).
		WritesMustFail = true
	err = gen.Export(context.Background(), queryTpl, nil, outputPathTpl)
	if err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Errorf("Export() didn't return the expected error: %v", err)

	}
	fakeBucket.Object(
		"v0/test/2020/histogram_daily_stats.json").(*gcsfake.ObjectHandle).
		WritesMustFail = false
}
