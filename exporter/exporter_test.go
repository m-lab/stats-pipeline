package exporter

import (
	"bytes"
	"context"
	"errors"
	"log"
	"reflect"
	"strings"
	"testing"
	"time"

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
	exporter := New(bq, gcs, "test-bucket")
	if exporter == nil {
		t.Fatalf("New() returned nil.")
	}
	if exporter.bqClient != bq || exporter.storageClient != gcs ||
		exporter.bucket != "test-bucket" {
		t.Errorf("New() didn't return the expected exporter instance")
	}
}

// func TestJSONExporter_Export(t *testing.T) {
// 	const selectQuery = "SELECT * FROM test"

// 	// Create an iterator returning a fake row.
// 	it := &mockRowIterator{
// 		rows: []map[string]bigquery.Value{
// 			{
// 				"date":  "2020-01-01",
// 				"field": "test",
// 				"year":  2020,
// 				"histograms": []map[string]bigquery.Value{
// 					{"bucket_min": "0"},
// 				},
// 			},
// 		},
// 	}
// 	// Queries run via this BQ client always return the fake iterator created
// 	// above.
// 	bq := &mockClient{
// 		iterator: it,
// 	}
// 	// A fake GCS client and bucket to export data to.
// 	gcs := &gcsfake.GCSClient{}
// 	fakeBucket := gcsfake.NewBucketHandle()
// 	gcs.AddTestBucket("test", fakeBucket)

// 	outputPathTpl := template.Must(template.New("outputPathTpl").
// 		Parse("v0/{{ .field }}/{{ .year }}/histogram_daily_stats.json"))

// 	gen := New(bq, gcs, "test")
// 	err := gen.Export(context.Background(), selectQuery, outputPathTpl)
// 	if err != nil {
// 		t.Fatalf("Export() returned an error: %v", err)
// 	}
// 	reader, err := fakeBucket.Object(
// 		"v0/test/2020/histogram_daily_stats.json").NewReader(
// 		context.Background())
// 	content, err := ioutil.ReadAll(reader)
// 	testingx.Must(t, err, "cannot read from GCS object")
// 	if string(content) != `[{"bucket_min":"0"}]` {
// 		t.Errorf("Export() wrote unexpected data on GCS: %v", string(content))
// 	}
// 	if len(bq.queries) != 1 || bq.queries[0] != "SELECT * FROM test" {
// 		t.Errorf("Export() did not run the expected queries: %v", bq.queries)
// 	}
// 	it.Reset()
// 	// Make the iterator return an error.
// 	it.iterErr = errors.New("iterator error")
// 	err = gen.Export(context.Background(), selectQuery, outputPathTpl)
// 	if !errors.Is(err, it.iterErr) {
// 		t.Errorf("Export() didn't return the expected error: %v", err)
// 	}
// 	it.iterErr = nil
// 	// Replace client with one whose queries always fail.
// 	gen.bqClient = &mockClient{
// 		queryReadMustFail: true,
// 	}
// 	err = gen.Export(context.Background(), selectQuery, outputPathTpl)
// 	if err == nil || !strings.Contains(err.Error(), "Read() failed") {
// 		t.Errorf("Export() didn't return the expected error: %v", err)
// 	}
// 	gen.bqClient = bq
// 	// Feed json.Marshal a row that cannot be marshalled.
// 	// Note: this is not something that can really happen when data comes
// 	// from BigQuery, I think.
// 	badDataIt := &mockRowIterator{
// 		rows: []map[string]bigquery.Value{
// 			{
// 				"date":  "2020-01-01",
// 				"field": "test",
// 				"year":  2020,
// 				"histograms": []map[string]bigquery.Value{
// 					{"this-will-fail": make(chan int)},
// 				},
// 			},
// 		},
// 	}
// 	bq.iterator = badDataIt
// 	err = gen.Export(context.Background(), selectQuery, outputPathTpl)
// 	if !strings.Contains(err.Error(), "unsupported type") {
// 		t.Errorf("Export() didn't return the expected error: %v", err)
// 	}
// 	bq.iterator = it
// 	// Bad output template.
// 	failingOutputPathTpl := template.Must(template.New("fail").Parse("{{nil}}"))
// 	err = gen.Export(context.Background(), selectQuery, failingOutputPathTpl)
// 	if err == nil || !strings.Contains(err.Error(), "not a command") {
// 		t.Errorf("Export() didn't return the expected error: %v", err)
// 	}
// 	it.Reset()
// 	// Make writes to the output path fail.
// 	fakeBucket.Object(
// 		"v0/test/2020/histogram_daily_stats.json").(*gcsfake.ObjectHandle).
// 		WritesMustFail = true
// 	err = gen.Export(context.Background(), selectQuery, outputPathTpl)
// 	if err == nil || !strings.Contains(err.Error(), "write failed") {
// 		t.Errorf("Export() didn't return the expected error: %v", err)

// 	}
// }

func TestJSONExporter_marshalAndUpload(t *testing.T) {
	jobs := make(chan *UploadJob)
	fakeRow := bqRow{
		"field": "value",
	}
	rows := []bqRow{fakeRow}
	go func() {
		err := marshalAndUpload("test", rows, jobs)
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
		err := marshalAndUpload("this-will-fail", rows, jobs)
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
	var uploadQLen int32
	var queriesDone int32

	results := make(chan UploadResult)
	go printStats(ctx, &uploadQLen, &queriesDone, 1, results)
	// Send a successful upload and an error, then check the output after a
	// second.
	results <- UploadResult{
		objName: "test",
	}
	results <- UploadResult{
		objName: "failed",
		err:     errors.New("upload failed"),
	}

	time.Sleep(1 * time.Second)
	if !strings.Contains(out.String(), "uploaded: 1") ||
		!strings.Contains(out.String(), "1 errors") {
		t.Errorf("printStats() didn't print the expected output")
	}
}

func Test_getFieldsFromPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want []string
	}{
		{
			name: "ok",
			path: "{{ .foo }}/{{.bar}}",
			want: []string{"foo", "bar"},
		},
		{
			name: "no-matches",
			path: "{{foo}}/{{bar}}",
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getFieldsFromPath(tt.path); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getFieldsFromPath() = %v, want %v", got, tt.want)
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
