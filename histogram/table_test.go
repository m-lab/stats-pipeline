package histogram

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/go/rtx"
)

func TestNewTable(t *testing.T) {
	table := NewTable("test_table", "dataset", "SELECT 1", &testClient{})
	if table == nil {
		t.Errorf("NewTable() returned nil.")
	}
}

func TestTable_queryConfig(t *testing.T) {
	testQuery := "SELECT 1"
	table := NewTable("test", "dataset", "", &testClient{})
	q := table.queryConfig(testQuery)
	if q.Q != testQuery {
		t.Errorf("queryConfig(): expected %s, got %s.", testQuery, q.Q)
	}
}

type testClient struct {
	bqiface.Client
	queryReadMustFail bool
	queryRunMustFail  bool
	queries           []string
}

type testDataset struct {
	bqiface.Dataset
	name string
}

type testTable struct {
	bqiface.Table
	ds   string
	name string
}

type testRowIterator struct {
	bqiface.RowIterator
}

func (c *testClient) Dataset(name string) bqiface.Dataset {
	return &testDataset{
		name: name,
	}
}

func (c *testClient) Query(query string) bqiface.Query {
	return &mockQuery{
		client:       c,
		q:            query,
		readMustFail: c.queryReadMustFail,
		runMustFail:  c.queryRunMustFail,
	}
}

func (ds *testDataset) Table(name string) bqiface.Table {
	return &testTable{
		ds:   ds.name,
		name: name,
	}
}

func (t *testTable) DatasetID() string {
	return t.ds
}

func (t *testTable) TableID() string {
	return t.name
}

// ********** mockQuery **********
type mockQuery struct {
	bqiface.Query
	client       *testClient
	q            string
	qc           bqiface.QueryConfig
	readMustFail bool
	runMustFail  bool
}

func (q *mockQuery) Run(context.Context) (bqiface.Job, error) {
	if q.runMustFail {
		return nil, errors.New("Run() failed")
	}
	// Store the query's content into the client so it can be checked later.
	q.client.queries = append(q.client.queries, q.q)
	return &mockJob{}, nil
}

func (q *mockQuery) Read(context.Context) (bqiface.RowIterator, error) {
	if q.readMustFail {
		return nil, errors.New("Read() failed")
	}
	// Store the query's content into the client so it can be checked later.
	q.client.queries = append(q.client.queries, q.q)
	return &testRowIterator{}, nil
}

func (q *mockQuery) SetQueryConfig(qc bqiface.QueryConfig) {
	q.qc = qc
}

// ***** testJob *****
type mockJob struct {
	bqiface.Job
	waitMustFail bool
}

func (j *mockJob) Wait(context.Context) (*bigquery.JobStatus, error) {
	if j.waitMustFail {
		return nil, errors.New("Wait() failed")
	}
	return &bigquery.JobStatus{
		State: bigquery.Done,
	}, nil
}

func TestTable_deleteRows(t *testing.T) {
	table := NewTable("test", "dataset", "query", &testClient{})
	err := table.deleteRows(context.Background(), time.Now(), time.Now().Add(1*time.Minute))
	if err != nil {
		t.Errorf("deleteRows() returned err: %v", err)
	}

	table = NewTable("test", "dataset", "query", &testClient{
		queryReadMustFail: true,
	})
	err = table.deleteRows(context.Background(), time.Now(), time.Now().Add(1*time.Minute))
	if err == nil {
		t.Errorf("deleteRows(): expected err, returned nil.")
	}
}

func TestTable_UpdateHistogram(t *testing.T) {
	start, err := time.Parse(dateFormat, "2020-01-01")
	rtx.Must(err, "cannot parse start time")
	end, err := time.Parse(dateFormat, "2020-12-31")
	rtx.Must(err, "cannot parse end time")
	tests := []struct {
		name    string
		query   string
		client  *testClient
		want    []string
		wantErr bool
	}{
		{
			name:   "ok",
			query:  "histogram generation query",
			client: &testClient{},
			want: []string{
				"DELETE FROM test_ds.test_table WHERE test_date BETWEEN \"2020-01-01\" AND \"2020-12-31\"",
				"histogram generation query",
			},
		},
		{
			name:  "delete-rows-failure",
			query: "test",
			client: &testClient{
				queryReadMustFail: true,
			},
			wantErr: true,
		},
		{
			name:  "query-run-failure",
			query: "test",
			client: &testClient{
				queryRunMustFail: true,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hist := &Table{
				Table:  tt.client.Dataset("test_ds").Table("test_table"),
				Query:  tt.query,
				Client: tt.client,
			}
			if err := hist.UpdateHistogram(context.Background(), start,
				end); (err != nil) != tt.wantErr {
				t.Errorf("Table.UpdateHistogram() error = %v, wantErr %v", err, tt.wantErr)
			}

			if mockClient, ok := hist.Client.(*testClient); ok {
				if tt.want != nil && !reflect.DeepEqual(mockClient.queries, tt.want) {
					t.Errorf("UpdateHistogram(): expected %v, got %v", tt.want,
						mockClient.queries)
				}
			} else {
				t.Fatalf("UpdateHistogram(): client isn't a mockClient.")
			}
		})
	}
}
