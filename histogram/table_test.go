package histogram

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/go/prometheusx/promtest"
	"github.com/m-lab/go/rtx"
	"google.golang.org/api/googleapi"
)

// ***** mockClient *****
type mockClient struct {
	bqiface.Client
	queryReadMustFail bool
	queryRunMustFail  bool
	tableMissingErr   bool
	queries           []string
}

func (c *mockClient) Dataset(name string) bqiface.Dataset {
	return &mockDataset{
		name:            name,
		tableMissingErr: c.tableMissingErr,
	}
}

func (c *mockClient) Query(query string) bqiface.Query {
	return &mockQuery{
		client:       c,
		q:            query,
		readMustFail: c.queryReadMustFail,
		runMustFail:  c.queryRunMustFail,
	}
}

// ***** mockDataset *****
type mockDataset struct {
	bqiface.Dataset
	name            string
	tableMissingErr bool
}

func (ds *mockDataset) Table(name string) bqiface.Table {
	return &mockTable{
		ds:              ds.name,
		name:            name,
		tableMissingErr: ds.tableMissingErr,
	}
}

// ***** mockTable *****
type mockTable struct {
	bqiface.Table
	ds              string
	name            string
	tableMissingErr bool
}

func (t *mockTable) DatasetID() string {
	return t.ds
}

func (t *mockTable) TableID() string {
	return t.name
}

func (t *mockTable) FullyQualifiedName() string {
	return t.name
}

func (t *mockTable) Metadata(ctx context.Context) (*bigquery.TableMetadata, error) {
	if t.tableMissingErr {
		return nil, &googleapi.Error{
			Code: http.StatusNotFound,
		}
	}
	return nil, nil
}

// ********** mockQuery **********
type mockQuery struct {
	bqiface.Query
	client       *mockClient
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
	return &mockRowIterator{}, nil
}

func (q *mockQuery) SetQueryConfig(qc bqiface.QueryConfig) {
	q.qc = qc
}

// ***** mockRowIterator *****
type mockRowIterator struct {
	bqiface.RowIterator
}

// ***** mockJob *****
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
		Statistics: &bigquery.JobStatistics{
			TotalBytesProcessed: 10,
		},
	}, nil
}

// ***** Tests *****
func TestNewTable(t *testing.T) {
	table := NewTable("test_table", "dataset", "SELECT 1", &mockClient{})
	if table == nil {
		t.Errorf("NewTable() returned nil.")
	}
}

func TestTable_queryConfig(t *testing.T) {
	testQuery := "SELECT 1"
	table := NewTable("test", "dataset", "", &mockClient{})
	q := table.queryConfig(testQuery)
	if q.Q != testQuery {
		t.Errorf("queryConfig(): expected %s, got %s.", testQuery, q.Q)
	}
}

func TestTable_deleteRows(t *testing.T) {
	table := NewTable("test", "dataset", "query", &mockClient{})
	err := table.deleteRows(context.Background(), time.Now(), time.Now().Add(1*time.Minute))
	if err != nil {
		t.Errorf("deleteRows() returned err: %v", err)
	}

	table = NewTable("test", "dataset", "query", &mockClient{
		tableMissingErr: true,
	})
	err = table.deleteRows(context.Background(), time.Now(), time.Now().Add(1*time.Minute))
	if err != nil {
		t.Errorf("deleteRows() returned err: %v", err)
	}

	table = NewTable("test", "dataset", "query", &mockClient{
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
		client  *mockClient
		want    []string
		wantErr bool
	}{
		{
			name:   "ok",
			query:  "histogram generation query",
			client: &mockClient{},
			want: []string{
				"DELETE FROM test_ds.test_table WHERE date BETWEEN \"2020-01-01\" AND \"2020-12-31\"",
				"histogram generation query",
			},
		},
		{
			name:  "delete-rows-failure",
			query: "test",
			client: &mockClient{
				queryReadMustFail: true,
			},
			wantErr: true,
		},
		{
			name:  "query-run-failure",
			query: "test",
			client: &mockClient{
				queryRunMustFail: true,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hist := &Table{
				Table:  tt.client.Dataset("test_ds").Table("test_table"),
				query:  tt.query,
				client: tt.client,
			}
			if err := hist.UpdateHistogram(context.Background(), start,
				end); (err != nil) != tt.wantErr {
				t.Errorf("Table.UpdateHistogram() error = %v, wantErr %v", err, tt.wantErr)
			}

			if mockClient, ok := hist.client.(*mockClient); ok {
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

func TestPrometheusMetrics(t *testing.T) {
	queryBytesProcessMetric.WithLabelValues("x")

	promtest.LintMetrics(t)
}
