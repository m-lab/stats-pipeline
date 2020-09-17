package histogram

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
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
	queryMustFail bool
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

type testQuery struct {
	bqiface.Query
	q            string
	readMustFail bool
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
	return &testQuery{
		q:            query,
		readMustFail: c.queryMustFail,
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

func (q *testQuery) Read(context.Context) (bqiface.RowIterator, error) {
	if q.readMustFail {
		return nil, errors.New("error")
	}
	return &testRowIterator{}, nil
}

func TestTable_deleteRows(t *testing.T) {
	table := NewTable("test", "dataset", "query", &testClient{})
	err := table.deleteRows(context.Background(), time.Now(), time.Now().Add(1*time.Minute))
	if err != nil {
		t.Errorf("deleteRows() returned err: %v", err)
	}

	table = NewTable("test", "dataset", "query", &testClient{
		queryMustFail: true,
	})
	err = table.deleteRows(context.Background(), time.Now(), time.Now().Add(1*time.Minute))
	if err == nil {
		t.Errorf("deleteRows(): expected err, returned nil.")
	}
}
