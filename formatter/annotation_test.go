// Package formatter provides query formatters for export types supported by the stats pipeline.
package formatter

import (
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/stats-pipeline/config"
)

func TestAnnotationQueryFormatter_Source(t *testing.T) {
	tests := []struct {
		name    string
		project string
		config  config.Config
		year    int
		want    string
	}{
		{
			name:    "success",
			project: "mlab-testing",
			config: config.Config{
				Dataset: "statistics",
				Table:   "bananas",
			},
			year: 2019,
			want: "mlab-testing.statistics.bananas",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewTCPINFOAnnotationQueryFormatter()
			if got := f.Source(tt.project, tt.config, tt.year); got != tt.want {
				t.Errorf("AnnotationQueryFormatter.Source() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestAnnotationQueryFormatter_Partitions(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{
			name:   "success",
			source: "a.b.c",
			want: `SELECT DATE(TestTime) as date
         FROM a.b.c
         WHERE DATE(TestTime) < DATE('2020-03-11')
         GROUP BY date
         ORDER BY date`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewTCPINFOAnnotationQueryFormatter()
			if got := f.Partitions(tt.source); got != tt.want {
				t.Errorf("AnnotationQueryFormatter.Partitions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnnotationQueryFormatter_Partition(t *testing.T) {
	tests := []struct {
		name string
		row  map[string]bigquery.Value
		want string
	}{
		{
			name: "success",
			row: map[string]bigquery.Value{
				"date": civil.DateOf(time.Date(2020, time.June, 01, 0, 0, 0, 0, time.UTC)),
			},
			want: `2020-06-01`,
		},
		{
			name: "error-missing-date",
			row: map[string]bigquery.Value{
				"missing_date": 10,
			},
			want: "0001-01-01",
		},
		{
			name: "error-date-wrong-type",
			row: map[string]bigquery.Value{
				"date": time.Date(2020, time.June, 01, 0, 0, 0, 0, time.UTC),
			},
			want: "0001-01-01",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewTCPINFOAnnotationQueryFormatter()
			if got := f.Partition(tt.row); got != tt.want {
				t.Errorf("AnnotationQueryFormatter.Partition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnnotationQueryFormatter_Marshal(t *testing.T) {
	tests := []struct {
		name    string
		rows    []map[string]bigquery.Value
		want    []byte
		wantErr bool
	}{
		{
			name: "success",
			rows: []map[string]bigquery.Value{
				{
					"UUID": "abcdefghijklmnop",
				},
				{
					"UUID": "IGNORED",
				},
			},
			want: []byte(`{"UUID":"abcdefghijklmnop","Timestamp":"0001-01-01T00:00:00Z","Server":{},"Client":{}}`),
		},
		{
			name: "failure",
			rows: []map[string]bigquery.Value{
				{
					// Functions are valid bigquery.Values but cannot be marshalled to JSON.
					"test": func() {},
				},
			},
			wantErr: true,
		},
		{
			name:    "failure-empty-array",
			rows:    []map[string]bigquery.Value{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewTCPINFOAnnotationQueryFormatter()
			got, err := f.Marshal(tt.rows)
			if (err != nil) != tt.wantErr {
				t.Errorf("AnnotationQueryFormatter.Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AnnotationQueryFormatter.Marshal() = %q, want %q", string(got), string(tt.want))
			}
		})
	}
}
