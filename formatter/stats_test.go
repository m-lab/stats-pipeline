// Package formatter provides query formatters for export types supported by the stats pipeline.
package formatter

import (
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/stats-pipeline/config"
)

func TestStatsQueryFormatter_Source(t *testing.T) {
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
			want: "mlab-testing.statistics.bananas_2019",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewStatsQueryFormatter()
			if got := f.Source(tt.project, tt.config, tt.year); got != tt.want {
				t.Errorf("StatsQueryFormatter.Source() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestStatsQueryFormatter_Partitions(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{
			name:   "success",
			source: "a.b.c",
			want: `SELECT shard
	    FROM a.b.c
		GROUP BY shard
		ORDER BY COUNT(*) DESC`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewStatsQueryFormatter()
			if got := f.Partitions(tt.source); got != tt.want {
				t.Errorf("StatsQueryFormatter.Partitions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatsQueryFormatter_Where(t *testing.T) {
	tests := []struct {
		name string
		row  map[string]bigquery.Value
		want string
	}{
		{
			name: "success",
			row: map[string]bigquery.Value{
				"shard": int64(1234),
			},
			want: "WHERE shard = 1234",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewStatsQueryFormatter()
			if got := f.Where(tt.row); got != tt.want {
				t.Errorf("StatsQueryFormatter.Where() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatsQueryFormatter_Marshal(t *testing.T) {
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
					"test": 1234,
				},
			},
			want: []byte(`[{"test":1234}]`),
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewStatsQueryFormatter()
			got, err := f.Marshal(tt.rows)
			if (err != nil) != tt.wantErr {
				t.Errorf("StatsQueryFormatter.Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StatsQueryFormatter.Marshal() = %q, want %q", string(got), string(tt.want))
			}
		})
	}
}
