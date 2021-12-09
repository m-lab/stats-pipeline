// Package formatter provides query formatters for export types supported by the stats pipeline.
package formatter

import (
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/stats-pipeline/config"
	"github.com/m-lab/uuid-annotator/annotator"
)

func TestHopAnnotation1QueryFormatter_Source(t *testing.T) {
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
				Dataset: "base_tables",
				Table:   "traceroute",
			},
			year: 2019,
			want: "mlab-testing.base_tables.traceroute",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewTracerouteHopAnnotation1QueryFormatter()
			if got := f.Source(tt.project, tt.config, tt.year); got != tt.want {
				t.Errorf("HopAnnotation1QueryFormatter.Source() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestHopAnnotation1QueryFormatter_Partitions(t *testing.T) {
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
         WHERE DATE(TestTime) BETWEEN DATE('2019-03-29') AND DATE('2021-09-08')
         GROUP BY date
         ORDER BY date`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewTracerouteHopAnnotation1QueryFormatter()
			if got := f.Partitions(tt.source); got != tt.want {
				t.Errorf("HopAnnotation1QueryFormatter.Partitions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHopAnnotation1QueryFormatter_Partition(t *testing.T) {
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
		{
			name: "error-empty-map",
			row:  map[string]bigquery.Value{},
			want: "0001-01-01",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewTracerouteHopAnnotation1QueryFormatter()
			if got := f.Partition(tt.row); got != tt.want {
				t.Errorf("HopAnnotation1QueryFormatter.Partition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHopAnnotation1QueryFormatter_Marshal(t *testing.T) {
	tests := []struct {
		name    string
		rows    []map[string]bigquery.Value
		want    []byte
		wantErr bool
	}{
		{
			name: "hopannotation1-schema-success",
			rows: []map[string]bigquery.Value{
				{
					"ID":        "abcdefghijklmnop",
					"Timestamp": "2021-03-21T11:09:00Z",
					"Annotations": &annotator.ClientAnnotations{
						Geo: &annotator.Geolocation{
							ContinentCode:       "EU",
							CountryCode:         "ES",
							CountryName:         "Spain",
							Region:              "CT",
							Subdivision1ISOCode: "CT",
							Subdivision1Name:    "Catalonia",
							Subdivision2ISOCode: "B",
							Subdivision2Name:    "Barcelona",
							City:                "Canet de Mar",
							PostalCode:          "08360",
							Latitude:            1,
							Longitude:           2,
							AccuracyRadiusKm:    1,
						},
						Network: &annotator.Network{
							CIDR:     "84.88.0.0/17",
							ASNumber: 13041,
							ASName:   "Consorci de Universitaris de Catalunya",
						},
					},
				},
			},
			want: []byte(`{"ID":"abcdefghijklmnop","Timestamp":"2021-03-21T11:09:00Z","Annotations":{"Geo":{"ContinentCode":"EU","CountryCode":"ES","CountryName":"Spain","Region":"CT","Subdivision1ISOCode":"CT","Subdivision1Name":"Catalonia","Subdivision2ISOCode":"B","Subdivision2Name":"Barcelona","City":"Canet de Mar","PostalCode":"08360","Latitude":1,"Longitude":2,"AccuracyRadiusKm":1},"Network":{"CIDR":"84.88.0.0/17","ASNumber":13041,"ASName":"Consorci de Universitaris de Catalunya"}}}`),
		},
		{
			name: "annotation-schema-ignored",
			rows: []map[string]bigquery.Value{
				{
					"UUID":      "abcdefghijklmnop",
					"Timestamp": "0001-01-01T00:00:00Z",
					"Server":    annotator.ServerAnnotations{},
					"Client":    annotator.ClientAnnotations{},
				},
			},
			want: []byte(`{"ID":"","Timestamp":"0001-01-01T00:00:00Z","Annotations":null}`),
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
			f := NewTracerouteHopAnnotation1QueryFormatter()
			got, err := f.Marshal(tt.rows)
			if (err != nil) != tt.wantErr {
				t.Errorf("HopAnnotation1QueryFormatter.Marshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("HopAnnotation1QueryFormatter.Marshal() = %q, want %q", string(got), string(tt.want))
			}
		})
	}
}
