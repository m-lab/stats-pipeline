package formatter

import (
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/stats-pipeline/config"
	"github.com/m-lab/traceroute-caller/hopannotation"
)

// HopAnnotation1QueryFormatter prepares export queries for hopannotation1 data
// exported by the stats pipeline.
type HopAnnotation1QueryFormatter struct{}

// NewTracerouteHopAnnotation1QueryFormatter creates a new HopAnnotation1QueryFormatter.
func NewTracerouteHopAnnotation1QueryFormatter() *HopAnnotation1QueryFormatter {
	return &HopAnnotation1QueryFormatter{}
}

// Source returns a fully qualified bigquery table name. The year is ignored.
func (f *HopAnnotation1QueryFormatter) Source(project string, config config.Config, year int) string {
	return fmt.Sprintf("%s.%s.%s", project, config.Dataset, config.Table)
}

// Partitions returns a bigquery query for listing all partitions for a given
// source table. The HopAnnotation1 query partitions on `date`.
func (f *HopAnnotation1QueryFormatter) Partitions(source string) string {
	return fmt.Sprintf(
		`SELECT DATE(TestTime) as date
         FROM %s
         WHERE DATE(TestTime) BETWEEN DATE('2013-05-08') AND DATE('2021-09-08')
         GROUP BY date
         ORDER BY date`, source)
}

// Partition returns a date partition id based on a row returned by running the
// Partitions() query. The partition id can be used in query templates.  The
// HopAnnotation1 formatter condition searches on the Date.
func (f *HopAnnotation1QueryFormatter) Partition(row map[string]bigquery.Value) string {
	date, ok := row["date"]
	if !ok {
		return "0001-01-01" // a noop expression.
	}
	partition, ok := date.(civil.Date)
	if !ok {
		return "0001-01-01" // a noop expression.
	}
	return fmt.Sprintf("%d-%02d-%02d", partition.Year, int(partition.Month), partition.Day)
}

// Marshal converts an export query row into a byte result suitable for writing
// to disk. For hopannotation1 export, the format is marshalled to hopannotation.HopAnnotation1{},
// and then to JSON.
func (f *HopAnnotation1QueryFormatter) Marshal(rows []map[string]bigquery.Value) ([]byte, error) {
	if len(rows) == 0 {
		return nil, errors.New("zero length record")
	}
	// Serialize the bigquery row to JSON. This will include empty fields.
	j, err := json.Marshal(rows[0])
	if err != nil {
		return nil, err
	}

	// Load JSON into real hopannotation1 struct.
	v := hopannotation.HopAnnotation1{}
	err = json.Unmarshal(j, &v)
	if err != nil {
		return nil, err
	}

	// Serialize the actual type to JSON, which omits empty fields.
	return json.Marshal(v)
}
