package formatter

import (
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/m-lab/stats-pipeline/config"
	"github.com/m-lab/uuid-annotator/annotator"
)

// AnnotationQueryFormatter prepares export queries for annotation data exported
// by the stats pipeline.
type AnnotationQueryFormatter struct {
	DateExpr string // BigQuery expression used to extract a row's date.
}

// NewTCPINFOAnnotationQueryFormatter creates a new AnnotationQueryFormatter.
func NewTCPINFOAnnotationQueryFormatter() *AnnotationQueryFormatter {
	return &AnnotationQueryFormatter{DateExpr: "DATE(TestTime)"}
}

// Source returns a fully qualified bigquery table name. The year is ignored.
func (f *AnnotationQueryFormatter) Source(project string, config config.Config, year int) string {
	return fmt.Sprintf("%s.%s.%s", project, config.Dataset, config.Table)
}

// Partitions returns a bigquery query for listing all partitions for a given
// source table. The Annotation query partitions on `date`.
func (f *AnnotationQueryFormatter) Partitions(source string) string {
	return fmt.Sprintf(
		`SELECT %s as date
         FROM %s
         WHERE %s < DATE('2020-03-11')
         GROUP BY date
         ORDER BY date`, f.DateExpr, source, f.DateExpr)
}

// Partition returns a date partition id based on a row returned by running the
// Partitions() query. The partition id can be used in query templates.  The
// Annotation formatter conditions searches on the Date.
func (f *AnnotationQueryFormatter) Partition(row map[string]bigquery.Value) string {
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
// to disk. For annotation export, the format is marshalled to annotation.Annotations and then to JSON.
func (f *AnnotationQueryFormatter) Marshal(rows []map[string]bigquery.Value) ([]byte, error) {
	if len(rows) == 0 {
		return nil, errors.New("zero length record")
	}
	// Serialize the bigquery row to JSON. This will include empty fields.
	j, err := json.Marshal(rows[0])
	if err != nil {
		return nil, err
	}

	// Load JSON into real annotation struct.
	v := annotator.Annotations{}
	err = json.Unmarshal(j, &v)
	if err != nil {
		return nil, err
	}

	// Serialize the actual type to JSON, which omits empty fields.
	return json.Marshal(v)
}
