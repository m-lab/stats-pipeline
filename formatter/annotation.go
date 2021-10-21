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

// Where returns a bigquery "WHERE" clause based on a row returned by running
// the Partitions() query. The Annotation formatter conditions searches on the Date.
func (f *AnnotationQueryFormatter) Where(row map[string]bigquery.Value) string {
	partition := row["date"].(civil.Date)
	return fmt.Sprintf("WHERE %s = DATE('%d-%02d-%02d')", f.DateExpr, partition.Year, int(partition.Month), partition.Day)

}

// Marshal converts an export query row into a byte result suitable for writing
// to disk. For annottion export, the format is marshalled to annotation.Annotations and then to JSON.
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
