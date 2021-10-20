// Package formatter provides query formatters for export types supported by the stats pipeline.
package formatter

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/stats-pipeline/config"
)

// StatsQueryFormatter prepares export queries for statstics in the stats pipeline.
type StatsQueryFormatter struct{}

// NewStatsQueryFormatter creates a new StatsQueryFormatter.
func NewStatsQueryFormatter() *StatsQueryFormatter {
	return &StatsQueryFormatter{}
}

// Source returns a fully qualified bigquery table name including a year suffix
// used by the stats pipeline.
func (f *StatsQueryFormatter) Source(project string, config config.Config, year int) string {
	return fmt.Sprintf("%s.%s.%s_%d", project, config.Dataset, config.Table, year)
}

// Partitions returns a bigquery query for listing all partitions for a given
// source table.
func (f *StatsQueryFormatter) Partitions(source string) string {
	return fmt.Sprintf(
		`SELECT shard
	    FROM %s
		GROUP BY shard
		ORDER BY COUNT(*) DESC`, source)
}

// Where returns a bigquery "WHERE" clause based on a row returned by running
// the Partitions() query.
func (f *StatsQueryFormatter) Where(row map[string]bigquery.Value) string {
	partition := row["shard"].(int64)
	return fmt.Sprintf("WHERE shard = %d", partition)
}

// Marshal converts an export query row into a byte result suitable for writing
// to disk. For stats pipeline export, the format is JSON.
func (f *StatsQueryFormatter) Marshal(rows []map[string]bigquery.Value) ([]byte, error) {
	j, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}
	return j, nil
}
