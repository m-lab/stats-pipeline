package config

// Config is a configuration object for the stats pipeline.
type Config struct {
	// HistogramQueryFile is the path to the query generating the histogram table.
	HistogramQueryFile string

	// DateField is the name of the date field in the query.
	// This is used to determine which rows to delete from the histogram table
	// when updating a certain range of dates.
	DateField string

	// PartitionField is the field used to partition the histogram table.
	// It may be the same as DateField or a different field.
	PartitionField string

	// PartitionType is the type of partitioning used.
	// Possible values are:
	//   - "time": partition by timestamp, date or datetime
	//   - "range": partition by integer range
	PartitionType string

	// ExportQueryFile is the path to the export query.
	ExportQueryFile string

	// Dataset is the dataset name.
	Dataset string

	// Table is the histogram table name.
	Table string

	// OutputPath is a template defining the output path on GCS.
	OutputPath string
}
