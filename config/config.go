package config

// Config is a configuration object for the stats pipeline.
type Config struct {
	// HistogramQueryFile is the path to the query generating the histogram table.
	// This field is required.
	HistogramQueryFile string

	// DateField is the name of the date field in the query.
	// This is used to determine which rows to delete from the histogram table
	// when updating a certain range of dates. This field is required.
	DateField string

	// PartitionField is the field used to partition the histogram table.
	// It may be the same as DateField or a different field.
	// This field is optional.
	PartitionField string

	// PartitionType is the type of partitioning used.
	// Possible values are:
	//   - "time": partition by timestamp, date or datetime
	//   - "range": partition by integer range
	// This field is optional.
	PartitionType string

	// ExportQueryFile is the path to the export query.
	// This field is required.
	ExportQueryFile string

	// Dataset is the dataset name. This field is required.
	Dataset string

	// Table is the histogram table name. This field is required.
	Table string

	// OutputPath is a template defining the output path - either local or GCS.
	// This field is required.
	OutputPath string
}
