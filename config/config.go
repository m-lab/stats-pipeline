package config

// Config is a configuration object for the stats pipeline.
type Config struct {
	// HistogramQueryFile is the path to the query generating the histogram table.
	HistogramQueryFile string
	// ExportQueryFile is the path to the export query.
	ExportQueryFile string
	// Dataset is the dataset name.
	Dataset string
	// Table is the histogram table name.
	Table string
	// OutputPath is a template defining the output path on GCS.
	OutputPath string
	// PartitionField is the field to partition the table on.
	PartitionField string
}
