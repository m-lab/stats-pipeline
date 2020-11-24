package config

// Config is a configuration object for the stats pipeline.
type Config struct {
	// HistogramQuery is the path to the query generating the histogram table.
	HistogramQuery string
	// ExportQuery is the pth to the export query.
	ExportQuery string
	// Dataset is the dataset name.
	Dataset string
	// Table is the histogram table name.
	Table string
	// OutputPath is a template defining the output path on GCS.
	OutputPath string
	// PartitionField is the field to partition the table on.
	PartitionField string
}
