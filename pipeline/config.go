package pipeline

type Config struct {
	Histograms map[string]HistogramConfig
	Exports    map[string]ExportConfig
}

type HistogramConfig struct {
	QueryFile string
	Dataset   string
	Table     string
}

type ExportConfig struct {
	SourceTable string
	QueryFile   string
	OutputPath  string
}
