package pipeline

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"text/template"
	"time"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/stats-pipeline/exporter"
	"github.com/m-lab/stats-pipeline/histogram"
)

const dateFormat = "2006-01-02"

type Handler struct {
	bqClient bqiface.Client
	exporter *exporter.JSONExporter
	config   Config
}

func NewHandler(bqClient bqiface.Client, exporter *exporter.JSONExporter,
	config Config) *Handler {
	return &Handler{
		bqClient: bqClient,
		exporter: exporter,
		config:   config,
	}
}

// ServeHTTP handles requests to the /pipeline endpoint.
// This endpoint runs the entire statistics generation pipeline for the
// provided year, i.e. every configured histogram table is updated and every
// configured exporting task is run.
//
// The querystring parameters are:
// - year (mandatory): the year to generate statistics for.
//
// This endpoint accepts only GET requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	year := r.URL.Query().Get("year")
	if year == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing parameter: year"))
		return
	}
	// Update all the histogram tables.
	for name, config := range h.config.Histograms {
		log.Printf("Updating histogram table: %s...", name)
		err := h.generateHistogramForYear(r.Context(), config, year)
		if err != nil {
			// If one of the histogram queries fail, we still want to try the
			// remaining ones.
			log.Printf("Cannot update histogram %s: %v", name, err)
			continue
		}
	}
	// Export data to GCS.
	for name, config := range h.config.Exports {
		log.Printf("Exporting %s...", name)
		// Read query file
		content, err := ioutil.ReadFile(config.QueryFile)
		if err != nil {
			log.Printf("Cannot read query file %s, skipping (%v)",
				config.QueryFile, err)
			continue
		}
		selectTpl := template.Must(template.New(name).
			Option("missingkey=zero").Parse(string(content)))
		outputTpl := template.Must(template.New(name).Parse(config.OutputPath))

		h.exporter.Export(r.Context(), config.SourceTable, selectTpl,
			outputTpl)
	}
}

func (h *Handler) generateHistogramForYear(ctx context.Context,
	config HistogramConfig, year string) error {
	content, err := ioutil.ReadFile(config.QueryFile)
	if err != nil {
		return err
	}
	hist := histogram.NewTable(config.Table, config.Dataset, string(content),
		h.bqClient)
	start, err := time.Parse(dateFormat, year+"-01-01")
	if err != nil {
		return err
	}
	end, err := time.Parse(dateFormat, year+"-12-31")
	if err != nil {
		return err
	}
	err = hist.UpdateHistogram(ctx, start, end)
	if err != nil {
		return err
	}
	return nil
}
