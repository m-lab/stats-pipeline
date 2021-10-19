package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"text/template"
	"time"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/stats-pipeline/config"
	"github.com/m-lab/stats-pipeline/histogram"
)

const dateFormat = "2006-01-02"

var (
	newHistogramTable = func(name, ds string, config histogram.QueryConfig,
		client bqiface.Client) HistogramTable {
		return histogram.NewTable(name, ds, config, client)
	}
)

// HistogramTable is an updatable histogram table.
type HistogramTable interface {
	UpdateHistogram(context.Context, time.Time, time.Time) error
}

// Exporter is a configurable data exporter.
type Exporter interface {
	Export(context.Context, config.Config, *template.Template, int) error
}

// Handler is the handler for /v0/pipeline.
type Handler struct {
	bqClient bqiface.Client
	exporter Exporter
	configs  map[string]config.Config

	pipelineCanRun chan bool
}

type pipelineStep string

const (
	histogramsStep pipelineStep = "histograms"
	exportsStep    pipelineStep = "exports"
)

type pipelineResult struct {
	CompletedSteps []pipelineStep
	Errors         []string
}

func newPipelineResult() pipelineResult {
	return pipelineResult{
		CompletedSteps: []pipelineStep{},
		Errors:         []string{},
	}
}

// NewHandler returns a new Handler.
func NewHandler(bqClient bqiface.Client, exporter Exporter,
	config map[string]config.Config) *Handler {
	pipelineCanRun := make(chan bool, 1)
	pipelineCanRun <- true
	return &Handler{
		bqClient:       bqClient,
		exporter:       exporter,
		configs:        config,
		pipelineCanRun: pipelineCanRun,
	}
}

// ServeHTTP handles requests to the /pipeline endpoint.
// This endpoint runs the entire statistics generation pipeline for the
// provided year, i.e. every configured histogram table is updated and every
// configured exporting task is run.
//
// The querystring parameters are:
// - year (mandatory): the year to generate statistics for.
// - step: specify which step of the pipeline to run (histograms or exports).
//   A value of "all" runs all the steps.
//
// This endpoint accepts only POST requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer log.Print("handler exited")
	w.Header().Set("Content-Type", "application/json")

	result := newPipelineResult()
	if r.Method != http.MethodPost {
		result.Errors = append(result.Errors, http.StatusText(
			http.StatusMethodNotAllowed))
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(result)
		return
	}
	// Read start / end dates from the request.
	start := r.URL.Query().Get("start")
	if start == "" {
		result.Errors = append(result.Errors, errMissingStartDate)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(result)
		return
	}
	end := r.URL.Query().Get("end")
	if end == "" {
		result.Errors = append(result.Errors, errMissingEndDate)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(result)
		return
	}
	// Validate the dates and convert them to time.Time using ValidateDates.
	startTime, endTime, err := ValidateDates(start, end)
	if err != nil {
		result.Errors = append(result.Errors, err.Error())
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(result)
		return
	}
	step := r.URL.Query().Get("step")
	if step == "" {
		result.Errors = append(result.Errors, errMissingStep)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(result)
		return
	}
	// Check if the pipeline is already running. Only one instance of the
	// pipeline can be run at a time.
	select {
	case <-h.pipelineCanRun:
		defer func() {
			// Make sure the pipeline can run again once finished.
			h.pipelineCanRun <- true
		}()
	default:
		result.Errors = append(result.Errors, errAlreadyRunning)
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(result)
		return
	}
	// Run the pipeline.
	result, err = h.runPipeline(r.Context(), step, startTime, endTime)
	if err != nil {
		result.Errors = append(result.Errors, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(result)
		return
	}
	// Return the pipeline result.
	json.NewEncoder(w).Encode(result)
}

// runPipeline runs the entire statistics generation pipeline for the provided
// start / end dates.
func (h *Handler) runPipeline(ctx context.Context, step string,
	start, end time.Time) (pipelineResult, error) {
	result := newPipelineResult()

	// Get all the yearly ranges between the start and end date.
	// Since output tables are per year, if the start and end dates
	// are in different years, we need to update the table for each
	// year.
	ranges := getYearlyRanges(start, end)

	if step == "all" || step == "histograms" {
		// Update all the histogram tables.
		for name, config := range h.configs {
			for _, r := range ranges {
				if ctx.Err() != nil {
					// If the request's context has been canceled, we must
					// return here.
					return result, ctx.Err()
				}
				rangeStart := r[0]
				rangeEnd := r[1]

				log.Printf("Updating histogram table %s between %s and %s...",
					name, rangeStart, rangeEnd)
				err := h.runQueryBetweenDates(ctx, config, rangeStart, rangeEnd)
				if err != nil {
					// If one of the histogram queries fail, we still want to
					// try the remaining ones for this range.
					log.Printf("Cannot update histogram %s: %v", name, err)
					result.Errors = append(result.Errors,
						fmt.Sprintf("Cannot update histogram %s: %v", name, err))
					continue
				}
			}

		}
		result.CompletedSteps = append(result.CompletedSteps, histogramsStep)
	}

	if step == "all" || step == "exports" {
		// Export data to GCS.
		for name, config := range h.configs {
			for _, r := range ranges {
				year := r[0].Year()
				if ctx.Err() != nil {
					// If the request's context has been canceled, we must
					// return here.
					return result, ctx.Err()
				}
				log.Printf("Exporting %s for year %d...", name, year)
				err := h.exportYear(ctx, config, end.Year())
				if err != nil {
					log.Printf("Error while exporting %s: %v",
						config.Table, err)
					result.Errors = append(result.Errors, fmt.Sprintf(
						"Error while exporting %s: %v", config.Table, err))
				}
			}
		}
		result.CompletedSteps = append(result.CompletedSteps, exportsStep)
	}

	return result, nil
}

// runQueryBetweenDates reads the query file and runs the query for the given
// start and end dates.
func (h *Handler) runQueryBetweenDates(ctx context.Context,
	config config.Config, start, end time.Time) error {
	// Read query file
	content, err := ioutil.ReadFile(config.HistogramQueryFile)
	if err != nil {
		return fmt.Errorf("cannot read query file %s: %v",
			config.HistogramQueryFile, err)
	}
	// Append year to the table name.
	table := fmt.Sprintf("%s_%d", config.Table, end.Year())
	// Configure the histogram query runner.
	queryConfig := histogram.QueryConfig{
		Query:          string(content),
		DateField:      config.DateField,
		PartitionField: config.PartitionField,
		PartitionType:  config.PartitionType,
	}

	output := newHistogramTable(table, config.Dataset, queryConfig,
		h.bqClient)
	err = output.UpdateHistogram(ctx, start, end)
	if err != nil {
		return fmt.Errorf("cannot update histogram table %s: %v",
			table, err)
	}
	return nil
}

// exportYear runs the exporter for the given year.
func (h *Handler) exportYear(ctx context.Context, config config.Config,
	year int) error {
	// Read query file
	content, err := ioutil.ReadFile(config.ExportQueryFile)
	if err != nil {
		return fmt.Errorf("cannot read export query file %s: %v",
			config.ExportQueryFile, err)
	}
	// Append year to the table name.
	table := fmt.Sprintf("%s_%d", config.Table, year)
	// Create template based on the export query file.
	selectTpl := template.Must(template.New(table).
		Option("missingkey=zero").Parse(string(content)))
	// Run the exporter for the given year.
	return h.exporter.Export(ctx, config, selectTpl, year)
}

// ValidateDates checks that the start and end dates are valid and returns
// them as time.Time.
func ValidateDates(start, end string) (time.Time, time.Time, error) {
	startTime, err := time.Parse(dateFormat, start)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	endTime, err := time.Parse(dateFormat, end)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return startTime, endTime, nil
}

// getYearlyRanges splits the start/end range into one or more per-year ranges.
// For example, if the start date is 2017-01-01 and the end date is 2017-12-31,
// this function will return a slice with a single range of 2017-01-01 to
// 2017-12-31. If the start date is 2017-01-01 and the end date is 2018-07-01,
// this function will return a slice with two ranges of 2017-01-01 to 2017-12-31
// and 2018-01-01 to 2018-07-01.
func getYearlyRanges(start, end time.Time) [][]time.Time {
	var ranges [][]time.Time
	if start.Year() == end.Year() {
		// If the start and end dates are in the same year, we only need one
		// range.
		ranges = append(ranges, []time.Time{start, end})
	} else {
		// If the start and end dates are in different years, we need a range
		// for each year.
		// We start by adding the first year.
		ranges = append(ranges, []time.Time{start, time.Date(start.Year(),
			12, 31, 0, 0, 0, 0, time.UTC)})

		// Then we add the remaining years. If the start and end years are
		// adjacent, this loop won't do anything.
		for year := start.Year() + 1; year < end.Year(); year++ {
			ranges = append(ranges, []time.Time{time.Date(year, 1, 1, 0, 0, 0,
				0, time.UTC), time.Date(year, 12, 31, 0, 0, 0, 0, time.UTC)})
		}

		// Finally, we add the last year.
		ranges = append(ranges, []time.Time{time.Date(end.Year(), 1, 1, 0, 0, 0,
			0, time.UTC), end})

	}
	return ranges
}
