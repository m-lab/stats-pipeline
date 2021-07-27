package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"text/template"
	"time"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/stats-pipeline/config"
	"github.com/m-lab/stats-pipeline/histogram"
)

type mockClient struct {
	bqiface.Client
}

type mockExporter struct{}

type mockHistogramTable struct{}

func (ex *mockExporter) Export(context.Context, config.Config, *template.Template, int) error {
	return nil
}

func (h *mockHistogramTable) UpdateHistogram(context.Context, time.Time, time.Time) error {
	return nil
}

func TestHandler_ServeHTTP(t *testing.T) {
	mc := &mockClient{}
	me := &mockExporter{}
	conf := map[string]config.Config{
		"test": {
			HistogramQueryFile: "testdata/test_histogram.sql",
			ExportQueryFile:    "testdata/test_export.sql",
			Dataset:            "test",
			Table:              "testtable",
		},
	}

	newHistogramTable = func(name, ds string, config histogram.QueryConfig,
		client bqiface.Client) HistogramTable {
		return &mockHistogramTable{}
	}

	tests := []struct {
		name       string
		w          http.ResponseWriter
		r          *http.Request
		bqClient   bqiface.Client
		exporter   Exporter
		config     map[string]config.Config
		statusCode int
		response   *pipelineResult
	}{
		{
			name:     "ok",
			bqClient: mc,
			exporter: me,
			config:   conf,
			r: httptest.NewRequest(http.MethodPost,
				"/v0/pipeline?start=2021-01-01&end=2021-12-31&step=all",
				bytes.NewReader([]byte{})),
			statusCode: http.StatusOK,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{histogramsStep, exportsStep},
				Errors:         []string{},
			},
		},
		{
			name: "invalid-method",
			r: httptest.NewRequest(http.MethodGet,
				"/v0/pipeline?year=2020&step=all", nil),
			statusCode: http.StatusMethodNotAllowed,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{},
				Errors: []string{
					http.StatusText(http.StatusMethodNotAllowed),
				},
			},
		},
		{
			name: "missing-parameter-start",
			r: httptest.NewRequest(http.MethodPost, "/v0/pipeline?end=2021-12-31&step=all",
				nil),
			statusCode: http.StatusBadRequest,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{},
				Errors:         []string{errMissingStartDate},
			},
		},
		{
			name: "missing-parameter-end",
			r: httptest.NewRequest(http.MethodPost, "/v0/pipeline?start=2021-01-01&step=all",
				nil),
			statusCode: http.StatusBadRequest,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{},
				Errors:         []string{errMissingEndDate},
			},
		},
		{
			name: "missing-parameter-step",
			r: httptest.NewRequest(http.MethodPost,
				"/v0/pipeline?start=2021-01-01&end=2021-12-31", nil),
			statusCode: http.StatusBadRequest,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{},
				Errors:         []string{errMissingStep},
			},
		},
		{
			name:       "action-histograms",
			r:          httptest.NewRequest(http.MethodPost, "/v0/pipeline?start=2021-01-01&end=2021-12-31&step=histograms", bytes.NewReader([]byte{})),
			statusCode: http.StatusOK,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{histogramsStep},
				Errors:         []string{},
			},
		},
		{
			name:       "action-exports",
			r:          httptest.NewRequest(http.MethodPost, "/v0/pipeline?start=2021-01-01&end=2021-12-31&step=exports", bytes.NewReader([]byte{})),
			statusCode: http.StatusOK,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{exportsStep},
				Errors:         []string{},
			},
		},
		{
			name:       "action-all",
			r:          httptest.NewRequest(http.MethodPost, "/v0/pipeline?start=2021-01-01&end=2021-12-31&step=all", bytes.NewReader([]byte{})),
			statusCode: http.StatusOK,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{histogramsStep, exportsStep},
				Errors:         []string{},
			},
		},
		{
			name:       "invalid-start",
			r:          httptest.NewRequest(http.MethodPost, "/v0/pipeline?start=xyz&end=2021-12-31&step=all", bytes.NewReader([]byte{})),
			statusCode: http.StatusBadRequest,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{},
				Errors:         []string{"parsing time \"xyz\" as \"2006-01-02\": cannot parse \"xyz\" as \"2006\""},
			},
		},
		{
			name:       "invalid-end",
			r:          httptest.NewRequest(http.MethodPost, "/v0/pipeline?start=2021-01-01&end=xyz&step=all", bytes.NewReader([]byte{})),
			statusCode: http.StatusBadRequest,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{},
				Errors:         []string{"parsing time \"xyz\" as \"2006-01-02\": cannot parse \"xyz\" as \"2006\""},
			},
		},
		{
			name:       "invalid-range-multiple-years",
			r:          httptest.NewRequest(http.MethodPost, "/v0/pipeline?start=2020-01-01&end=2021-12-31&step=all", bytes.NewReader([]byte{})),
			statusCode: http.StatusBadRequest,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{},
				Errors:         []string{errInvalidDateRange},
			},
		},
		{
			name:       "invalid-range-start-after-end",
			r:          httptest.NewRequest(http.MethodPost, "/v0/pipeline?start=2021-01-01&end=2020-12-31&step=all", bytes.NewReader([]byte{})),
			statusCode: http.StatusBadRequest,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{},
				Errors:         []string{errInvalidDateRange},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(tt.bqClient, tt.exporter, tt.config)
			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, tt.r)
			statusCode := recorder.Result().StatusCode
			if statusCode != tt.statusCode {
				t.Errorf("ServeHTTP(): expected %v, got %v", tt.statusCode, statusCode)
			}
			// Read response body and compare with the expected value.
			if tt.response != nil {
				body, err := ioutil.ReadAll(recorder.Result().Body)
				if err != nil {
					t.Errorf("Error while reading response body")
				}
				var responseJSON pipelineResult
				err = json.Unmarshal(body, &responseJSON)
				if err != nil {
					t.Errorf("Error while unmarshalling response body")
				}

				if !reflect.DeepEqual(responseJSON, *tt.response) {
					t.Errorf("Invalid response body: %v, expected %v", responseJSON,
						tt.response)
				}
			}

		})
	}
}

func TestNewHandler(t *testing.T) {
	mc := &mockClient{}
	me := &mockExporter{}
	config := map[string]config.Config{}
	h := NewHandler(mc, me, config)
	if h == nil {
		t.Fatalf("NewHandler() returned nil")
	}
	if h.bqClient != mc || h.exporter != me || !reflect.DeepEqual(h.configs, config) {
		t.Errorf("NewHandler() didn't return the expected handler")
	}
	// Check we can read from the channel.
	if _, ok := <-h.pipelineCanRun; !ok {
		t.Errorf("NewHandler() didn't return a properly initialized handler.")
	}
}
