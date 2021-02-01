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
)

type mockClient struct {
	bqiface.Client
}

type mockExporter struct{}

type mockHistogramTable struct{}

func (ex *mockExporter) Export(context.Context, config.Config, *template.Template, string) error {
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

	newHistogramTable = func(name, ds, query string,
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
				"/v0/pipeline?year=2020&step=all", bytes.NewReader([]byte{})),
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
			name: "missing-parameter-year",
			r: httptest.NewRequest(http.MethodPost, "/v0/pipeline",
				nil),
			statusCode: http.StatusBadRequest,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{},
				Errors:         []string{errMissingYear},
			},
		},
		{
			name: "missing-parameter-step",
			r: httptest.NewRequest(http.MethodPost,
				"/v0/pipeline?year=2020", nil),
			statusCode: http.StatusBadRequest,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{},
				Errors:         []string{errMissingStep},
			},
		},
		{
			name:       "action-histogram",
			r:          httptest.NewRequest(http.MethodPost, "/v0/pipeline?year=2020&step=histograms", bytes.NewReader([]byte{})),
			statusCode: http.StatusOK,
			response: &pipelineResult{
				CompletedSteps: []pipelineStep{histogramsStep},
				Errors:         []string{},
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
		t.Errorf("NewHandler() returned nil")
	}
	if h.bqClient != mc || h.exporter != me || !reflect.DeepEqual(h.configs, config) {
		t.Errorf("NewHandler() didn't return the expected handler")
	}
	// Check we can read from the channel.
	if _, ok := <-h.pipelineCanRun; !ok {
		t.Errorf("NewHandler() didn't return a properly initialized handler.")
	}
}
