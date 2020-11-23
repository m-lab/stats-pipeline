package pipeline

import (
	"bytes"
	"context"
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

func (ex *mockExporter) Export(context.Context, config.ExportConfig, *template.Template, string) error {
	return nil
}

func (h *mockHistogramTable) UpdateHistogram(context.Context, time.Time, time.Time) error {
	return nil
}

func TestHandler_ServeHTTP(t *testing.T) {
	mc := &mockClient{}
	me := &mockExporter{}
	conf := config.Config{
		Histograms: map[string]config.HistogramConfig{
			"test": {
				Dataset:   "test",
				QueryFile: "testdata/test_histogram.sql",
				Table:     "testtable",
			},
		},
		Exports: map[string]config.ExportConfig{
			"test": {
				OutputPath:  "{{.test}}/output.json",
				QueryFile:   "testdata/test_export.sql",
				SourceTable: "testtable",
			},
		},
	}

	newHistogramTable = func(name string, ds string, query string, client bqiface.Client) HistogramTable {
		return &mockHistogramTable{}
	}

	tests := []struct {
		name       string
		w          http.ResponseWriter
		r          *http.Request
		bqClient   bqiface.Client
		exporter   Exporter
		config     config.Config
		statusCode int
	}{
		{
			name:       "ok",
			bqClient:   mc,
			exporter:   me,
			config:     conf,
			r:          httptest.NewRequest(http.MethodPost, "/v0/pipeline?year=2020", bytes.NewReader([]byte{})),
			statusCode: http.StatusOK,
		},
		{
			name:       "invalid-method",
			r:          httptest.NewRequest(http.MethodGet, "/v0/pipeline?year=2020", nil),
			statusCode: http.StatusMethodNotAllowed,
		},
		{
			name:       "missing-parameter",
			r:          httptest.NewRequest(http.MethodPost, "/v0/pipeline", nil),
			statusCode: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handler{
				bqClient: tt.bqClient,
				exporter: tt.exporter,
				config:   tt.config,
			}
			recorder := httptest.NewRecorder()
			h.ServeHTTP(recorder, tt.r)
			statusCode := recorder.Result().StatusCode
			if statusCode != tt.statusCode {
				t.Errorf("ServeHTTP(): expected %v, got %v", tt.statusCode, statusCode)
			}
		})
	}
}

func TestNewHandler(t *testing.T) {
	mc := &mockClient{}
	me := &mockExporter{}
	config := config.Config{}
	h := NewHandler(mc, me, config)
	if h == nil {
		t.Errorf("NewHandler() returned nil")
	}
	if h.bqClient != mc || h.exporter != me || !reflect.DeepEqual(h.config, config) {
		t.Errorf("NewHandler() didn't return the expected handler")
	}
}
