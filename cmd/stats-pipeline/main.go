package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"runtime"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/httpx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/go/uploader"
	"github.com/m-lab/stats-pipeline/config"
	"github.com/m-lab/stats-pipeline/exporter"
	"github.com/m-lab/stats-pipeline/formatter"
	"github.com/m-lab/stats-pipeline/output"
	"github.com/m-lab/stats-pipeline/pipeline"
)

const dateFormat = "2006-01-02"

var (
	project    string
	listenAddr string
	bucket     string
	outputType = flagx.Enum{
		Options: []string{"gcs", "local"},
		Value:   "gcs",
	}
	exportType = flagx.Enum{
		Options: []string{"stats", "annotation", "hopannotation1"},
		Value:   "stats",
	}

	configFile = flagx.File{}
	mainCtx    = context.Background()
)

func init() {
	flag.StringVar(&listenAddr, "listenaddr", ":8080", "Address to listen on")
	flag.StringVar(&project, "project", "mlab-sandbox",
		"GCP Project ID to use")
	flag.StringVar(&bucket, "bucket", "statistics-mlab-sandbox",
		"GCS bucket to export the result to")
	flag.Var(&configFile, "config", "JSON configuration file")
	flag.Var(&outputType, "output", "Output to gcs or local files.")
	flag.Var(&exportType, "export", "Generate and export the named data type.")
}

func makeHTTPServer(listenAddr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:    listenAddr,
		Handler: h,
	}
}

func main() {
	flag.Parse()
	log.SetFlags(log.LUTC | log.Lshortfile | log.LstdFlags)
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not parse env args")

	// Try parsing provided config file.
	var configs map[string]config.Config
	err := json.Unmarshal(configFile.Get(), &configs)
	rtx.Must(err, "cannot parse configuration file")

	bqClient, err := bigquery.NewClient(mainCtx, project)
	rtx.Must(err, "error initializing BQ client")

	gcsClient, err := storage.NewClient(mainCtx)
	rtx.Must(err, "error initializing GCS client")

	var wr exporter.Writer
	switch outputType.Value {
	case "gcs":
		wr = output.NewGCSWriter(uploader.New(stiface.AdaptClient(gcsClient), bucket))
	case "local":
		wr = output.NewLocalWriter(mainCtx, bucket)
	}

	var f exporter.Formatter
	switch exportType.Value {
	case "stats":
		f = formatter.NewStatsQueryFormatter()
	case "annotation":
		f = formatter.NewTCPINFOAnnotationQueryFormatter()
	case "hopannotation1":
		f = formatter.NewTracerouteHopAnnotation1QueryFormatter()
	}
	exp := exporter.New(bqiface.AdaptClient(bqClient), project, wr, f)

	// Initialize handlers.
	pipelineHandler := pipeline.NewHandler(bqiface.AdaptClient(bqClient),
		exp, configs)

	// Initialize mux.
	mux := http.NewServeMux()
	mux.Handle("/v0/pipeline", pipelineHandler)

	log.Printf("GOMAXPROCS is %d", runtime.GOMAXPROCS(0))

	// Start main HTTP server.
	s := makeHTTPServer(listenAddr, mux)
	rtx.Must(httpx.ListenAndServeAsync(s), "Could not start HTTP server")
	defer s.Close()

	// Start Prometheus server for monitoring.
	promServer := prometheusx.MustServeMetrics()
	defer promServer.Close()

	// Keep serving until the context is canceled.
	<-mainCtx.Done()
}
