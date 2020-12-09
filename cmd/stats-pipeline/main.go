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
	"github.com/m-lab/stats-pipeline/config"
	"github.com/m-lab/stats-pipeline/exporter"
	"github.com/m-lab/stats-pipeline/pipeline"
)

const dateFormat = "2006-01-02"

var (
	project    string
	listenAddr string
	bucket     string

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

	exporter := exporter.New(bqiface.AdaptClient(bqClient),
		stiface.AdaptClient(gcsClient), project, bucket)

	// Initialize handlers.
	pipelineHandler := pipeline.NewHandler(bqiface.AdaptClient(bqClient),
		exporter, configs)

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
