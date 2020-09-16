package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/stats-pipeline/exporter"
	"github.com/m-lab/stats-pipeline/histogram"
)

const (
	defaultProject = "mlab-sandbox"
	defaultDataset = "statistics"
	defaultTable   = "continent_country_region_histogram"
	defaultBucket  = "statistics-mlab-sandbox"

	defaultQueryTimeout = 5 * time.Minute
)

var (
	projectID   string
	destDataset string
	destTable   string
	destBucket  string

	timeout            time.Duration
	startDate          = flagx.DateTime{}
	endDate            = flagx.DateTime{}
	histogramQueryFile = flagx.File{}
	regionQueryFile    = flagx.File{}
)

func init() {
	flag.StringVar(&projectID, "project", defaultProject, "GCP Project ID to use")
	flag.StringVar(&destDataset, "dest.dataset", defaultDataset, "Destination dataset")
	flag.StringVar(&destTable, "dest.table", defaultTable, "Destination table")
	flag.StringVar(&destBucket, "dest.bucket", defaultBucket, "Destination GCS bucket")

	flag.DurationVar(&timeout, "query.timeout", defaultQueryTimeout,
		"Timeout for each BQ query")

	flag.Var(&startDate, "query.startdate", "Start date (YYYY-mm-dd)")
	flag.Var(&endDate, "query.enddate", "End date (YYYY-mm-dd)")

	flag.Var(&histogramQueryFile, "query.histogram",
		"Path to a file containing the histogram query")
	flag.Var(&regionQueryFile, "query.regions",
		"Path to a file containing the query to get all the available "+
			"continent/country/region combinations")
}

var mainCtx = context.Background()

func main() {
	flag.Parse()
	log.SetFlags(log.LUTC | log.Lshortfile | log.LstdFlags)
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not parse env args")

	// Check provided query has non-zero length.
	statsQuery := string(histogramQueryFile.Get())
	if len(statsQuery) == 0 {
		log.Fatalln("Please provide a valid histogram query file.")
	}

	client, err := bigquery.NewClient(mainCtx, projectID)
	if err != nil {
		log.Fatalf("Cannot connect to BigQuery: %v\n", err)
	}
	defer client.Close()

	t := histogram.NewTable(destTable, destDataset, statsQuery, client)
	err = t.UpdateHistogram(mainCtx, startDate.UTC(), endDate.UTC())
	rtx.Must(err, "Cannot update histogram table")

	gcsClient, err := storage.NewClient(mainCtx)
	b := gcsClient.Bucket(destBucket)

	ex := exporter.JSONExporter{
		Client:    bqiface.AdaptClient(client),
		TableName: projectID + "." + destDataset + "." + destTable,
		Bucket:    b,
	}
	err = ex.UploadContinentCountryRegion(mainCtx, "NA", "US", "2020")
	rtx.Must(err, "Cannot generate JSON data")
	if err != nil {
		fmt.Println(err)
	}
}
