package exporter

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"log"
	"regexp"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/m-lab/stats-pipeline/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/api/iterator"
)

// Field used for partitioning tables. Must match what is used in the histogram
// queries.
const partitionField = "shard"

var (
	fieldRegex           = regexp.MustCompile(`{{\s*\.([A-Za-z0-9_]+)\s*}}`)
	bytesProcessedMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stats_pipeline_exporter_bytes_processed",
		Help: "Bytes processed by the exporter",
	}, []string{
		"table",
	})

	cacheHitMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stats_pipeline_exporter_cache_hits",
		Help: "Number of cache hits",
	}, []string{
		"table",
	})

	uploadedBytesMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stats_pipeline_exporter_uploaded_bytes",
		Help: "Bytes uploaded to GCS",
	}, []string{
		"table",
	})

	queryTotalMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stats_pipeline_exporter_queries",
		Help: "Export queries to be processed for the current table",
	}, []string{
		"table",
	})

	queryProcessedMetric = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stats_pipeline_exporter_queries_processed",
		Help: "Queries processed for the current table",
	}, []string{
		"table",
	})

	inFlightUploadsHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "stats_pipeline_exporter_inflight_uploads",
			Help:    "Inflight uploads histogram",
			Buckets: []float64{1, 2, 4, 8, 16},
		},
		[]string{"table"},
	)

	// Histogram bucket to record the upload queue size.
	uploadQueueSizeHistogram = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "stats_pipeline_exporter_uploads_queue_size",
			Help:    "Upload queue size histogram",
			Buckets: []float64{0, 1, 2, 4, 8},
		},
		[]string{"table"},
	)

	// Number of goroutines for querying BQ.
	nQueryWorkers = flag.Int("exporter.query-workers", 15,
		"Number of goroutines to use for parallel querying")

	// Name of the field used to partition tables.
	nUploadWorkers = flag.Int("exporter.upload-workers", 30,
		"Number of goroutines to use for parallel upload")
)

// Convenience type for a bigquery row.
type bqRow = map[string]bigquery.Value

// Writer defines the interface for saving files to GCS or locally.
type Writer interface {
	Write(ctx context.Context, path string, content []byte) error
}

// JSONExporter is a JSON exporter for histogram data on BigQuery.
type JSONExporter struct {
	bqClient  bqiface.Client
	projectID string
	output    Writer
	format    Formatter

	queryJobs  chan *QueryJob
	uploadJobs chan *UploadJob
	results    chan UploadResult

	queriesDone     int32
	uploadQLen      int32
	inflightUploads int32
}

// UploadJob is a job for uploading data to a GCS bucket.
type UploadJob struct {
	table   string
	objName string
	content []byte
}

// UploadResult is the result of a GCS upload.
type UploadResult struct {
	objName string
	err     error
}

// QueryJob is a job for running queries on BQ.
type QueryJob struct {
	name       string
	query      string
	fields     []string
	outputPath *template.Template
}

// New creates a new JSONExporter.
func New(bqClient bqiface.Client, projectID string, output Writer, format Formatter) *JSONExporter {
	return &JSONExporter{
		bqClient:  bqClient,
		projectID: projectID,
		output:    output,
		format:    format,

		queryJobs:  make(chan *QueryJob),
		uploadJobs: make(chan *UploadJob),
		results:    make(chan UploadResult),
	}
}

// Formatter is the interface for all types that format table sources and
// queries used during the export process.
type Formatter interface {
	Source(project string, config config.Config, year int) string
	Partitions(source string) string
	Where(row map[string]bigquery.Value) string
	Marshal(rows []map[string]bigquery.Value) ([]byte, error)
}

// Export runs the provided SQL query and, for each row in the result, uploads
// a file to the provided config.OutputPath on GCS. This file contains the JSON
// representation of the rows.
// config.OutputPath is a template whose parameters are provided by the BigQuery
// row's fields. If a field is present in the output path, it's removed from the
// BigQuery row's JSON representation, to reduce redundancy.
//
// e.g. if outputPath is "{{ .year }}/output.json" and we have a row per year,
// the histograms will be uploaded to:
// - 2010/output.json
// - 2020/output.json
// - etc.
//
// If any of the steps (running the query, reading the result, marshalling,
// uploading) fails, this function returns the corresponding error.
//
// Note: config.OutputPath should not start with a "/".
func (exporter *JSONExporter) Export(ctx context.Context,
	config config.Config, queryTpl *template.Template,
	year int) error {

	// Retrieve list of fields from the output path template string.
	fields, err := getFieldsFromPath(config.OutputPath)
	if err != nil {
		return err
	}
	log.Printf("Fields: %s", fields)

	// Make output path template.
	outputPath, err := template.New("outputPath").Parse(config.OutputPath)
	if err != nil {
		return err
	}

	// The fully qualified name for a table is project.dataset.table_year.
	sourceTable := exporter.format.Source(exporter.projectID, config, year)

	// Generate WHERE clauses to shard the export query.
	clauses, err := exporter.getPartitionFilters(ctx, sourceTable)
	if err != nil {
		log.Print(err)
		return err
	}

	// Create channels for query/upload jobs and results.
	exporter.queryJobs = make(chan *QueryJob)
	exporter.uploadJobs = make(chan *UploadJob)
	exporter.results = make(chan UploadResult)

	// Set counters to zero.
	exporter.uploadQLen = 0
	exporter.inflightUploads = 0
	exporter.queriesDone = 0

	// Reset metrics for this table to zero.
	resetMetrics(config.Table)
	inFlightUploadsHistogram.Reset()
	uploadQueueSizeHistogram.Reset()

	// The number of queries to run is the same as the number of clauses
	// generated earlier.
	queryTotalMetric.WithLabelValues(config.Table).Set(float64(len(clauses)))

	// Start a goroutine to print statistics periodically.
	printStatsCtx, cancelPrintStats := context.WithCancel(ctx)
	go exporter.printStats(printStatsCtx, len(clauses))
	defer cancelPrintStats()

	queryWg := sync.WaitGroup{}
	// Create queryWorkers.
	for w := 1; w <= *nQueryWorkers; w++ {
		queryWg.Add(1)
		log.Printf("Created queryWorker with ID: %d\n", w)
		go exporter.queryWorker(ctx, &queryWg)
	}

	// Create uploadWorkers.
	uploadWg := sync.WaitGroup{}
	for w := 1; w <= *nUploadWorkers; w++ {
		uploadWg.Add(1)
		go exporter.uploadWorker(ctx, &uploadWg)
	}

	// The goroutines' termination is controlled by closing the channels they
	// work on. The fist WaitGroup makes sure all the query workers have been
	// terminated before terminating the upload workers. The second one makes
	// sure all the upload workers have been terminated before returning.
	// This makes sure close/wait are always called, and in the right order.
	defer func() {
		close(exporter.queryJobs)
		queryWg.Wait()
		close(exporter.uploadJobs)
		uploadWg.Wait()
		close(exporter.results)
	}()

	for _, v := range clauses {
		// If the context has been canceled, stop sending jobs.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Execute the query template and send the query to one of the
		// available queryWorker functions.
		var buf bytes.Buffer
		err = queryTpl.Execute(&buf, map[string]string{
			"sourceTable": sourceTable,
			"whereClause": v,
		})
		if err != nil {
			log.Print(err)
			break
		}

		// Send a new QueryJob to the channel.
		exporter.queryJobs <- &QueryJob{
			name:       config.Table,
			query:      buf.String(),
			fields:     fields,
			outputPath: outputPath,
		}
		// Atomically increase the queriesDone counter and update metric.
		atomic.AddInt32(&exporter.queriesDone, 1)
		queryProcessedMetric.WithLabelValues(config.Table).Inc()
	}
	return nil
}

// queryWorker reads the next available QueryJob from the queryJobs channel and
// processes the result.
func (exporter *JSONExporter) queryWorker(ctx context.Context,
	wg *sync.WaitGroup) {

	// Make sure we decrement the waitgroup's counter before returning.
	defer wg.Done()

	for j := range exporter.queryJobs {
		// If the context has been canceled, stop processing jobs.
		if ctx.Err() != nil {
			return
		}
		// Run the SELECT query to get histogram data.
		log.Printf("Running query: %s", j.query)
		q := exporter.bqClient.Query(j.query)
		job, err := q.Run(ctx)
		if err != nil {
			log.Print(err)
			continue
		}
		jobStatus, err := job.Wait(ctx)
		if err != nil {
			log.Print(err)
			continue
		}
		if jobStatus.Err() != nil {
			log.Print(err)
			continue
		}
		it, err := job.Read(ctx)
		if err != nil {
			log.Print(err)
			continue
		}
		// Update bytes processed.
		if queryDetails, ok := jobStatus.Statistics.Details.(*bigquery.QueryStatistics); ok {
			if queryDetails.CacheHit {
				cacheHitMetric.WithLabelValues(j.name).Inc()
			}
			bytesProcessedMetric.WithLabelValues(j.name).Add(float64(queryDetails.TotalBytesProcessed))
		}
		// Iterate over the returned rows and upload results to GCS.
		err = exporter.processQueryResults(it, j)
		if err != nil {
			log.Print(err)
			continue
		}
	}
}

// processQueryResults loops over a RowIterator.
// For each row it generates a row key combining the fields in QueryJob.fields.
// When the row key changes, it means a file containing the rows read so far
// is ready to be uploaded, so the rows are marshalled and sent to the
// uploadJobs channel.
//
// For every UploadJob sent over the channel, it also atomically increments
// uploadCounter.
func (exporter *JSONExporter) processQueryResults(it bqiface.RowIterator,
	j *QueryJob) error {
	var currentFile []bqRow
	var lastRow bqRow
	var currentRow bqRow
	var err error
	// Setting MaxSize here allows to fetch more rows with a single fetch.
	it.PageInfo().MaxSize = 100000

	for err = it.Next(&currentRow); err == nil; err = it.Next(&currentRow) {
		// If any of j.fields changed between this row and the previous one,
		// upload the current file. Ignore the first row.
		if lastRow != nil {
			for _, f := range j.fields {
				if currentRow[f] != lastRow[f] {
					// upload file, empty currentFile, break
					exporter.uploadFile(j, currentFile, lastRow)
					currentFile = nil
					break
				}
			}
		}
		// We are in the middle or start of a file, so just append the current
		// row to currentFile. The partitionField is removed from the output.
		currentFile = append(currentFile, removeFieldsFromRow(currentRow,
			[]string{partitionField}))
		// Save relevant fields for comparison in the next iteration.
		// Note: we can't just do lastRow = currentRow here as it would be a
		// reference; we need to copy.
		if lastRow == nil {
			lastRow = make(bqRow)
		}
		for _, f := range j.fields {
			lastRow[f] = currentRow[f]
		}
	}

	if err == iterator.Done {
		// If this was the last row, upload the file so far.
		exporter.uploadFile(j, currentFile, lastRow)
		// This is the expected behavior, so we don't consider this an error.
		return nil
	}

	return err
}

// uploadFile marshals the BigQuery rows and uploads the resulting JSON to the
// GCS path defined in the QueryJob. Template variables are taken from the
// first row in the slice.
func (exporter *JSONExporter) uploadFile(j *QueryJob, rows []bqRow, lastRow bqRow) error {
	if len(rows) == 0 {
		return errors.New("empty rows slice")
	}
	buf := new(bytes.Buffer)
	// Use the first row to fill in the template variables.
	err := j.outputPath.Execute(buf, lastRow)
	if err != nil {
		return err
	}
	atomic.AddInt32(&exporter.uploadQLen, 1)
	exporter.marshalAndUpload(j.name, buf.String(), rows, exporter.uploadJobs)
	return nil
}

// uploadWorker receives UploadJobs from the channel and uploads files to GCS.
func (exporter *JSONExporter) uploadWorker(ctx context.Context, wg *sync.WaitGroup) {

	// Make sure we decrement the waitgroup's counter before returning.
	defer wg.Done()

	for j := range exporter.uploadJobs {
		// If the context has been canceled, stop processing jobs.
		if ctx.Err() != nil {
			return
		}
		// The uploadQueue counter is decremented before starting to upload
		// the file, so that in-flight uploads aren't counted.
		// After this, we observe the size of the upload queue and put its
		// length in a Prometheus metric.
		atomic.AddInt32(&exporter.uploadQLen, -1)
		uploadQueueSizeHistogram.WithLabelValues(j.table).Observe(float64(
			atomic.LoadInt32(&exporter.uploadQLen)))

		atomic.AddInt32(&exporter.inflightUploads, 1)
		inFlightUploadsHistogram.WithLabelValues(j.table).Observe(float64(
			atomic.LoadInt32(&exporter.inflightUploads)))
		err := exporter.output.Write(ctx, j.objName, j.content)
		atomic.AddInt32(&exporter.inflightUploads, -1)

		uploadedBytesMetric.WithLabelValues(j.table).Add(float64(len(j.content)))
		exporter.results <- UploadResult{
			objName: j.objName,
			err:     err,
		}
	}
}

// getPartitionFilters returns all the WHERE clauses to filter by partition,
// sorted by decreasing size. This allows to process the largest partitions
// first.
// E.g. if partitionField is continent_code_hash, this will return 7 clauses:
// - WHERE continent_code_hash = <partition>
// - [...]
func (exporter *JSONExporter) getPartitionFilters(ctx context.Context,
	fullyQualifiedTable string) ([]string, error) {
	partitions := exporter.format.Partitions(fullyQualifiedTable)
	log.Print(partitions)
	q := exporter.bqClient.Query(partitions)
	it, err := q.Read(ctx)
	if err != nil {
		log.Print(err)
		return nil, err
	}
	// Generate the complete where clause for each query.
	var clauses []string
	for {
		var row bqRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, exporter.format.Where(row))
	}
	return clauses, nil
}

// marshalAndUpload marshals the BigQuery rows into a JSON array and sends a
// new UploadJob to the uploadJobs channel so the result is uploaded to GCS as
// objName.
func (exporter *JSONExporter) marshalAndUpload(tableName, objName string, rows []bqRow,
	uploadJobs chan<- *UploadJob) error {
	j, err := exporter.format.Marshal(rows)
	if err != nil {
		return err
	}

	uploadJobs <- &UploadJob{
		table:   tableName,
		objName: objName,
		content: j,
	}
	return nil
}

// printStats prints statistics about the ongoing export every second.
func (exporter *JSONExporter) printStats(ctx context.Context, totQueries int) {
	uploaded := 0
	errors := 0
	start := time.Now()
	t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case res := <-exporter.results:
			if res.err != nil {
				errors++
			} else {
				uploaded++
			}
		case <-t.C:
			log.Printf(
				"Elapsed: %s, queries: %d/%d, uploaded: %d (%d errors), files/s: %f, UL queue: %d",
				time.Since(start).Round(time.Second).String(),
				atomic.LoadInt32(&exporter.queriesDone), totQueries, uploaded, errors,
				float64(uploaded)/time.Since(start).Seconds(),
				atomic.LoadInt32(&exporter.uploadQLen))
		}
	}
}

// getFieldsFromPath takes the outputPath template string and returns the
// fields matched by the capture group in fieldRegex.
func getFieldsFromPath(path string) ([]string, error) {
	var fields []string
	matches := fieldRegex.FindAllStringSubmatch(path, -1)
	if len(matches) == 0 {
		return nil, errors.New("no fields found in the path template")
	}
	for _, m := range matches {
		fields = append(fields, m[1])
	}
	return fields, nil
}

// removeFieldsFromRow returns a new BQ row without the specified fields. It
// also removes the partitioning field if present.
func removeFieldsFromRow(row bqRow, fields []string) bqRow {
	newRow := bqRow{}
	for fieldName, fieldValue := range row {
		found := false
		for _, k := range fields {
			if fieldName == k {
				found = true
			}
		}
		if !found {
			newRow[fieldName] = fieldValue
		}
	}
	return newRow
}

// resetMetrics sets all the metrics for a given table to zero.
func resetMetrics(tableName string) {
	queryProcessedMetric.WithLabelValues(tableName).Set(0)
	uploadedBytesMetric.WithLabelValues(tableName).Set(0)
	bytesProcessedMetric.WithLabelValues(tableName).Set(0)
	cacheHitMetric.WithLabelValues(tableName).Set(0)
}
