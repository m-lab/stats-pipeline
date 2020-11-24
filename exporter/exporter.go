package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/go/uploader"
	"github.com/m-lab/stats-pipeline/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/api/iterator"
)

const (
	// TODO(roberto): make these values configurable, either from a
	// command-line argument or on a per-export basis in the configuration
	// file. There is probably some potential for extra optimizations here.
	// Number of goroutines for querying BQ.
	nQueryWorkers = 15

	// Numbers of goroutines for uploading to GCS.
	nUploadWorkers = 25
)

var (
	fieldRegex           = regexp.MustCompile(`{{\s*\.([A-Za-z0-9_]+)\s*}}`)
	bytesProcessedMetric = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stats_pipeline_exporter_bytes_processed_total",
		Help: "Bytes processed by the exporter",
	}, []string{
		"table",
	})

	cacheHitMetric = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stats_pipeline_exporter_cache_hit_total",
		Help: "Number of cache hits",
	}, []string{
		"table",
	})

	uploadedBytesMetric = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stats_pipeline_exporter_uploaded_bytes_total",
		Help: "Bytes uploaded to GCS",
	}, []string{
		"table",
	})
)

// Convenience type for a bigquery row.
type bqRow = map[string]bigquery.Value

// JSONExporter is a JSON exporter for histogram data on BigQuery.
type JSONExporter struct {
	bqClient      bqiface.Client
	storageClient stiface.Client

	bucket    string
	projectID string
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
func New(bqClient bqiface.Client, storageClient stiface.Client, projectID,
	bucket string) *JSONExporter {
	return &JSONExporter{
		bqClient:      bqClient,
		storageClient: storageClient,
		projectID:     projectID,
		bucket:        bucket,
	}
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
	year string) error {

	// Retrieve list of fields from the output path template string.
	fields := getFieldsFromPath(config.OutputPath)
	log.Printf("Fields: %s", fields)

	// Make output path template.
	outputPath, err := template.New("outputPath").Parse(config.OutputPath)
	if err != nil {
		return err
	}

	// The fully qualified name for a table is project.dataset.table_year.
	sourceTable := fmt.Sprintf("%s.%s.%s_%s", exporter.projectID, config.Dataset,
		config.Table, year)

	// Generate WHERE clauses to shard the export query.
	clauses, err := exporter.generateWhereClauses(ctx, sourceTable, config.PartitionField)
	if err != nil {
		log.Print(err)
		return err
	}

	// Create channels for query/upload jobs and results.
	queryJobs := make(chan *QueryJob)
	uploadJobs := make(chan *UploadJob)
	results := make(chan UploadResult)

	// Counters to record progress.
	var uploadQLen int32
	var queriesDone int32

	// Start a goroutine to print statistics periodically.
	printStatsCtx, cancelPrintStats := context.WithCancel(ctx)
	go printStats(printStatsCtx, &uploadQLen, &queriesDone, len(clauses), results)
	defer cancelPrintStats()

	queryWg := sync.WaitGroup{}
	// Create queryWorkers.
	for w := 1; w <= nQueryWorkers; w++ {
		queryWg.Add(1)
		go exporter.queryWorker(ctx, &uploadQLen, &queryWg, queryJobs, uploadJobs)
	}

	// Create uploadWorkers.
	up := uploader.New(exporter.storageClient, exporter.bucket)
	uploadWg := sync.WaitGroup{}
	for w := 1; w <= nUploadWorkers; w++ {
		uploadWg.Add(1)
		go exporter.uploadWorker(ctx, &uploadQLen, &uploadWg, up, uploadJobs, results)
	}

	for _, v := range clauses {
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
		select {
		case <-ctx.Done():
			// If the context has been closed, we stop writing to the channel.
			break
		default:
			log.Printf("Running query: %s", buf.String())
			queryJobs <- &QueryJob{
				name:       sourceTable,
				query:      buf.String(),
				fields:     fields,
				outputPath: outputPath,
			}
			atomic.AddInt32(&queriesDone, 1)
		}
	}
	// The goroutines' termination is controlled by closing the channels they
	// work on. The fist WaitGroup makes sure all the query workers have been
	// terminated before terminating the upload workers. The second one makes
	// sure all the upload workers have been terminated before returning.
	close(queryJobs)
	queryWg.Wait()
	close(uploadJobs)
	uploadWg.Wait()
	return nil
}

// queryWorker reads the next available QueryJob from the queryJobs channel and
// processes the result.
//
// For each row it generates a row key based on the fields in QueryJob.fields.
// When the row key changes, it means a file containing the rows read so far
// is ready to be uploaded, so the rows are marshalled and sent to the
// uploadJobs channel.
//
// NOTE: for this to work correctly, the results MUST be ordered by the fields
// in queryJob.fields.
func (exporter *JSONExporter) queryWorker(ctx context.Context,
	uploadCounter *int32, wg *sync.WaitGroup, queryJobs <-chan *QueryJob,
	uploadJobs chan<- *UploadJob) {

	// Make sure we decrement the waitgroup's counter before returning.
	defer wg.Done()

	for j := range queryJobs {
		// Run the SELECT query to get histogram data.
		q := exporter.bqClient.Query(j.query)
		job, err := q.Run(ctx)
		if err != nil {
			log.Print(err)
			break
		}
		jobStatus, err := job.Wait(ctx)
		if err != nil {
			log.Print(err)
			break
		}
		if jobStatus.Err() != nil {
			log.Print(err)
			break
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
		var currentFile []bqRow
		var currentRowKey string
		var oldRow bqRow
		for {
			var currentRow bqRow
			it.PageInfo().MaxSize = 100000
			err := it.Next(&currentRow)
			if err != nil {
				if err == iterator.Done {
					// If this was the last row, upload the file so far.
					buf := new(bytes.Buffer)
					err = j.outputPath.Execute(buf, oldRow)
					if err != nil {
						log.Printf("Cannot generate path (tpl:%s): %v",
							j.outputPath.Root.String(), err)
						break
					}
					atomic.AddInt32(uploadCounter, 1)
					marshalAndUpload(j.name, buf.String(), currentFile, uploadJobs)
				} else {
					if !errors.Is(err, context.Canceled) {
						// If there was an unexpected error while reading the
						// next row, print the error and return.
						log.Print(err)
					}
				}
				break
			}
			// Calculate the row key by combining all the fields that are in the
			// output path template.
			rowKey := ""
			for _, f := range j.fields {
				// Try reading the field as either string or int64 and
				// append the result to rowKey.
				if s, ok := currentRow[f].(string); ok {
					rowKey += s
				} else if i, ok := currentRow[f].(int64); ok {
					rowKey += strconv.Itoa(int(i))
				}
			}
			// If this is the first row, currentRowKey will be empty.
			if currentRowKey == "" {
				currentRowKey = rowKey
			} else if rowKey != currentRowKey {
				// If the row key has changed, send the current rows to GCS
				// and start a new file.
				buf := new(bytes.Buffer)
				err = j.outputPath.Execute(buf, oldRow)
				if err != nil {
					log.Printf("Cannot generate path (tpl:%s): %v",
						j.outputPath.Root.String(), err)
					break
				}
				atomic.AddInt32(uploadCounter, 1)
				marshalAndUpload(j.name, buf.String(), currentFile, uploadJobs)
				// Empty currentFile and set currentRowKey to the new
				// rowKey.
				currentFile = currentFile[:0]
				currentRowKey = rowKey
			}
			// We are in the middle of a file, so just append the current
			// row to currentFile. Fields that appear in the output path
			// are removed to avoid redundancy in the JSON and create
			// smaller files.
			currentFile = append(currentFile, removeFieldsFromRow(currentRow, j.fields))
			oldRow = currentRow
		}
	}
}

// uploadWorker receives UploadJobs from the channel and uploads files to GCS.
func (exporter *JSONExporter) uploadWorker(ctx context.Context,
	uploadQueue *int32, wg *sync.WaitGroup,
	up *uploader.Uploader, jobs <-chan *UploadJob,
	results chan<- UploadResult) {

	// Make sure we decrement the waitgroup's counter before returning.
	defer wg.Done()

	for j := range jobs {
		// The uploadQueue counter is decremented before starting to upload
		// the file, so that in-flight uploads aren't counted.
		atomic.AddInt32(uploadQueue, -1)
		_, err := up.Upload(ctx, j.objName, j.content)
		uploadedBytesMetric.WithLabelValues(j.table).Add(float64(len(j.content)))
		results <- UploadResult{
			objName: j.objName,
			err:     err,
		}
	}
}

// generateWhereClauses generates all the WHERE clauses to filter by partition,
// sorted by decreasing size. This allows to process the largest partitions
// first.
func (exporter *JSONExporter) generateWhereClauses(ctx context.Context,
	fullyQualifiedTable, partitionField string) ([]string, error) {
	selectQuery := fmt.Sprintf(`SELECT %s, count(*) FROM %s GROUP BY %[1]s
		ORDER BY count(*) DESC`, partitionField, fullyQualifiedTable)
	log.Print(selectQuery)
	q := exporter.bqClient.Query(selectQuery)
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
		partition := row[partitionField].(int64)
		clauses = append(clauses, fmt.Sprintf("WHERE %s = %d", partitionField, partition))
	}
	return clauses, nil
}

// marshalAndUpload marshals the BigQuery rows into a JSON array and sends a
// new UploadJob to the uploadJobs channel so the result is uploaded to GCS as
// objName.
func marshalAndUpload(tableName, objName string, rows []bqRow,
	uploadJobs chan<- *UploadJob) error {
	j, err := json.Marshal(rows)
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
func printStats(ctx context.Context, uploadQLen *int32, queriesDone *int32,
	totQueries int, results <-chan UploadResult) {
	uploaded := 0
	errors := 0
	start := time.Now()
	t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case res := <-results:
			if res.err != nil {
				errors++
			} else {
				uploaded++
			}
		case <-t.C:
			log.Printf(
				"Elapsed: %s, queries: %d/%d, uploaded: %d (%d errors), files/s: %f, UL queue: %d",
				time.Since(start).Round(time.Second).String(),
				atomic.LoadInt32(queriesDone), totQueries, uploaded, errors,
				float64(uploaded)/time.Since(start).Seconds(),
				atomic.LoadInt32(uploadQLen))
		}
	}
}

// getFieldsFromPath takes the outputPath template string and returns the
// fields matched by the capture group in fieldRegex.
func getFieldsFromPath(path string) []string {
	var fields []string
	matches := fieldRegex.FindAllStringSubmatch(path, -1)
	for _, m := range matches {
		fields = append(fields, m[1])
	}
	return fields
}

// removeFieldsFromRow returns a new BQ row without the specified fields.
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
