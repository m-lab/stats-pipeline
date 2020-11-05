package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"text/template/parse"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/go/uploader"
	"google.golang.org/api/iterator"
)

// Convenience type for a bigquery row.
type bqRow = map[string]bigquery.Value

// JSONExporter is a JSON exporter for histogram data on BigQuery.
type JSONExporter struct {
	bqClient      bqiface.Client
	storageClient stiface.Client

	bucket string
}

type UploadJob struct {
	objName string
	content []byte
}

type QueryJob struct {
	query      string
	fields     []string
	outputPath *template.Template
}

// New generates a new JSONExporter.
func New(bqClient bqiface.Client, storageClient stiface.Client, bucket string) *JSONExporter {
	return &JSONExporter{
		bqClient:      bqClient,
		storageClient: storageClient,
		bucket:        bucket,
	}
}

// Export runs the provided SQL query and, for each row in the result, uploads
// a file to the provided outputPath on GCS. This file contains the JSON
// representation of the "histograms" field, which must be present on each row.
// The outputPath is a template whose parameters are provided by the BigQuery
// row's fields.
// e.g. if outputPath is "{{ .Year }}/output.json" and we have a row per year,
// the histograms will be uploaded to:
// - 2010/output.json
// - 2020/output.json
// - etc.
//
// If any of the steps (running the query, reading the result, marshalling,
// uploading) fails, this function returns the corresponding error.
//
// Note: outputPath should not start with a "/".
func (exporter *JSONExporter) Export(ctx context.Context, sourceTable string,
	queryTpl *template.Template, outputPath *template.Template) error {
	// Retrieve list of fields from the output path template.
	var templateFields []string
	var fields []string
	fieldRegex, err := regexp.Compile(`{{\s*\.([A-Za-z0-9_]+)\s*}}`)
	if err != nil {
		return err
	}
	templateFields = listNodeFields(outputPath.Tree.Root, templateFields)
	log.Printf("Template fields: %s", templateFields)
	for _, f := range templateFields {
		m := fieldRegex.FindStringSubmatch(f)
		fields = append(fields, m[1])
	}
	log.Printf("Fields: %s", fields)

	// Generate WHERE clauses to shard the export query.
	clauses, err := exporter.generateWhereClauses(ctx, sourceTable, fields)
	if err != nil {
		log.Print(err)
		return err
	}

	queryJobs := make(chan *QueryJob)
	uploadJobs := make(chan *UploadJob)
	results := make(chan error)
	var uploadCounter int32
	go func(counter *int32) {
		for ctx.Err() == nil {
			log.Printf("Upload queue: %d", atomic.LoadInt32(counter))
			time.Sleep(1 * time.Second)
		}
	}(&uploadCounter)
	queryWg := sync.WaitGroup{}
	// Create queryWorkers and uploadWorkers.
	for w := 1; w <= 15; w++ {
		queryWg.Add(1)
		go exporter.queryWorker(w, &uploadCounter, &queryWg, ctx, queryJobs, uploadJobs)
	}
	up := uploader.New(exporter.storageClient, exporter.bucket)
	uploadWg := sync.WaitGroup{}
	for w := 1; w <= 20; w++ {
		uploadWg.Add(1)
		go exporter.uploadWorker(w, &uploadCounter, &uploadWg, ctx, up, uploadJobs, results)
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
			return err
		}
		queryJobs <- &QueryJob{
			query:      buf.String(),
			fields:     fields,
			outputPath: outputPath,
		}
	}
	close(queryJobs)
	queryWg.Wait()
	close(uploadJobs)
	return nil
}

func (gen *JSONExporter) UploadFile(name string, rows []map[string]bigquery.Value,
	jobs chan<- *UploadJob) error {
	j, err := json.Marshal(rows)
	if err != nil {
		return err
	}

	jobs <- &UploadJob{
		objName: name,
		content: j,
	}
	return nil
}

func (gen *JSONExporter) queryWorker(id int, uploadCounter *int32, wg *sync.WaitGroup,
	ctx context.Context,
	queryJobs <-chan *QueryJob,
	uploadJobs chan<- *UploadJob) {

	defer wg.Done()

	var j *QueryJob
	for {
		select {
		case <-ctx.Done():
			return
		case j = <-queryJobs:
			log.Printf("w: %d, q: %s", id, j.query)
			// Run the SELECT query to get histogram data
			q := gen.bqClient.Query(j.query)
			it, err := q.Read(ctx)
			if err != nil {
				log.Print(err)
				break
			}
			// Iterate over the returned rows and upload results to GCS.
			var currentFile []bqRow
			var currentRowKey string
			var oldRow bqRow
			for {
				var newRow bqRow
				it.PageInfo().MaxSize = 100000
				err := it.Next(&newRow)
				if err != nil {
					log.Print(err)
					break
				}
				if err == iterator.Done {
					log.Println("Done!")
					// If this was the last row, upload the file so far.
					buf := new(bytes.Buffer)
					err = j.outputPath.Execute(buf, oldRow)
					if err != nil {
						log.Printf("Cannot generate path (tpl:%s): %v",
							j.outputPath.Root.String(), err)
					}
					log.Printf("Uploading %s (len: %d)...", buf.String(), len(currentFile))
					atomic.AddInt32(uploadCounter, 1)
					gen.UploadFile(buf.String(), currentFile, uploadJobs)
					break
				}
				// Calculate the row key by combining all the fields that are in the
				// output path template.
				rowKey := ""
				for _, f := range j.fields {
					if s, ok := newRow[f].(string); ok {
						rowKey += s
					} else if i, ok := newRow[f].(int64); ok {
						rowKey += strconv.Itoa(int(i))
					}
				}
				if currentRowKey == "" {
					currentRowKey = rowKey
				} else if rowKey != currentRowKey {
					// If the row key has changed, send the current data to GCS and start
					// a new file.
					log.Printf("Row key: %s -> %s", currentRowKey, rowKey)
					buf := new(bytes.Buffer)
					err = j.outputPath.Execute(buf, oldRow)
					if err != nil {
						log.Printf("Cannot generate path (tpl:%s): %v",
							j.outputPath.Root.String(), err)
						break
					}
					//log.Printf("Uploading %s (len: %d)...", buf.String(), len(currentFile))
					atomic.AddInt32(uploadCounter, 1)
					gen.UploadFile(buf.String(), currentFile, uploadJobs)

					currentFile = currentFile[:0]
					currentRowKey = rowKey
				}
				currentFile = append(currentFile, removeFieldsFromRow(newRow, j.fields))
				oldRow = newRow
			}
		}
	}
}

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

func (gen *JSONExporter) uploadWorker(id int, uploadCounter *int32,
	wg *sync.WaitGroup,
	ctx context.Context, up *uploader.Uploader,
	jobs <-chan *UploadJob, results chan<- error) {
	defer wg.Done()

	var j *UploadJob
	for {
		select {
		case <-ctx.Done():
			return
		case j = <-jobs:
			atomic.AddInt32(uploadCounter, -1)
			_, err := up.Upload(ctx, j.objName, j.content)
			if err != nil {
				results <- err
			}
			log.Printf("Uploaded: %s", j.objName)
		}
	}
}

func listNodeFields(node parse.Node, res []string) []string {
	if node.Type() == parse.NodeAction {
		res = append(res, node.String())
	}

	if ln, ok := node.(*parse.ListNode); ok {
		for _, n := range ln.Nodes {
			res = listNodeFields(n, res)
		}
	}
	return res
}

func (gen *JSONExporter) generateWhereClauses(ctx context.Context, tableName string,
	fields []string) ([]string, error) {
	// If we have two or fewer fields, there is no need for sharding so we
	// return a single, empty WHERE clause.
	if len(fields) <= 2 {
		return []string{""}, nil
	}
	// Get all the combinations of the first len(fields)-2 fields from the
	// input table.
	joinedFields := strings.Join(fields[:len(fields)-2], ", ")
	selectQuery := "SELECT " + joinedFields +
		" FROM " + tableName +
		" GROUP BY " + joinedFields +
		" ORDER BY " + joinedFields
	q := gen.bqClient.Query(selectQuery)
	it, err := q.Read(ctx)
	if err != nil {
		log.Print(err)
		return nil, err
	}
	// Generate the complete where clause for each result.
	var results []string
	for {
		var row bqRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		clauses := []string{}
		for k, v := range row {
			clauses = append(clauses, fmt.Sprintf("%s = \"%s\"", k, v))
		}
		log.Printf("WHERE " + strings.Join(clauses, " AND "))
		results = append(results, fmt.Sprintf("WHERE "+strings.Join(clauses, " AND ")))
	}
	return results, nil
}
