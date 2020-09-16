package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"cloud.google.com/go/storage"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"google.golang.org/api/iterator"
)

type JSONExporter struct {
	Client    bqiface.Client
	Bucket    *storage.BucketHandle
	TableName string
}

func (ex *JSONExporter) upload(ctx context.Context, data []byte,
	objPath string) error {
	obj := ex.Bucket.Object(objPath)
	w := obj.NewWriter(ctx)
	defer w.Close()

	if _, err := io.Copy(w, bytes.NewReader(data)); err != nil {
		return err
	}

	return nil
}

// UploadContinentCountryRegion generates and uploads a daily stats JSON for every
// continent/country/region combination.
func (ex *JSONExporter) UploadContinentCountryRegion(ctx context.Context, continent, country, year string) error {
	query := "SELECT * FROM " + ex.TableName + " WHERE continent_code = \"" +
		continent + "\" AND country_code = \"" + country +
		"\" AND EXTRACT(YEAR FROM test_date) = " + year

	log.Println(query)
	q := ex.Client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	type bqRow = map[string]bigquery.Value
	regionDailyHistograms := make(map[string][]bqRow)
	for {
		var row bqRow
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		region := row["ISO3166_2region1"].(string)
		regionDailyHistograms[region] = append(regionDailyHistograms[region], row)
	}
	for k, v := range regionDailyHistograms {
		j, err := json.Marshal(v)
		if err != nil {
			return err
		}
		path := fmt.Sprintf("/v0/%s/%s/%s/%s/histogram_daily_stats.json",
			continent, country, k, year)

		log.Printf("Uploading daily histogram to %s\n", path)
		err = ex.upload(ctx, j, path)
		if err != nil {
			log.Printf("Error uploading to %s: %v\n", path, err)
		}
	}
	return nil
}
