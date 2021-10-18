package types_test

import (
	"context"
	"testing"

	"github.com/m-lab/stats-pipeline/config"
	"github.com/m-lab/stats-pipeline/exporter/types"
)

func Test_Export(t *testing.T) {
	exp := types.New(nil, "", nil)
	err := exp.Export(context.Background(), config.Config{}, nil, "2020")
	if err == nil {
		t.Fatal("want error, got nil")
	}
}
