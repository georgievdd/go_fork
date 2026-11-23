package pipeline_test

import (
	"os"
	"path/filepath"
	"testing"

	"example.com/order-pipeline/internal/pipeline"
	"example.com/order-pipeline/tests/testutil"
)

func TestRunPipelineRecordsStageLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "steps.jsonl")
	t.Setenv("ORDER_PIPELINE_TRACE", path)
	sinkReady := make(chan struct{})
	pipeline.RunPipeline(
		func(_ <-chan any, out chan<- any) {
			<-sinkReady
			out <- 42
		},
		func(in <-chan any, _ chan<- any) {
			close(sinkReady)
			for range in {
			}
		},
	)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("RunPipeline должен вызывать шаги через stagetrace.Run: %v", err)
	}
	testutil.AssertStageTrace(t, string(data))
}
