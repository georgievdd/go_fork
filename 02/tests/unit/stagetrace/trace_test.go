package stagetrace_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"example.com/order-pipeline/pkg/stagetrace"
	"example.com/order-pipeline/tests/testutil"
)

func TestWithoutTracePassesOriginalChannels(t *testing.T) {
	t.Setenv("ORDER_PIPELINE_TRACE", "")
	in, out := make(chan any), make(chan any)
	called := false
	stagetrace.Run(func(received <-chan any, sent chan<- any) {
		called = true
		if received != in || sent != out {
			t.Error("трассировщик заменил каналы шага")
		}
	}, in, out)
	if !called {
		t.Fatal("шаг не был вызван")
	}
}

func TestConcurrentCallsProduceCompleteTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	t.Setenv("ORDER_PIPELINE_TRACE", path)
	ready := make(chan struct{}, 3)
	release := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < 3; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			stagetrace.Run(func(_ <-chan any, _ chan<- any) {
				ready <- struct{}{}
				<-release
			}, nil, nil)
		}()
	}
	for i := 0; i < 3; i++ {
		<-ready
	}
	close(release)
	workers.Wait()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	testutil.AssertStageTrace(t, string(data))
}

func TestPanicIsPreservedAndCallIsFinished(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	t.Setenv("ORDER_PIPELINE_TRACE", path)
	func() {
		defer func() {
			if got := recover(); got != "ошибка шага" {
				t.Fatalf("изменилось значение panic: %v", got)
			}
		}()
		stagetrace.Run(func(_ <-chan any, _ chan<- any) {
			panic("ошибка шага")
		}, nil, nil)
	}()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	var start, finish stagetrace.Event
	if err := decoder.Decode(&start); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&finish); err != nil {
		t.Fatal(err)
	}
	if start.Event != "start" || finish.Event != "finish" || start.ID != finish.ID {
		t.Fatal(start, finish)
	}
}
