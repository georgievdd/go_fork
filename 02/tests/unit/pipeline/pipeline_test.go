package pipeline_test

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"example.com/order-pipeline/internal/pipeline"
)

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value, ok := <-ch:
		if !ok {
			t.Fatal("канал закрыт раньше ожидаемого значения")
		}
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("не получено значение: возможна блокировка или накопление всего входа")
		var zero T
		return zero
	}
}

func completed(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("горутины не завершились")
	}
}

func TestRunPipelineEmptyAndSingle(t *testing.T) {
	pipeline.RunPipeline()
	called := false
	pipeline.RunPipeline(func(in <-chan any, out chan<- any) {
		if _, ok := <-in; ok {
			t.Error("первый вход должен быть закрыт и пуст")
		}
		called = true
	})
	if !called {
		t.Fatal("единственный этап не вызван")
	}
}

func TestRunPipelineStreamsBeforeInputCloses(t *testing.T) {
	firstSeen := make(chan struct{})
	done := make(chan struct{})
	var got []int
	go func() {
		defer close(done)
		pipeline.RunPipeline(
			func(_ <-chan any, out chan<- any) {
				out <- 10
				// Не закрываем вход пока первый результат не дошёл до конца
				<-firstSeen
				out <- 20
			},
			func(in <-chan any, out chan<- any) {
				for value := range in {
					out <- value.(int) + 1
				}
			},
			func(in <-chan any, _ chan<- any) {
				for value := range in {
					got = append(got, value.(int))
					if len(got) == 1 {
						close(firstSeen)
					}
				}
			},
		)
	}()
	completed(t, done)
	if !reflect.DeepEqual(got, []int{11, 21}) {
		t.Fatalf("получено %v", got)
	}
}

func TestRunPipelineWaitsForSinkCleanup(t *testing.T) {
	ready, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	go func() {
		pipeline.RunPipeline(
			func(_ <-chan any, _ chan<- any) {},
			func(in <-chan any, _ chan<- any) {
				for range in {
				}
				close(ready)
				<-release
			},
		)
		close(done)
	}()
	completed(t, ready)
	select {
	case <-done:
		t.Fatal("RunPipeline вернулся до завершения последнего этапа")
	case <-time.After(50 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	completed(t, done)
}

func TestRunPipelineWaitsForStageWorkers(t *testing.T) {
	const count = 100
	done := make(chan struct{})
	got := make(map[int]int)
	go func() {
		defer close(done)
		pipeline.RunPipeline(
			func(_ <-chan any, out chan<- any) {
				for i := 0; i < count; i++ {
					out <- i
				}
			},
			func(in <-chan any, out chan<- any) {
				var workers sync.WaitGroup
				for value := range in {
					workers.Add(1)
					go func(value any) {
						defer workers.Done()
						out <- value
					}(value)
				}
				workers.Wait()
			},
			func(in <-chan any, _ chan<- any) {
				for value := range in {
					got[value.(int)]++
				}
			},
		)
	}()
	completed(t, done)
	if len(got) != count {
		t.Fatalf("получено %d из %d значений", len(got), count)
	}
	for value, calls := range got {
		if calls != 1 {
			t.Fatalf("значение %d получено %d раз", value, calls)
		}
	}
}

func TestRunPipelineBackpressureStopsSource(t *testing.T) {
	release := make(chan struct{})
	capacity, total := make(chan int, 1), make(chan int, 1)
	accepted, done := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	count := 0
	go func() {
		defer close(done)
		pipeline.RunPipeline(
			func(_ <-chan any, out chan<- any) {
				limit := <-total
				for i := 0; i < limit; i++ {
					out <- i
					accepted <- struct{}{}
				}
			},
			func(in <-chan any, _ chan<- any) {
				capacity <- cap(in)
				<-release
				for range in {
					count++
				}
			},
		)
	}()
	buffer := receive(t, capacity)
	total <- buffer + 1
	for i := 0; i < buffer; i++ {
		receive(t, accepted)
	}
	select {
	case <-accepted:
		t.Error("источник продолжил отправку после заполнения канала при остановленном получателе")
	case <-time.After(50 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	completed(t, done)
	if count != buffer+1 {
		t.Fatalf("получено %d из %d значений", count, buffer+1)
	}
}

func TestMergeReadsReadyInputWhileAnotherIsOpen(t *testing.T) {
	slow, fast := make(chan any), make(chan any)
	out := pipeline.Merge(slow, nil, fast)
	go func() { fast <- "fast"; close(fast) }()
	if value := receive(t, out); value != "fast" {
		t.Fatalf("получено %v", value)
	}
	// Один вход уже закончился но второй ещё может прислать значение
	go func() { slow <- "slow"; close(slow) }()
	if value := receive(t, out); value != "slow" {
		t.Fatalf("получено %v", value)
	}
	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("лишнее значение")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Merge не закрыл выход после закрытия всех входов")
	}
}

func TestMergeEmptyNilAndAlreadyClosed(t *testing.T) {
	closed := make(chan any)
	close(closed)
	for _, inputs := range [][]<-chan any{nil, {nil, nil}, {nil, closed}} {
		out := pipeline.Merge(inputs...)
		select {
		case _, ok := <-out:
			if ok {
				t.Fatal("ожидался пустой закрытый выход")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Merge завис на пустом или nil-входе")
		}
	}
}

func TestMergePreservesAllValuesFromAllSenders(t *testing.T) {
	var inputs []<-chan any
	for i := 0; i < 8; i++ {
		input := make(chan any)
		inputs = append(inputs, input)
		go func() {
			defer close(input)
			for j := 0; j < 16; j++ {
				input <- i*16 + j
			}
		}()
	}
	counts := make(map[int]int)
	var next [8]int
	ordered := true
	done := make(chan struct{})
	go func() {
		defer close(done)
		for value := range pipeline.Merge(inputs...) {
			n := value.(int)
			counts[n]++
			if n < 0 || n >= 128 {
				ordered = false
				continue
			}
			producer, offset := n/16, n%16
			if offset != next[producer] {
				ordered = false
			}
			next[producer]++
		}
	}()
	completed(t, done)
	if !ordered {
		t.Error("нарушен порядок значений одного входа")
	}
	if len(counts) != 128 {
		t.Fatalf("получено %d уникальных значений из 128", len(counts))
	}
	for value, count := range counts {
		if count != 1 {
			t.Fatalf("%d получен %d раз", value, count)
		}
	}
}

func TestMergeDrainsBufferedInputsAfterClose(t *testing.T) {
	input := make(chan any, 3)
	for i := 0; i < 3; i++ {
		input <- i
	}
	close(input)
	out := pipeline.Merge(input)
	for i := 0; i < 3; i++ {
		if got := receive(t, out); got != i {
			t.Fatalf("получено %v, ожидалось %d", got, i)
		}
	}
	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("лишнее значение")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("выход Merge не закрылся")
	}
}

func TestRunPipelineWithDifferentStageCounts(t *testing.T) {
	for count := 0; count <= 15; count++ {
		var got []int
		stages := []pipeline.Stage{
			func(_ <-chan any, out chan<- any) {
				for value := 0; value < 10; value++ {
					out <- value
				}
			},
		}
		for i := 0; i < count; i++ {
			stages = append(stages, func(in <-chan any, out chan<- any) {
				for value := range in {
					out <- value.(int) + 1
				}
			})
		}
		stages = append(stages, func(in <-chan any, _ chan<- any) {
			for value := range in {
				got = append(got, value.(int))
			}
		})
		pipeline.RunPipeline(stages...)
		if len(got) != 10 {
			t.Fatal(got)
		}
		for i, value := range got {
			if value != i+count {
				t.Fatalf("неверный результат при %d этапах обработки: %v", count, got)
			}
		}
	}
}
