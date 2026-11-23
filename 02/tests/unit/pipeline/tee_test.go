package pipeline_test

import (
	"reflect"
	"testing"
	"time"

	"example.com/order-pipeline/internal/pipeline"
)

func TestTeeCopiesEveryValueToBothStagesInOrder(t *testing.T) {
	input := make(chan any, 6)
	values := []any{7, nil, "товар", 7, false, uint64(42)}
	for _, value := range values {
		input <- value
	}
	close(input)
	left, right := pipeline.Tee(input)
	if left == nil || right == nil || left == right {
		t.Fatal("Tee должен возвращать два разных ненулевых канала")
	}

	var got [2][]any
	done := make(chan struct{}, 2)
	for i, branch := range []<-chan any{left, right} {
		go func() {
			defer func() { done <- struct{}{} }()
			pipeline.RunPipeline(
				func(_ <-chan any, out chan<- any) {
					for value := range branch {
						out <- value
					}
				},
				func(in <-chan any, _ chan<- any) {
					for value := range in {
						got[i] = append(got[i], value)
					}
				},
			)
		}()
	}
	completed(t, done)
	completed(t, done)
	for i, branch := range got {
		if !reflect.DeepEqual(branch, values) {
			t.Fatalf("ветка %d получила %v, ожидалось %v", i, branch, values)
		}
	}
}

func TestTeeServesEitherReadyBranchAndAppliesBackpressure(t *testing.T) {
	for _, first := range []string{"left", "right"} {
		t.Run(first, func(t *testing.T) {
			input := make(chan any)
			left, right := pipeline.Tee(input)
			ready, slow := left, right
			if first == "right" {
				ready, slow = right, left
			}
			// Заполняем буфер медленной ветки и отправляем ещё одно значение
			// Теперь источник должен ждать пока медленная ветка начнёт читать
			count := cap(slow) + 2
			accepted := make(chan struct{}, 1)
			go func() {
				defer close(input)
				for value := 1; value <= count; value++ {
					input <- value
					accepted <- struct{}{}
				}
			}()

			// Готовая ветка получает значение даже если другая пока занята
			for value := 1; value < count; value++ {
				if got := receive(t, ready); got != value {
					t.Fatalf("получено %v, ожидалось %d", got, value)
				}
				receive(t, accepted)
			}
			select {
			case <-accepted:
				t.Fatal("Tee принял следующий вход до отправки текущего в обе ветки")
			case <-time.After(50 * time.Millisecond):
			}
			for value := 1; value < count; value++ {
				if got := receive(t, slow); got != value {
					t.Fatalf("получено %v, ожидалось %d", got, value)
				}
			}
			if got := receive(t, ready); got != count {
				t.Fatal(got)
			}
			if got := receive(t, slow); got != count {
				t.Fatal(got)
			}
			receive(t, accepted)
			assertTeeClosed(t, left, right)
		})
	}
}

func TestTeeEmptyAndNilInputClosesBothOutputs(t *testing.T) {
	closed := make(chan any)
	close(closed)
	for _, input := range []<-chan any{nil, closed} {
		left, right := pipeline.Tee(input)
		assertTeeClosed(t, left, right)
	}
}

func assertTeeClosed(t *testing.T, branches ...<-chan any) {
	t.Helper()
	for _, branch := range branches {
		select {
		case _, ok := <-branch:
			if ok {
				t.Fatal("лишнее значение в выходе Tee")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Tee не закрыл выход после завершения входа")
		}
	}
}
