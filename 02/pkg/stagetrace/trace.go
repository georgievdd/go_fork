// Пакет stagetrace записывает начало и завершение шагов для интеграционного теста
package stagetrace

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
)

type Event struct {
	ID       string `json:"id"`
	Event    string `json:"event"`
	Function string `json:"function"`
}

var nextID atomic.Uint64
var output sync.Mutex

// Run вызывает шаг с исходными каналами и записывает его начало и завершение
// Без переменной окружения шаг выполняется без записи трассы
func Run(stage func(in <-chan any, out chan<- any), in <-chan any, out chan<- any) {
	path := os.Getenv("ORDER_PIPELINE_TRACE")
	if path == "" {
		stage(in, out)
		return
	}
	function := runtime.FuncForPC(reflect.ValueOf(stage).Pointer()).Name()
	event := Event{ID: fmt.Sprintf("%d-%d", os.Getpid(), nextID.Add(1)), Event: "start", Function: function}
	write(path, event)
	defer func() {
		event.Event = "finish"
		write(path, event)
	}()
	stage(in, out)
}

func write(path string, event Event) {
	output.Lock()
	defer output.Unlock()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		panic(fmt.Errorf("не удалось открыть трассу шагов: %w", err))
	}
	encodeErr := json.NewEncoder(file).Encode(event)
	closeErr := file.Close()
	if encodeErr != nil {
		panic(fmt.Errorf("не удалось записать трассу шагов: %w", encodeErr))
	}
	if closeErr != nil {
		panic(fmt.Errorf("не удалось закрыть трассу шагов: %w", closeErr))
	}
}
