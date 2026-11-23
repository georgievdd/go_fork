// Пакет testutil содержит общие проверки для тестов задания.
package testutil

import (
	"encoding/json"
	"io"
	"strings"
	"testing"

	"example.com/order-pipeline/pkg/stagetrace"
)

func AssertStageTrace(t *testing.T, text string) {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(text))
	started := make(map[string]string)
	finished := make(map[string]bool)
	active := 0
	overlapped := false
	for {
		var event stagetrace.Event
		err := decoder.Decode(&event)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("неверная трасса шагов: %v", err)
		}
		if event.ID == "" || event.Function == "" {
			t.Fatalf("у события шага нет ID или имени функции: %+v", event)
		}
		switch event.Event {
		case "start":
			if _, exists := started[event.ID]; exists {
				t.Fatalf("повторное начало шага %s", event.ID)
			}
			started[event.ID] = event.Function
			active++
			overlapped = overlapped || active > 1
		case "finish":
			if started[event.ID] != event.Function || finished[event.ID] {
				t.Fatalf("завершение шага без начала или повторное завершение: %+v", event)
			}
			finished[event.ID] = true
			active--
		default:
			t.Fatalf("неизвестное событие шага: %+v", event)
		}
	}
	if len(started) < 2 {
		t.Fatalf("ожидалась работа нескольких шагов через RunPipeline а в трассе %d", len(started))
	}
	if active != 0 || len(finished) != len(started) {
		t.Fatalf("не все шаги завершились: начало %d завершение %d", len(started), len(finished))
	}
	if !overlapped {
		t.Fatal("шаги выполнялись последовательно без пересечения времени работы")
	}
}
