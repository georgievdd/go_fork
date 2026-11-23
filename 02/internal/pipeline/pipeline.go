// Пакет pipeline помогает соединять шаги через каналы
package pipeline

import (
	"sync"

	"example.com/order-pipeline/pkg/stagetrace"
)

const buffer = 64

// Stage читает вход до закрытия и ждёт своих отправителей
// Выход закрывает RunPipeline
type Stage func(in <-chan any, out chan<- any)

// RunPipeline запускает шаги и ждёт их завершения
// Буфер можно выбрать самим но его размер не должен зависеть от длины входа
// Первый вход закрыт а последний шаг только читает
// Каждый шаг вызывается через готовый трассировщик
func RunPipeline(stages ...Stage) {
	in := make(chan any)
	close(in)
	var workers sync.WaitGroup
	for _, stage := range stages {
		out := make(chan any, buffer)
		workers.Add(1)
		go func(stage Stage, in <-chan any, out chan<- any) {
			defer workers.Done()
			defer close(out)
			stagetrace.Run(stage, in, out)
		}(stage, in, out)
		in = out
	}
	workers.Wait()
}

// Merge собирает значения из всех входов в один канал
// Пропускает nil и закрывает выход после всех входов
func Merge(inputs ...<-chan any) <-chan any {
	out := make(chan any)
	var readers sync.WaitGroup
	for _, input := range inputs {
		if input == nil {
			continue
		}
		readers.Add(1)
		go func(input <-chan any) {
			defer readers.Done()
			for value := range input {
				out <- value
			}
		}(input)
	}
	go func() {
		readers.Wait()
		close(out)
	}()
	return out
}

type teeTask struct {
	value any
	sent  *sync.WaitGroup
}

// Tee отправляет каждое значение в обе ветки
// Порядок в каждой ветке сохраняется а готовая ветка может получить значение первой
// Следующее значение читается после отправки текущего в обе ветки
// После завершения входа оба выхода закрываются а nil считается пустым входом
// Обе ветки нужно читать до закрытия а общие данные только читать или копировать
func Tee(input <-chan any) (left, right <-chan any) {
	first := make(chan any, buffer)
	second := make(chan any, buffer)
	tasks := [2]chan teeTask{make(chan teeTask), make(chan teeTask)}
	var senders sync.WaitGroup
	senders.Add(2)
	for i, branch := range [2]chan any{first, second} {
		go func(tasks <-chan teeTask, branch chan<- any) {
			defer senders.Done()
			defer close(branch)
			for task := range tasks {
				branch <- task.value
				task.sent.Done()
			}
		}(tasks[i], branch)
	}
	go func() {
		if input != nil {
			for value := range input {
				var sent sync.WaitGroup
				sent.Add(2)
				tasks[0] <- teeTask{value: value, sent: &sent}
				tasks[1] <- teeTask{value: value, sent: &sent}
				sent.Wait()
			}
		}
		close(tasks[0])
		close(tasks[1])
		senders.Wait()
	}()
	return first, second
}
