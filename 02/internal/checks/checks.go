package checks

import (
	"fmt"
	"io"
	"sync"

	"example.com/order-pipeline/internal/pipeline"
	"example.com/order-pipeline/pkg/model"
	"example.com/order-pipeline/pkg/orderclient"
	"example.com/order-pipeline/pkg/supplier"
)

const workers = 8

type Delivery struct {
	Check     model.OrderCheck
	Published chan struct{}
}

type Outcome struct {
	Line string
	Fail bool
}

type Deps struct {
	Orders   []model.Order
	Pairs    map[model.RiskPosition]bool
	Supplier *supplier.Client
	Client   *orderclient.Client
	Out      io.Writer
}

type runner struct {
	deps    Deps
	failure error
}

func Run(deps Deps) error {
	r := runner{deps: deps}
	pipeline.RunPipeline(r.sourceStage, r.checkStage, r.reportStage)
	return r.failure
}

func (r *runner) sourceStage(_ <-chan any, out chan<- any) {
	for _, order := range r.deps.Orders {
		out <- order
	}
}

func (r *runner) checkStage(in <-chan any, out chan<- any) {
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for value := range in {
				order := value.(model.Order)
				out <- Delivery{Check: r.checkOrder(order), Published: make(chan struct{})}
			}
		}()
	}
	wg.Wait()
}

func (r *runner) reportStage(in <-chan any, _ chan<- any) {
	left, right := pipeline.Tee(in)
	saved := make(chan any, pipelineBuffer)
	var savers sync.WaitGroup
	for i := 0; i < workers; i++ {
		savers.Add(1)
		go func() {
			defer savers.Done()
			for value := range right {
				delivery := value.(Delivery)
				if delivery.Check.Err != nil {
					continue
				}
				<-delivery.Published
				outcome := Outcome{Line: confirmedLine(delivery.Check)}
				if err := r.deps.Client.SetRisk(delivery.Check.ID, delivery.Check.Risky, delivery.Check.RiskyLines); err != nil {
					outcome = Outcome{Line: errorLine(delivery.Check), Fail: true}
				}
				saved <- outcome
			}
		}()
	}
	go func() {
		savers.Wait()
		close(saved)
	}()
	merged := pipeline.Merge(left, saved)
	for value := range merged {
		var (
			line      string
			fail      bool
			published chan struct{}
		)
		switch v := value.(type) {
		case Delivery:
			line, fail = pendingLine(v.Check)
			published = v.Published
		case Outcome:
			line, fail = v.Line, v.Fail
		}
		if _, err := fmt.Fprintln(r.deps.Out, line); err != nil {
			fail = true
			r.rememberFailure(err)
		}
		if published != nil {
			close(published)
		}
		if fail {
			r.rememberFailure(fmt.Errorf("заказ не завершён успешно: %s", line))
		}
	}
}

func (r *runner) rememberFailure(err error) {
	if r.failure == nil {
		r.failure = err
	}
}

func (r *runner) checkOrder(order model.Order) model.OrderCheck {
	var pending []model.RiskPosition
	seen := make(map[model.RiskPosition]bool, len(order.Items))
	for _, item := range order.Items {
		key := model.RiskPosition{ProductID: item.ProductID, SupplyID: item.SupplyID}
		if r.deps.Pairs[key] && !seen[key] {
			seen[key] = true
			pending = append(pending, key)
		}
	}
	sortPairs(pending)
	risky := make(map[model.RiskPosition]bool, len(pending))
	for _, key := range pending {
		value, err := r.deps.Supplier.IsProblematic(key.ProductID, key.SupplyID)
		if err != nil {
			return model.OrderCheck{ID: order.ID, Err: fmt.Errorf("заказ %d: проверка пары %d/%d: %w", order.ID, key.ProductID, key.SupplyID, err)}
		}
		risky[key] = value
	}
	lineSeen := make(map[uint64]bool, len(order.Items))
	var lines []uint64
	for _, item := range order.Items {
		key := model.RiskPosition{ProductID: item.ProductID, SupplyID: item.SupplyID}
		if risky[key] && !lineSeen[item.LineID] {
			lineSeen[item.LineID] = true
			lines = append(lines, item.LineID)
		}
	}
	sortLines(lines)
	return model.OrderCheck{ID: order.ID, Risky: len(lines) > 0, RiskyLines: lines}
}
