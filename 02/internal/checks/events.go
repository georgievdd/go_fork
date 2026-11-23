package checks

import (
	"slices"
	"strconv"
	"strings"

	"example.com/order-pipeline/pkg/model"
)

const pipelineBuffer = 64

func SelectCandidates(orders map[model.OrderID]model.Order, selected map[uint64]bool) []model.Order {
	candidates := make([]model.Order, 0, len(orders))
	for _, order := range orders {
		for _, item := range order.Items {
			if selected[item.ProductID] {
				candidates = append(candidates, order)
				break
			}
		}
	}
	slices.SortFunc(candidates, func(a, b model.Order) int {
		return int(a.ID) - int(b.ID)
	})
	return candidates
}

func pendingLine(check model.OrderCheck) (string, bool) {
	if check.Err != nil {
		return stateLine("error", check.ID, "unknown", "-"), true
	}
	return stateLine("pending", check.ID, riskWord(check), linesWord(check)), false
}

func confirmedLine(check model.OrderCheck) string {
	return stateLine("confirmed", check.ID, riskWord(check), linesWord(check))
}

func errorLine(check model.OrderCheck) string {
	return stateLine("error", check.ID, riskWord(check), linesWord(check))
}

func riskWord(check model.OrderCheck) string {
	if check.Risky {
		return "true"
	}
	return "false"
}

func linesWord(check model.OrderCheck) string {
	if len(check.RiskyLines) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(check.RiskyLines))
	for _, line := range check.RiskyLines {
		parts = append(parts, strconv.FormatUint(line, 10))
	}
	return strings.Join(parts, ",")
}

func stateLine(state string, id model.OrderID, risk, lines string) string {
	return state + " " + strconv.FormatUint(uint64(id), 10) + " " + risk + " " + lines
}

func sortPairs(pairs []model.RiskPosition) {
	slices.SortFunc(pairs, func(a, b model.RiskPosition) int {
		if a.ProductID != b.ProductID {
			return int(a.ProductID - b.ProductID)
		}
		return int(a.SupplyID - b.SupplyID)
	})
}

func sortLines(lines []uint64) {
	slices.Sort(lines)
}
