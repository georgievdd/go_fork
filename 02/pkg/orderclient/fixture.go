package orderclient

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"

	"example.com/order-pipeline/pkg/model"
)

func loadOrders(path string) (map[model.OrderID]model.Order, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("%s: заголовок заказов: %w", path, err)
	}
	columns := []string{"order_id", "line_id", "product_id", "supply_id", "quantity"}
	if !slices.Equal(header, columns) {
		return nil, fmt.Errorf("%s: неверный заголовок заказов", path)
	}

	orders := make(map[model.OrderID]model.Order)
	seen := make(map[[2]uint64]bool)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s: чтение заказов: %w", path, err)
		}
		line, _ := reader.FieldPos(0)
		var values [5]uint64
		for i, raw := range row {
			value, err := strconv.ParseUint(raw, 10, 64)
			if err != nil || value == 0 {
				return nil, fmt.Errorf("%s: строка %d: неверный %s", path, line, columns[i])
			}
			values[i] = value
		}
		key := [2]uint64{values[0], values[1]}
		if seen[key] {
			return nil, fmt.Errorf("%s: строка %d: повторная строка %d заказа %d", path, line, values[1], values[0])
		}
		seen[key] = true
		id := model.OrderID(values[0])
		order := orders[id]
		order.ID = id
		order.Items = append(order.Items, model.OrderItem{
			LineID: values[1], ProductID: values[2], SupplyID: values[3], Quantity: values[4],
		})
		orders[id] = order
	}
	return orders, nil
}
