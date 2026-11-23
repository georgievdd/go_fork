package supplier

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"

	"example.com/order-pipeline/pkg/model"
)

// Load — совместимое имя для New: читает CSV и создаёт клиент поставщика.
func Load(path string) (*Client, error) {
	return New(path)
}

func loadStatuses(path string) (map[model.RiskPosition]bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("%s: заголовок статусов поставщика: %w", path, err)
	}
	if !slices.Equal(header, []string{"product_id", "supply_id", "problematic"}) {
		return nil, fmt.Errorf("%s: неверный заголовок статусов поставщика", path)
	}
	values := make(map[model.RiskPosition]bool)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s: чтение статусов поставщика: %w", path, err)
		}
		line, _ := reader.FieldPos(0)
		product, err := strconv.ParseUint(row[0], 10, 64)
		if err != nil || product == 0 {
			return nil, fmt.Errorf("%s: строка %d: неверный product_id", path, line)
		}
		supply, err := strconv.ParseUint(row[1], 10, 64)
		if err != nil || supply == 0 {
			return nil, fmt.Errorf("%s: строка %d: неверный supply_id", path, line)
		}
		key := model.RiskPosition{ProductID: product, SupplyID: supply}
		if _, ok := values[key]; ok {
			return nil, fmt.Errorf("%s: строка %d: повторная пара статуса", path, line)
		}
		if row[2] != "true" && row[2] != "false" {
			return nil, fmt.Errorf("%s: строка %d: неверный флаг problematic", path, line)
		}
		values[key] = row[2] == "true"
	}
	return values, nil
}
