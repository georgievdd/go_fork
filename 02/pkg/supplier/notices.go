package supplier

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

// Notice — уведомление о товаре, который нужно проверить в указанной поставке.
// Оно не содержит ответа о риске: за ним нужно обратиться к IsProblematic.
type Notice struct {
	ProductCode string
	SupplyID    uint64
	Reason      string
}

// ReadNotices читает CSV-уведомления и возвращает готовые записи.
// Соответствие кодов каталогу и пар составу поставок проверяет вызывающий код.
// Чтение файла не считается внешним запросом и не меняет Stats.
func (c *Client) ReadNotices(path string) ([]Notice, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("%s: заголовок уведомлений: %w", path, err)
	}
	if !slices.Equal(header, []string{"product_code", "supply_id", "reason"}) {
		return nil, fmt.Errorf("%s: неверный заголовок уведомлений", path)
	}
	var notices []Notice
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s: чтение уведомлений: %w", path, err)
		}
		line, _ := reader.FieldPos(0)
		code := strings.TrimSpace(row[0])
		if code == "" || strings.ContainsAny(code, ":;") || strings.ContainsFunc(code, unicode.IsSpace) {
			return nil, fmt.Errorf("%s: строка %d: неверный product_code", path, line)
		}
		supply, err := strconv.ParseUint(row[1], 10, 64)
		if err != nil || supply == 0 {
			return nil, fmt.Errorf("%s: строка %d: неверный supply_id", path, line)
		}
		reason := strings.TrimSpace(row[2])
		if reason == "" {
			return nil, fmt.Errorf("%s: строка %d: пустая причина проверки", path, line)
		}
		notices = append(notices, Notice{ProductCode: code, SupplyID: supply, Reason: reason})
	}
	return notices, nil
}
