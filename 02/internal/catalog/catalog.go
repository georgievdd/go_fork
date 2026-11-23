package catalog

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"example.com/order-pipeline/pkg/model"
)

type Catalog struct {
	products map[uint64]bool
	codes    map[string]uint64
	pairs    map[model.RiskPosition]bool
}

func Load(productsPath, suppliesPath string) (*Catalog, error) {
	products, codes, err := loadProducts(productsPath)
	if err != nil {
		return nil, err
	}
	pairs, err := loadSupplies(suppliesPath, products)
	if err != nil {
		return nil, err
	}
	return &Catalog{products: products, codes: codes, pairs: pairs}, nil
}

func (c *Catalog) ProductByCode(code string) (uint64, bool) {
	id, ok := c.codes[code]
	return id, ok
}

func (c *Catalog) HasProduct(productID uint64) bool {
	return c.products[productID]
}

func (c *Catalog) HasPair(productID, supplyID uint64) bool {
	return c.pairs[model.RiskPosition{ProductID: productID, SupplyID: supplyID}]
}

func loadProducts(path string) (map[uint64]bool, map[string]uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("%s: заголовок каталога: %w", path, err)
	}
	if !slices.Equal(header, []string{"id", "sku", "codes"}) {
		return nil, nil, fmt.Errorf("%s: неверный заголовок каталога", path)
	}
	products := make(map[uint64]bool)
	codes := make(map[string]uint64)
	addCode := func(code string, id uint64, line int) error {
		code = strings.TrimSpace(code)
		if code == "" || strings.ContainsAny(code, ":;") || strings.ContainsFunc(code, unicode.IsSpace) {
			return fmt.Errorf("%s: строка %d: неверный код товара %q", path, line, code)
		}
		if other, ok := codes[code]; ok && other != id {
			return fmt.Errorf("%s: строка %d: код %q уже относится к товару %d", path, line, code, other)
		}
		codes[code] = id
		return nil
	}
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("%s: чтение каталога: %w", path, err)
		}
		line, _ := reader.FieldPos(0)
		id, err := strconv.ParseUint(row[0], 10, 64)
		if err != nil || id == 0 {
			return nil, nil, fmt.Errorf("%s: строка %d: неверный id товара", path, line)
		}
		if products[id] {
			return nil, nil, fmt.Errorf("%s: строка %d: повторный товар %d", path, line, id)
		}
		if err := addCode(row[1], id, line); err != nil {
			return nil, nil, err
		}
		rawCodes := strings.TrimSpace(row[2])
		if rawCodes != "" {
			for _, code := range strings.Split(rawCodes, ";") {
				if err := addCode(code, id, line); err != nil {
					return nil, nil, err
				}
			}
		}
		products[id] = true
	}
	return products, codes, nil
}

func loadSupplies(path string, products map[uint64]bool) (map[model.RiskPosition]bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("%s: заголовок поставок: %w", path, err)
	}
	if !slices.Equal(header, []string{"supply_id", "supplier_id", "product_id"}) {
		return nil, fmt.Errorf("%s: неверный заголовок поставок", path)
	}
	pairs := make(map[model.RiskPosition]bool)
	suppliers := make(map[uint64]uint64)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s: чтение поставок: %w", path, err)
		}
		line, _ := reader.FieldPos(0)
		var values [3]uint64
		for i, raw := range row {
			value, err := strconv.ParseUint(raw, 10, 64)
			if err != nil || value == 0 {
				return nil, fmt.Errorf("%s: строка %d: неверный %s", path, line, header[i])
			}
			values[i] = value
		}
		supply, supplier, product := values[0], values[1], values[2]
		if !products[product] {
			return nil, fmt.Errorf("%s: строка %d: неизвестный товар %d", path, line, product)
		}
		if other, ok := suppliers[supply]; ok && other != supplier {
			return nil, fmt.Errorf("%s: строка %d: у поставки %d несколько поставщиков", path, line, supply)
		}
		suppliers[supply] = supplier
		key := model.RiskPosition{ProductID: product, SupplyID: supply}
		if pairs[key] {
			return nil, fmt.Errorf("%s: строка %d: повторная пара товар–поставка", path, line)
		}
		pairs[key] = true
	}
	return pairs, nil
}
