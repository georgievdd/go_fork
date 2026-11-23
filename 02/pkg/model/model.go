// Пакет model содержит общие типы для шагов обработки
package model

type Product struct {
	ID  uint64
	SKU string
}

type OrderID uint64

// RiskPosition хранит товар и его поставку
type RiskPosition struct {
	ProductID uint64
	SupplyID  uint64
}

type OrderItem struct {
	LineID    uint64
	ProductID uint64
	SupplyID  uint64
	Quantity  uint64
}

type Order struct {
	ID    OrderID
	Items []OrderItem
}

type OrderCheck struct {
	ID         OrderID
	Risky      bool
	Err        error
	RiskyLines []uint64
}
