// Пакет orderclient загружает заказы и сохраняет результаты их проверки.
package orderclient

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"example.com/order-pipeline/pkg/model"
)

const Limit = 8

var ErrLimit = errors.New("превышен лимит записи заказов")

type Stats struct{ Calls, Active, MaxActive, OverLimit int }
type Client struct {
	orders map[model.OrderID]model.Order
	marks  map[model.OrderID]model.OrderCheck
	mu     sync.Mutex
	stats  Stats
}

// New читает и проверяет CSV заказов. Вызывающему коду нужен только путь к файлу.
func New(path string) (*Client, error) {
	orders, err := loadOrders(path)
	if err != nil {
		return nil, err
	}
	return &Client{orders: orders, marks: make(map[model.OrderID]model.OrderCheck)}, nil
}

// Orders возвращает независимую копию заказов для поиска и проверки их позиций.
// Изменения карты и её элементов не затрагивают данные клиента.
func (c *Client) Orders() map[model.OrderID]model.Order {
	orders := make(map[model.OrderID]model.Order, len(c.orders))
	for id, order := range c.orders {
		order.Items = slices.Clone(order.Items)
		orders[id] = order
	}
	return orders
}

func (c *Client) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *Client) SetRisk(id model.OrderID, risky bool, riskyLines []uint64) error {
	c.mu.Lock()
	c.stats.Calls++
	if c.stats.Active >= Limit {
		c.stats.OverLimit++
		c.mu.Unlock()
		return ErrLimit
	}
	c.stats.Active++
	if c.stats.Active > c.stats.MaxActive {
		c.stats.MaxActive = c.stats.Active
	}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.stats.Active--
		c.mu.Unlock()
	}()
	time.Sleep(4 * time.Millisecond)
	order, ok := c.orders[id]
	if !ok {
		return fmt.Errorf("неизвестный заказ %d", id)
	}
	if risky != (len(riskyLines) > 0) {
		return errors.New("флаг не соответствует помеченным строкам")
	}
	allowed := make(map[uint64]bool)
	for _, item := range order.Items {
		allowed[item.LineID] = true
	}
	seen := make(map[uint64]bool)
	for _, line := range riskyLines {
		if !allowed[line] || seen[line] {
			return errors.New("неизвестная или повторная строка заказа")
		}
		seen[line] = true
	}
	copyOfLines := slices.Clone(riskyLines)
	slices.Sort(copyOfLines)
	c.mu.Lock()
	c.marks[id] = model.OrderCheck{ID: id, Risky: risky, RiskyLines: copyOfLines}
	c.mu.Unlock()
	return nil
}

// MarkedOrders возвращает копию сохранённых данных для тестов
func (c *Client) MarkedOrders() map[model.OrderID]model.OrderCheck {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[model.OrderID]model.OrderCheck, len(c.marks))
	for id, check := range c.marks {
		check.RiskyLines = slices.Clone(check.RiskyLines)
		result[id] = check
	}
	return result
}
