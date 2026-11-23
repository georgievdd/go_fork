// Пакет supplier даёт клиент для проверки товаров у поставщика
package supplier

import (
	"errors"
	"sync"
	"time"

	"example.com/order-pipeline/pkg/model"
)

const Limit = 8

var ErrLimit = errors.New("превышен лимит клиента поставщика")

type Stats struct{ Calls, Active, MaxActive, OverLimit int }
type Client struct {
	statuses map[model.RiskPosition]bool
	mu       sync.Mutex
	stats    Stats
}

// New читает и проверяет CSV статусов поставщика.
// После загрузки статусы доступны только через IsProblematic.
func New(path string) (*Client, error) {
	statuses, err := loadStatuses(path)
	if err != nil {
		return nil, err
	}
	return &Client{statuses: statuses}, nil
}

func (c *Client) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *Client) IsProblematic(productID, supplyID uint64) (bool, error) {
	c.mu.Lock()
	c.stats.Calls++
	if c.stats.Active >= Limit {
		c.stats.OverLimit++
		c.mu.Unlock()
		return false, ErrLimit
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
	time.Sleep(3 * time.Millisecond)
	value, ok := c.statuses[model.RiskPosition{ProductID: productID, SupplyID: supplyID}]
	if !ok {
		return false, errors.New("поставщик не знает пару товар–поставка")
	}
	return value, nil
}
