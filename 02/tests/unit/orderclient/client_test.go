package orderclient_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"example.com/order-pipeline/pkg/model"
	"example.com/order-pipeline/pkg/orderclient"
)

const ordersHeader = "order_id,line_id,product_id,supply_id,quantity\n"

func ordersFile(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "orders with spaces.csv")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestWritesStoreAndResetASeparateSnapshot(t *testing.T) {
	path := ordersFile(t, ordersHeader+"7,2,1,17,3\n")
	c, err := orderclient.New(path)
	if err != nil {
		t.Fatal(err)
	}
	orders := c.Orders()
	orders[7].Items[0].LineID = 99
	delete(orders, 7)
	if err := os.WriteFile(path, []byte(ordersHeader+"7,99,1,17,3\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := c.Orders()[7].Items[0].LineID; got != 2 {
		t.Fatalf("client data changed: line=%d", got)
	}
	lines := []uint64{2}
	if err := c.SetRisk(7, true, lines); err != nil {
		t.Fatal(err)
	}
	lines[0] = 99
	saved := c.MarkedOrders()
	if !saved[7].Risky || saved[7].RiskyLines[0] != 2 {
		t.Fatal(saved)
	}
	saved[7].RiskyLines[0] = 98
	if c.MarkedOrders()[7].RiskyLines[0] != 2 {
		t.Fatal("snapshot aliases storage")
	}
	if err := c.SetRisk(7, true, []uint64{99}); err == nil {
		t.Fatal("unknown line accepted")
	}
	if err := c.SetRisk(7, false, nil); err != nil {
		t.Fatal(err)
	}
	if c.MarkedOrders()[7].Risky || len(c.MarkedOrders()[7].RiskyLines) != 0 {
		t.Fatal("old risk not cleared")
	}
	if stats := c.Stats(); stats.Calls != 3 || stats.Active != 0 {
		t.Fatal(stats)
	}
}

func TestNewGroupsUnsortedOrderLines(t *testing.T) {
	c, err := orderclient.New(ordersFile(t, ordersHeader+"7,2,1,17,3\n8,2,9,18,1\n007,1,1,17,2"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[model.OrderID]model.Order{
		7: {ID: 7, Items: []model.OrderItem{
			{LineID: 2, ProductID: 1, SupplyID: 17, Quantity: 3},
			{LineID: 1, ProductID: 1, SupplyID: 17, Quantity: 2},
		}},
		8: {ID: 8, Items: []model.OrderItem{{LineID: 2, ProductID: 9, SupplyID: 18, Quantity: 1}}},
	}
	if got := c.Orders(); !reflect.DeepEqual(got, want) {
		t.Fatalf("orders=%v, want %v", got, want)
	}
	if c.Stats().Calls != 0 {
		t.Fatal("loading orders counted as a request")
	}
}

func TestNewRejectsInvalidOrders(t *testing.T) {
	cases := map[string]string{
		"empty file":           "",
		"wrong header":         "order_id,line_id,product_id,quantity,supply_id\n",
		"missing column":       ordersHeader + "7,2,1,17\n",
		"extra column":         ordersHeader + "7,2,1,17,3,extra\n",
		"bad quoting":          ordersHeader + "7,2,1,17,\"3\n",
		"zero order":           ordersHeader + "0,2,1,17,3\n",
		"zero line":            ordersHeader + "7,0,1,17,3\n",
		"zero product":         ordersHeader + "7,2,0,17,3\n",
		"zero supply":          ordersHeader + "7,2,1,0,3\n",
		"zero quantity":        ordersHeader + "7,2,1,17,0\n",
		"negative number":      ordersHeader + "7,2,1,17,-3\n",
		"non numeric ID":       ordersHeader + "seven,2,1,17,3\n",
		"overflow":             ordersHeader + "7,2,1,17,18446744073709551616\n",
		"duplicate order line": ordersHeader + "7,2,1,17,3\n007,02,2,18,1\n",
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if c, err := orderclient.New(ordersFile(t, data)); err == nil || c != nil {
				t.Fatalf("client=%v, err=%v", c, err)
			}
		})
	}
	if c, err := orderclient.New(filepath.Join(t.TempDir(), "missing.csv")); err == nil || c != nil {
		t.Fatalf("missing file: client=%v, err=%v", c, err)
	}
}

func TestNewAllowsEmptyOrders(t *testing.T) {
	c, err := orderclient.New(ordersFile(t, ordersHeader))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Orders()) != 0 || len(c.MarkedOrders()) != 0 {
		t.Fatal("expected empty orders and marks")
	}
}
