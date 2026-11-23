package supplier_test

import (
	"os"
	"path/filepath"
	"testing"

	"example.com/order-pipeline/pkg/supplier"
)

const statusesHeader = "product_id,supply_id,problematic\n"

func statusesFile(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "supplier status.csv")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestStatusSnapshotAndFalseAnswer(t *testing.T) {
	path := statusesFile(t, statusesHeader+"1,17,true\n01,18,false")
	c, err := supplier.New(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(statusesHeader+"1,17,false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := c.IsProblematic(1, 17); err != nil || !got {
		t.Fatal(got, err)
	}
	if got, err := c.IsProblematic(1, 18); err != nil || got {
		t.Fatal(got, err)
	}
	if _, err := c.IsProblematic(2, 17); err == nil {
		t.Fatal("unknown pair accepted")
	}
	stats := c.Stats()
	if stats.Calls != 3 || stats.Active != 0 || stats.OverLimit != 0 {
		t.Fatal(stats)
	}
}

func TestNewRejectsInvalidStatuses(t *testing.T) {
	cases := map[string]string{
		"empty file":      "",
		"wrong header":    "supply_id,product_id,problematic\n",
		"missing column":  statusesHeader + "1,17\n",
		"extra column":    statusesHeader + "1,17,true,extra\n",
		"bad quoting":     statusesHeader + "1,17,\"true\n",
		"zero product":    statusesHeader + "0,17,true\n",
		"zero supply":     statusesHeader + "1,0,true\n",
		"negative number": statusesHeader + "1,-17,true\n",
		"non numeric ID":  statusesHeader + "tea,17,true\n",
		"overflow":        statusesHeader + "18446744073709551616,17,true\n",
		"duplicate pair":  statusesHeader + "1,17,true\n01,017,false\n",
		"upper case flag": statusesHeader + "1,17,TRUE\n",
		"numeric flag":    statusesHeader + "1,17,1\n",
		"empty flag":      statusesHeader + "1,17,\n",
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if c, err := supplier.New(statusesFile(t, data)); err == nil || c != nil {
				t.Fatalf("client=%v, err=%v", c, err)
			}
		})
	}
	if c, err := supplier.New(filepath.Join(t.TempDir(), "missing.csv")); err == nil || c != nil {
		t.Fatalf("missing file: client=%v, err=%v", c, err)
	}
}

func TestNewAllowsEmptyStatuses(t *testing.T) {
	c, err := supplier.New(statusesFile(t, statusesHeader))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.IsProblematic(1, 17); err == nil {
		t.Fatal("unknown pair accepted")
	}
}

func TestLoadCreatesClientFromPath(t *testing.T) {
	c, err := supplier.Load(statusesFile(t, statusesHeader+"1,17,true\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.IsProblematic(1, 17); err != nil || !got {
		t.Fatal(got, err)
	}
}
