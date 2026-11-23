package supplier_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"example.com/order-pipeline/pkg/supplier"
)

const noticesHeader = "product_code,supply_id,reason\n"

func noticesFile(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "notices with spaces.csv")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadNoticesReturnsParsedRecords(t *testing.T) {
	c, err := supplier.New(statusesFile(t, statusesHeader+"1,17,true\n"))
	if err != nil {
		t.Fatal(err)
	}
	path := noticesFile(t, noticesHeader+" tea ,017,\" possible, defect \"\nSUP-TEA,17,repeat\ntea,17,repeat")
	notices, err := c.ReadNotices(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []supplier.Notice{
		{ProductCode: "tea", SupplyID: 17, Reason: "possible, defect"},
		{ProductCode: "SUP-TEA", SupplyID: 17, Reason: "repeat"},
		{ProductCode: "tea", SupplyID: 17, Reason: "repeat"},
	}
	if !reflect.DeepEqual(notices, want) {
		t.Fatalf("notices=%v, want %v", notices, want)
	}
	if c.Stats().Calls != 0 {
		t.Fatal("reading notices counted as a request")
	}
}

func TestReadNoticesRejectsInvalidFileWithoutPartialResults(t *testing.T) {
	c, err := supplier.New(statusesFile(t, statusesHeader))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"empty file":       "",
		"wrong header":     "code,supply_id,reason\n",
		"missing column":   noticesHeader + "tea,17\n",
		"extra column":     noticesHeader + "tea,17,defect,extra\n",
		"bad quoting":      noticesHeader + "tea,17,\"defect\n",
		"zero supply":      noticesHeader + "tea,0,defect\n",
		"negative supply":  noticesHeader + "tea,-17,defect\n",
		"overflow":         noticesHeader + "tea,18446744073709551616,defect\n",
		"empty code":       noticesHeader + " ,17,defect\n",
		"code with spaces": noticesHeader + "black tea,17,defect\n",
		"code with colon":  noticesHeader + "tea:black,17,defect\n",
		"code list":        noticesHeader + "tea;coffee,17,defect\n",
		"empty reason":     noticesHeader + "tea,17, \n",
		"bad last record":  noticesHeader + "tea,17,defect\ncoffee,17,\n",
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if notices, err := c.ReadNotices(noticesFile(t, data)); err == nil || notices != nil {
				t.Fatalf("notices=%v, err=%v", notices, err)
			}
		})
	}
	if notices, err := c.ReadNotices(filepath.Join(t.TempDir(), "missing.csv")); err == nil || notices != nil {
		t.Fatalf("missing file: notices=%v, err=%v", notices, err)
	}
}

func TestReadNoticesAllowsEmptyFileWithHeader(t *testing.T) {
	c, err := supplier.New(statusesFile(t, statusesHeader))
	if err != nil {
		t.Fatal(err)
	}
	if notices, err := c.ReadNotices(noticesFile(t, noticesHeader)); err != nil || len(notices) != 0 {
		t.Fatal(notices, err)
	}
}
