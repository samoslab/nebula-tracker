package db

import (
	"nebula-tracker/config"
	"testing"
)

func TestAvailableAddress(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	count := countAvailableAddress(tx)
	if count != 0 {
		t.Errorf("Failed.")
	}

	addAvailableAddress(tx, []*PreparedAddress{&PreparedAddress{Address: "addr-1", Checksum: "checksum-1"}, &PreparedAddress{Address: "addr-2", Checksum: "checksum-2"}})
	count = countAvailableAddress(tx)
	if count != 2 {
		t.Errorf("Failed.")
	}
	a, c := allocateAddress(tx)
	if a == "" || c == "" {
		t.Errorf("Failed. a: %s, c: %s", a, c)
	}
	count = countAvailableAddress(tx)
	if count != 1 {
		t.Errorf("Failed.")
	}
}
