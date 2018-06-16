package db

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

	addAvailableAddress(tx, []*PreparedAddress{&PreparedAddress{Address: "addr-1", Checksum: genChecksum("addr-1", conf.AddressChecksumToken)}, &PreparedAddress{Address: "addr-2", Checksum: genChecksum("addr-2", conf.AddressChecksumToken)}})
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

func genChecksum(address string, addressChecksumToken string) string {
	hash := hmac.New(sha256.New, []byte(addressChecksumToken))
	hash.Write([]byte(address))
	return hex.EncodeToString(hash.Sum(nil))
}
