package provider_chooser

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"nebula-tracker/db"
	"strconv"
	"testing"

	util_hash "github.com/samoslab/nebula/util/hash"
)

func TestChoose(t *testing.T) {
	initialized = true
	currentProviderIdx = 5
	pros := mockProviderInfoSlice(10)
	providers = &pros
	res := Choose(4)
	if len(res) != 4 {
		t.Error("failed")
	}
	if res[3].Host != "127.0.0.8" {
		t.Error("failed")
	}
	if currentProviderIdx != 9 {
		t.Errorf("failed: %d", currentProviderIdx)
	}
	res = Choose(5)
	if len(res) != 5 {
		t.Error("failed")
	}
	if res[0].Host != "127.0.0.9" {
		t.Errorf("failed: %s", res[0].Host)
	}
	if res[4].Host != "127.0.0.3" {
		t.Error("failed")
	}
	res = Choose(7)
	if len(res) != 7 {
		t.Error("failed")
	}
	if res[0].Host != "127.0.0.4" {
		t.Error("failed")
	}
	if res[4].Host != "127.0.0.8" {
		t.Error("failed")
	}
	if res[5].Host != "127.0.0.9" {
		t.Error("failed")
	}
	if res[6].Host != "127.0.0.0" {
		t.Error("failed")
	}
}

func mockProviderInfoSlice(count int) []db.ProviderInfo {
	slice := make([]db.ProviderInfo, 0, count)
	for i := 0; i < count; i++ {
		priKey, _ := rsa.GenerateKey(rand.Reader, 256*8)

		pubKey := &priKey.PublicKey
		pubKeyBytes := x509.MarshalPKCS1PublicKey(pubKey)
		nodeId := util_hash.Sha1(pubKeyBytes)
		nodeIdStr := base64.StdEncoding.EncodeToString(nodeId)
		slice = append(slice, db.ProviderInfo{NodeId: nodeIdStr,
			NodeIdBytes:       nodeId,
			PublicKey:         pubKeyBytes,
			BillEmail:         "test@test.com",
			EncryptKey:        []byte("test-encrypt-key"),
			WalletAddress:     "test-wallet-address",
			UpBandwidth:       2000000,
			DownBandwidth:     10000000,
			TestUpBandwidth:   2000000,
			TestDownBandwidth: 10000000,
			Availability:      0.98,
			Port:              6666,
			Host:              "127.0.0." + strconv.FormatInt(int64(i), 10),
			StorageVolume:     []uint64{200000000000}})
	}
	return slice
}
