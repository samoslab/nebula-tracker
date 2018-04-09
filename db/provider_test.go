package db

import (
	"encoding/base64"
	"testing"

	"nebula-tracker/config"
)

func TestDoProviderSave(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	nodeId := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test node id")))
	// t.Errorf("%s length: %d", nodeId, len(nodeId))
	email := "testemail@testemail.com"
	if existsProviderNodeId(tx, nodeId) {
		t.Errorf("Failed.")
	}
	if existsBillEmail(tx, email) {
		t.Errorf("Failed.")
	}
	saveProvider(tx, nodeId, []byte("test-public-key"), email, []byte("test-encrypt-key"), "wallet-address", []uint64{10000000000}, 4000000, 20000000, 4000000, 20000000, 0.98, 6666, "127.0.0.1", "", "random")
	if !existsProviderNodeId(tx, nodeId) {
		t.Errorf("Failed.")
	}
	if !existsBillEmail(tx, email) {
		t.Errorf("Failed.")
	}
	pubKey := getProviderPubKeyBytes(tx, nodeId)
	if pubKey == nil || len(pubKey) == 0 {
		t.Errorf("Failed.")
	}
	found, email2, emailVerified, randomCode, _ := getProviderRandomCode(tx, nodeId)
	if !found || emailVerified || randomCode != "random" || email != email2 {
		t.Errorf("Failed.")
	}
	updateProviderVerifyCode(tx, nodeId, "random2")
	found, _, emailVerified, randomCode, _ = getProviderRandomCode(tx, nodeId)
	if !found || emailVerified || randomCode != "random2" {
		t.Errorf("Failed.")
	}
	updateProviderEmailVerified(tx, nodeId)
	found, _, emailVerified, randomCode, _ = getProviderRandomCode(tx, nodeId)
	if !found || !emailVerified || randomCode != "" {
		t.Errorf("Failed.")
	}
	p := providerFindOne(tx, nodeId)
	if len(p.StorageVolume) != 1 {
		t.Errorf("Failed.")
	}
	if len(providerFindAll(tx)) != 0 {
		t.Errorf("Failed.")
	}

}
