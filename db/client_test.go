package db

import (
	"crypto/sha1"
	"encoding/base64"
	"testing"

	"nebula-tracker/config"
)

func TestDoClientSave(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	nodeId := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test node id")))
	// t.Errorf("%s length: %d", nodeId, len(nodeId))
	email := "testemail@testemail.com"
	if existsNodeId(tx, nodeId) {
		t.Errorf("Failed.")
	}
	if existsContactEmail(tx, email) {
		t.Errorf("Failed.")
	}
	saveClient(tx, nodeId, []byte("test-public-key"), email, "random")
	if !existsNodeId(tx, nodeId) {
		t.Errorf("Failed.")
	}
	if !existsContactEmail(tx, email) {
		t.Errorf("Failed.")
	}
	pubKey := getPubKeyBytes(tx, nodeId)
	if pubKey == nil || len(pubKey) == 0 {
		t.Errorf("Failed.")
	}
	found, email2, emailVerified, randomCode, _ := getRandomCode(tx, nodeId)
	if !found || emailVerified || randomCode != "random" || email != email2 {
		t.Errorf("Failed.")
	}
	updateVerifyCode(tx, nodeId, "random2")
	found, _, emailVerified, randomCode, _ = getRandomCode(tx, nodeId)
	if !found || emailVerified || randomCode != "random2" {
		t.Errorf("Failed.")
	}
	updateEmailVerified(tx, nodeId)
	found, _, emailVerified, randomCode, _ = getRandomCode(tx, nodeId)
	if !found || !emailVerified || randomCode != "" {
		t.Errorf("Failed.")
	}
}

func sha1Sum(content []byte) []byte {
	h := sha1.New()
	h.Write(content)
	return h.Sum(nil)
}
