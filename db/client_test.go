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
	if doClientExistsNodeId(tx, nodeId) {
		t.Errorf("Failed.")
	}
	if doClientExistsContactEmail(tx, email) {
		t.Errorf("Failed.")
	}
	doClientSave(tx, nodeId, []byte("test-public-key"), email, "random")
	if !doClientExistsNodeId(tx, nodeId) {
		t.Errorf("Failed.")
	}
	if !doClientExistsContactEmail(tx, email) {
		t.Errorf("Failed.")
	}
}

func sha1Sum(content []byte) []byte {
	h := sha1.New()
	h.Write(content)
	return h.Sum(nil)
}
