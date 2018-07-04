package db

import (
	"database/sql"
	"encoding/base64"
	"testing"
	"time"

	"nebula-tracker/config"
)

func TestFileVersion(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	nodeId := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test node id")))
	saveClient(tx, nodeId, []byte("test public key"), "test@test.com", "test")
	hash := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test hash")))
	fileData := sha1Sum([]byte("test hash"))
	fileSave(tx, nodeId, hash, nil, "", 123123, fileData, true, 123123*3, false)
	id1 := saveFileOwner(tx, nodeId, false, "test-file", 0, nil, "", uint64(time.Now().Unix()), &sql.NullString{}, 0)
	id := saveFileVersion(tx, id1, nodeId, hash, "")
	if id == nil || len(id) == 0 {
		t.Errorf("Failed.")
	}
}
