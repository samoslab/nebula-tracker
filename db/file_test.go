package db

import (
	"encoding/base64"
	"testing"

	"nebula-tracker/config"

	util_bytes "github.com/spolabs/nebula/util/bytes"
)

func Test(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	nodeId := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test node id")))
	hash := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test hash")))
	fileData := sha1Sum([]byte("test hash"))
	fileSave(tx, nodeId, hash, 123123, fileData, true, 123123*3)
	exist, active, data, partitionCount, blocks, size := fileRetrieve(tx, hash)
	if !exist {
		t.Errorf("Failed.")
	}
	if !active {
		t.Errorf("Failed.")
	}
	if size != 123123 {
		t.Errorf("Failed.")
	}
	if partitionCount != 0 || blocks != nil {
		t.Errorf("Failed.")
	}
	if !util_bytes.SameBytes(fileData, data) {
		t.Errorf("Failed.")
	}
	fileSaveDone(tx, hash, 2, []string{"aaa", "b\"bb"}, 123123*2)
	exist, active, data, partitionCount, blocks, size = fileRetrieve(tx, hash)
	if len(blocks) != 2 {
		t.Errorf("Failed. len: %d", len(blocks))
	}
}
