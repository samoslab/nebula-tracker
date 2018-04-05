package db

import (
	"database/sql"
	"encoding/base64"
	"nebula-tracker/config"
	"testing"
	"time"

	util_bytes "github.com/spolabs/nebula/util/bytes"
)

func TestFileOwner(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	nodeId := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test node id")))
	saveClient(tx, nodeId, []byte("test public key"), "test@test.com", "test")
	hash := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test hash")))
	fileData := sha1Sum([]byte("test hash"))
	fileSave(tx, nodeId, hash, 123123, fileData, true, 123123*3)
	id1 := saveFileOwner(tx, nodeId, true, "test-folder", nil, uint64(time.Now().Unix()), &sql.NullString{}, 0)
	id2 := saveFileOwner(tx, nodeId, true, "test-folder2", id1, uint64(time.Now().Unix()), &sql.NullString{}, 0)
	if !util_bytes.SameBytes(id1, queryId(tx, nodeId, nil, "test-folder")) {
		t.Error(id1)
		t.Error(queryId(tx, nodeId, nil, "test-folder"))
		t.Errorf("Failed.")
	}
	if !util_bytes.SameBytes(id2, queryId(tx, nodeId, id1, "test-folder2")) {
		t.Errorf("Failed.")
	}
	found, id2 := queryIdRecursion(tx, nodeId, "/test-folder/test-folder2")
	if !found || !util_bytes.SameBytes(id2, queryId(tx, nodeId, id1, "test-folder2")) {
		t.Errorf("Failed.")
	}
	id, isFolder := fileOwnerFileExists(tx, nodeId, nil, "test-folder")
	if !util_bytes.SameBytes(id, id1) || !isFolder {
		t.Errorf("Failed.")
	}

	id, isFolder = fileOwnerFileExists(tx, nodeId, id1, "test-folder2")
	if !util_bytes.SameBytes(id, id2) || !isFolder {
		t.Errorf("Failed.")
	}
	if fileOwnerListOfPathCount(tx, nodeId, nil) != 1 {
		t.Errorf("Failed.")
	}
	if fileOwnerListOfPathCount(tx, nodeId, id1) != 1 {
		t.Errorf("Failed.")
	}
	fofs := fileOwnerListOfPath(tx, nodeId, nil, 10, 1, "NAME", true)
	if len(fofs) != 1 || fofs[0].Name != "test-folder" {
		t.Errorf("Failed.")
	}
	fofs = fileOwnerListOfPath(tx, nodeId, id1, 10, 1, "MOD_TIME", true)
	if len(fofs) != 1 || fofs[0].Name != "test-folder2" {
		t.Errorf("Failed.")
	}
	fofs = fileOwnerListOfPath(tx, nodeId, id1, 10, 1, "SIZE", true)
	if len(fofs) != 1 || fofs[0].Name != "test-folder2" {
		t.Errorf("Failed.")
	}
	folder := fileOwnerMkFolders(tx, nodeId, id1, []string{"test1", "test-folder2", "test2"}, uint64(time.Now().Unix()))
	if folder != "test-folder2" {
		t.Errorf("Failed.")
	}
}
