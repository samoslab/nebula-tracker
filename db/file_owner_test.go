package db

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"nebula-tracker/config"
	"testing"
	"time"
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
	found, resId, isFolder := queryId(tx, nodeId, nil, "test-folder")
	if !bytes.Equal(id1, resId) {
		t.Error(id1)
		t.Error(queryId(tx, nodeId, nil, "test-folder"))
		t.Errorf("Failed.")
	}
	if !isFolder {
		t.Errorf("Failed.")
	}
	found, resId, isFolder = queryId(tx, nodeId, id1, "test-folder2")
	if !bytes.Equal(id2, resId) {
		t.Errorf("Failed.")
	}
	if !isFolder {
		t.Errorf("Failed.")
	}
	found, id2, isFolder = queryIdRecursion(tx, nodeId, "/test-folder/test-folder2")
	if !found || !bytes.Equal(id2, resId) {
		t.Errorf("Failed.")
	}
	id, isFolder := fileOwnerFileExists(tx, nodeId, nil, "test-folder")
	if !bytes.Equal(id, id1) || !isFolder {
		t.Errorf("Failed.")
	}

	id, isFolder = fileOwnerFileExists(tx, nodeId, id1, "test-folder2")
	if !bytes.Equal(id, id2) || !isFolder {
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
	if len(fofs) != 1 || fofs[0].Name != "test-folder2" || len(fofs[0].Id) == 0 {
		t.Errorf("Failed.")
	}
	nodeId2, isFolder := fileOwnerCheckId(tx, fofs[0].Id)
	if len(nodeId2) == 0 || !isFolder {
		t.Errorf("Failed.")
	}
	fofs = fileOwnerListOfPath(tx, nodeId, id1, 10, 1, "SIZE", true)
	if len(fofs) != 1 || fofs[0].Name != "test-folder2" {
		t.Errorf("Failed.")
	}
	count := fileOwnerListOfPathCount(tx, nodeId, nil)
	id3 := saveFileOwner(tx, nodeId, false, "test.txt", nil, uint64(time.Now().Unix()), &sql.NullString{String: hash, Valid: true}, 1287)
	hash2 := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test hash2")))
	fileSave(tx, nodeId, hash2, 432323, fileData, true, 432323*3)
	updateFileOwnerNewVersion(tx, id3, nodeId, uint64(time.Now().Unix()), hash2, 432323)
	if fileOwnerListOfPathCount(tx, nodeId, nil) != count+1 {
		t.Errorf("Failed.")
	}
	duplicate := fileOwnerMkFolders(tx, false, nodeId, id1, []string{"test1", "test-folder2", "test2"})
	if len(duplicate) != 1 {
		t.Errorf("Failed.")
	}
	if v, ok := duplicate["test-folder2"]; !ok || !v {
		t.Errorf("Failed.")
	}
}
