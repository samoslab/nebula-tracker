package db

import (
	"bytes"
	"encoding/base64"
	"testing"
	"time"

	"nebula-tracker/config"
)

func TestFileTiny(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	nodeId := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test node id")))
	hash := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test hash")))
	fileSave(tx, nodeId, hash, nil, "", 123123, nil, true, 123123*3, false)
	fileId, active, removed, done, _, size, creatorNodeId, lastModified := fileCheckExist(tx, nodeId, hash, 0)
	incrementRefCount(tx, fileId)
	if len(fileId) == 0 || !active || removed || !done || creatorNodeId != nodeId || time.Now().Unix()-lastModified.Unix() > 15 {
		t.Errorf("Failed.")
	}
	fileChangeRemoved(tx, fileId, true)
	fileId, active, removed, done, _, size, creatorNodeId, lastModified = fileCheckExist(tx, nodeId, hash, 0)
	if len(fileId) == 0 || !active || !removed || !done || creatorNodeId != nodeId || time.Now().Unix()-lastModified.Unix() > 15 {
		t.Errorf("Failed.")
	}
	fileChangeCreatorNodeId(tx, fileId, "test-new-node-id")
	fileId, active, removed, done, _, size, creatorNodeId, lastModified = fileCheckExist(tx, nodeId, hash, 0)
	if len(fileId) == 0 || !active || removed || !done || creatorNodeId == nodeId || time.Now().Unix()-lastModified.Unix() > 15 {
		t.Errorf("Failed.")
	}
	fileId, active, data, partitionCount, blocks, size, _, _ := fileRetrieve(tx, nodeId, hash, 0)
	if len(fileId) == 0 {
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
	if data != nil && len(data) != 0 {
		t.Errorf("Failed.")
	}
	hash = base64.StdEncoding.EncodeToString(sha1Sum([]byte("test hash2")))
	fileSave(tx, nodeId, hash, nil, "", 123123, nil, false, 123123*3, false)
	fileSaveDone(tx, nodeId, hash, 2, []string{`'a,a;a'`, `b;b;b,c,c,c`}, 123123*2, "", nil)
	fileId, active, data, partitionCount, blocks, size, _, _ = fileRetrieve(tx, nodeId, hash, 0)
	if len(blocks) != 2 {
		t.Errorf("Failed. len: %d", len(blocks))
	}
	if blocks[0] != `'a,a;a'` {
		t.Errorf("Failed. %s", blocks[0])
	}
	if blocks[1] != `b;b;b,c,c,c` {
		t.Errorf("Failed. %s", blocks[1])
	}
}

func TestFile(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	nodeId := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test node id")))
	hash := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test hash")))
	fileData := sha1Sum([]byte("test hash"))
	fileSave(tx, nodeId, hash, nil, "", 123123, fileData, true, 123123*3, false)
	fileId, active, data, partitionCount, blocks, size, _, _ := fileRetrieve(tx, nodeId, hash, 0)
	if len(fileId) == 0 {
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
	if !bytes.Equal(fileData, data) {
		t.Errorf("Failed.")
	}
	hash = base64.StdEncoding.EncodeToString(sha1Sum([]byte("test hash2")))
	fileSave(tx, nodeId, hash, nil, "", 123123, nil, false, 123123*3, false)
	fileSaveDone(tx, nodeId, hash, 2, []string{`'a,a;a'`, `b;b;b,c,c,c`}, 123123*2, "", nil)
	fileId, active, data, partitionCount, blocks, size, _, _ = fileRetrieve(tx, nodeId, hash, 0)
	if len(blocks) != 2 {
		t.Errorf("Failed. len: %d", len(blocks))
	}
	if blocks[0] != `'a,a;a'` {
		t.Errorf("Failed. %s", blocks[0])
	}
	if blocks[1] != `b;b;b,c,c,c` {
		t.Errorf("Failed. %s", blocks[1])
	}
}
