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
	fileSave(tx, nodeId, hash, 123123, nil, true, 123123*3)
	incrementRefCount(tx, hash)
	exist, active, removed, done, size, creatorNodeId, lastModified := fileCheckExist(tx, hash)
	if !exist || !active || removed || !done || creatorNodeId != nodeId || time.Now().Unix()-lastModified.Unix() > 15 {
		t.Errorf("Failed.")
	}
	fileChangeRemoved(tx, hash, true)
	exist, active, removed, done, size, creatorNodeId, lastModified = fileCheckExist(tx, hash)
	if !exist || !active || !removed || !done || creatorNodeId != nodeId || time.Now().Unix()-lastModified.Unix() > 15 {
		t.Errorf("Failed.")
	}
	fileChangeCreatorNodeId(tx, hash, "test-new-node-id")
	exist, active, removed, done, size, creatorNodeId, lastModified = fileCheckExist(tx, hash)
	if !exist || !active || removed || !done || creatorNodeId == nodeId || time.Now().Unix()-lastModified.Unix() > 15 {
		t.Errorf("Failed.")
	}
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
	if data != nil && len(data) != 0 {
		t.Errorf("Failed.")
	}
	fileSaveDone(tx, hash, 2, []string{`'a,a;a'`, `b;b;b,c,c,c`}, 123123*2)
	exist, active, data, partitionCount, blocks, size = fileRetrieve(tx, hash)
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
	if !bytes.Equal(fileData, data) {
		t.Errorf("Failed.")
	}
	fileSaveDone(tx, hash, 2, []string{`'a,a;a'`, `b;b;b,c,c,c`}, 123123*2)
	exist, active, data, partitionCount, blocks, size = fileRetrieve(tx, hash)
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
