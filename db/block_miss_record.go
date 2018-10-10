package db

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"time"

	task_pb "github.com/samoslab/nebula/tracker/task/pb"
)

func saveBlockMissRecord(tx *sql.Tx, nodeId string, miss []*task_pb.HashAndSize, ts time.Time) {
	stmt, err := tx.Prepare("insert into BLOCK_MISS_RECORD(HASH,SIZE,CREATION,PROVIDER_ID) values($1,$2,$3,$4)")
	defer stmt.Close()
	checkErr(err)
	for _, m := range miss {
		_, err = stmt.Exec(base64.StdEncoding.EncodeToString(m.Hash), m.Size, ts, nodeId)
		checkErr(err)
	}
}

func BlockMissProcess(nodeId string, miss []*task_pb.HashAndSize, timestamp uint64) {
	ts := time.Unix(int64(timestamp), 0)
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveBlockMissRecord(tx, nodeId, miss, ts)
	for _, m := range miss {
		blockHash := base64.StdEncoding.EncodeToString(m.Hash)
		fileId := queryFileId(tx, nodeId, blockHash, m.Size)
		if len(fileId) == 0 {
			fmt.Printf("can't get file id, nodeId: %s, hash: %s, size: %d\n", nodeId, blockHash, m.Size)
			continue
		}
		fileUpdateBlockNodeId(tx, fileId, blockHash, m.Size, nodeId, true)
		removeBlock(tx, fileId, blockHash, nodeId, ts)
		addReplicateTaskForMiss(tx, nodeId, fileId, blockHash, m.Size)
	}

	checkErr(tx.Commit())
	commit = true
	return
}
