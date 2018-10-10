package db

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	pb "github.com/samoslab/nebula/tracker/metadata/pb"
	task_pb "github.com/samoslab/nebula/tracker/task/pb"
)

const sql_save_block = "insert into BLOCK(HASH,SIZE,FILE_ID,CREATION,REMOVED,PROVIDER_ID) values($1,$2,$3,$4,false,$5)"

func saveBlocks(tx *sql.Tx, fileId []byte, creation time.Time, partitions []*pb.StorePartition) {
	fileUuid := bytesToUuid(fileId)
	stmt, err := tx.Prepare(sql_save_block)
	defer stmt.Close()
	checkErr(err)
	for _, sp := range partitions {
		for _, block := range sp.Block {
			for _, pid := range block.StoreNodeId {
				_, err = stmt.Exec(base64.StdEncoding.EncodeToString(block.Hash), block.Size, fileUuid, creation, base64.StdEncoding.EncodeToString(pid))
				checkErr(err)
			}
		}
	}
}

func restoreBlock(tx *sql.Tx, fileId []byte, blockHash string, nodeId string) bool {
	stmt, err := tx.Prepare("update BLOCK set REMOVED=false,REMOVE_TIME=NULL where FILE_ID=$1 and HASH=$2 and PROVIDER_ID=$3 ")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(bytesToUuid(fileId), blockHash, nodeId)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	return cnt == 1
}

func saveBlock(tx *sql.Tx, fileId []byte, creation time.Time, blockHash string, size uint64, nodeId string) {
	stmt, err := tx.Prepare(sql_save_block)
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(blockHash, size, bytesToUuid(fileId), creation, nodeId)
	checkErr(err)
}

func removeBlock(tx *sql.Tx, fileId []byte, blockHash string, nodeId string, timestamp time.Time) {
	stmt, err := tx.Prepare("update BLOCK set REMOVED=true,REMOVE_TIME=$4 where FILE_ID=$1 and HASH=$2 and PROVIDER_ID=$3 and REMOVED=false")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(bytesToUuid(fileId), blockHash, nodeId, timestamp)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func updateBlockLastProved(tx *sql.Tx, fileId []byte, blockHash string, nodeId string, timestamp time.Time) {
	stmt, err := tx.Prepare("update BLOCK set LAST_PROOVED=$4 where FILE_ID=$1 and HASH=$2 and PROVIDER_ID=$3")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(bytesToUuid(fileId), blockHash, nodeId, timestamp)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func queryFileId(tx *sql.Tx, providerId string, hash string, size uint64) (fileId []byte) {
	rows, err := tx.Query("select FILE_ID::bytes from BLOCK where PROVIDER_ID=$1 and HASH=$2 and SIZE=$3", providerId, hash, size)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&fileId)
		checkErr(err)
		return
	}
	return
}

func blockQuery(tx *sql.Tx, nodeId string, previous uint64) (blocks []*task_pb.HashAndSize, last uint64, hasNext bool) {
	const bathSize = 2000
	rows, err := tx.Query("select HASH,SIZE,CREATION from BLOCK where PROVIDER_ID=$1 and CREATION>$2 and REMOVED=false order by CREATION ASC LIMIT $3", nodeId, time.Unix(0, int64(previous)), bathSize)
	checkErr(err)
	defer rows.Close()
	blocks = make([]*task_pb.HashAndSize, 0, bathSize)
	for rows.Next() {
		var ts time.Time
		var hashStr string
		var size uint64
		err = rows.Scan(&hashStr, &size, &ts)
		checkErr(err)
		hash, err := base64.StdEncoding.DecodeString(hashStr)
		if err != nil {
			fmt.Printf("wrong data, nodeId: %s, hash: %s, size: %d\n", nodeId, hashStr, size)
			continue
		}
		blocks = append(blocks, &task_pb.HashAndSize{Hash: hash, Size: size})
		last = uint64(ts.UnixNano())
	}
	if len(blocks) == bathSize {
		hasNext = true
	}
	return
}

func BlockQuery(nodeId string, previous uint64) (blocks []*task_pb.HashAndSize, last uint64, hasNext bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	blocks, last, hasNext = blockQuery(tx, nodeId, previous)
	checkErr(tx.Commit())
	commit = true
	return
}

// func saveBlock(tx *sql.Tx, fileId []byte, creation time.Time, hash string, size int, pid string) {
// 	stmt, err := tx.Prepare("insert into BLOCK(HASH,SIZE,FILE_ID,CREATION,REMOVED,PROVIDER_ID) values($1,$2,$3,$4,false,$5)")
// 	defer stmt.Close()
// 	checkErr(err)
// 	_, err = stmt.Exec(hash, size, fileId, creation, pid)
// 	checkErr(err)
// }

// type blockObj struct {
// 	hash     string
// 	size     int
// 	fileId   []byte
// 	creation time.Time
// 	pid      string
// }

// func ProcessOldData() {
// 	tx, commit := beginTx()
// 	defer rollback(tx, &commit)
// 	rows, err := tx.Query("select ID,LAST_MODIFIED,BLOCKS from FILE")
// 	checkErr(err)
// 	defer rows.Close()
// 	slice := make([]blockObj, 0, 256)
// 	for rows.Next() {
// 		var id []byte
// 		var creation time.Time
// 		var block NullStrSlice
// 		err = rows.Scan(&id, &creation, &block)
// 		checkErr(err)
// 		if block.Valid {
// 			for _, blk := range block.StrSlice {
// 				arr := strings.Split(blk, BlockSep)
// 				if len(arr) != 5 {
// 					panic("length error")
// 				}
// 				intVal, err := strconv.Atoi(arr[1])
// 				if err != nil {
// 					panic(err)
// 				}
// 				nodeIds := strings.Split(arr[4], BlockNodeIdSep)
// 				if len(nodeIds) == 0 {
// 					panic("no provider node id")
// 				}
// 				for _, pid := range nodeIds {
// 					fmt.Printf("id: %x, creation: %d, hash: %s, size: %d, pid: %s\n", id, creation.Unix(), arr[0], intVal, pid)
// 					slice = append(slice, blockObj{hash: arr[0],
// 						size:     intVal,
// 						fileId:   id,
// 						creation: creation,
// 						pid:      pid})
// 				}
// 			}
// 		}
// 	}
// 	for _, bo := range slice {
// 		saveBlock(tx, bo.fileId, bo.creation, bo.hash, bo.size, bo.pid)
// 	}
// 	checkErr(tx.Commit())
// 	commit = true
// }
