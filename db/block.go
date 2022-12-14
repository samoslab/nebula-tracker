package db

import (
	"database/sql"
	"encoding/base64"
	"time"

	pb "github.com/samoslab/nebula/tracker/metadata/pb"
)

func saveBlocks(tx *sql.Tx, fileId []byte, creation time.Time, partitions []*pb.StorePartition) {
	stmt, err := tx.Prepare("insert into BLOCK(HASH,SIZE,FILE_ID,CREATION,REMOVED,PROVIDER_ID) values($1,$2,$3,$4,false,$5)")
	defer stmt.Close()
	checkErr(err)
	for _, sp := range partitions {
		for _, block := range sp.Block {
			for _, pid := range block.StoreNodeId {
				_, err = stmt.Exec(base64.StdEncoding.EncodeToString(block.Hash), block.Size, fileId, creation, base64.StdEncoding.EncodeToString(pid))
				checkErr(err)
			}
		}
	}
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
