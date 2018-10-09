package db

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	task_pb "github.com/samoslab/nebula/tracker/task/pb"
)

func buildTaskTypeClause(cate []string) string {
	if len(cate) == 0 {
		return ""
	}
	for i, str := range cate {
		cate[i] = fmt.Sprintf("TYPE='%s'", str)
	}
	return " and (" + strings.Join(cate, " OR ") + ")"
}
func GetTasksByProviderId(nodeId string, category uint32) (res []*task_pb.Task) {
	cate := make([]string, 0, 4)
	if category&0x1 == 0x1 {
		cate = append(cate, TASK_TYPE_REMOVE)
	}
	if category&0x2 == 0x2 {
		cate = append(cate, TASK_TYPE_PROVE)
	}
	if category&0x4 == 0x4 {
		cate = append(cate, TASK_TYPE_SEND)
	}
	if category&0x8 == 0x8 {
		cate = append(cate, TASK_TYPE_REPLICATE)
	}

	tx, commit := beginTx()
	defer rollback(tx, &commit)
	res = getTasksByProviderId(tx, nodeId, cate)
	checkErr(tx.Commit())
	commit = true
	return
}

func buildTask(rows *sql.Rows) (task *task_pb.Task, err error) {
	task = &task_pb.Task{}
	var typeStr string
	var oppositeId NullStrSlice
	var creation time.Time
	var fileHash, blockHash string
	err = rows.Scan(&task.Id, &creation, &typeStr, &task.FileId, &fileHash, &task.FileSize, &blockHash, &task.BlockSize, &oppositeId, &task.ProofId)
	checkErr(err)
	t, ok := task_pb.TaskType_value[typeStr]
	if !ok {
		return nil, fmt.Errorf("wront task type: %s, id: %x", typeStr, task.Id)
	}
	task.FileHash, err = base64.StdEncoding.DecodeString(fileHash)
	if err != nil {
		return nil, fmt.Errorf("wront task fileHash: %s, id: %x", fileHash, task.Id)
	}
	task.BlockHash, err = base64.StdEncoding.DecodeString(blockHash)
	if err != nil {
		return nil, fmt.Errorf("wront task blockHash: %s, id: %x", blockHash, task.Id)
	}
	task.Type = task_pb.TaskType(t)
	task.Creation = uint64(creation.Unix())
	if oppositeId.Valid {
		task.OppositeId = oppositeId.StrSlice
	}
	return
}

func getTasksByProviderId(tx *sql.Tx, nodeId string, cate []string) []*task_pb.Task {
	rows, err := tx.Query("SELECT ID::bytes,CREATION,TYPE,FILE_ID::bytes,FILE_HASH,FILE_SIZE,BLOCK_HASH,BLOCK_SIZE,OPPOSITE_ID,PROOF_ID::bytes FROM TASK where PROVIDER_ID=$1 and REMOVED=false and FINISHED=false and (EXPIRE_TIME is null or now()<EXPIRE_TIME) "+buildTaskTypeClause(cate)+" order by creation asc limit 320", nodeId)
	checkErr(err)
	defer rows.Close()
	taskList := make([]*task_pb.Task, 0, 16)
	for rows.Next() {
		task, err := buildTask(rows)
		if err != nil {
			fmt.Println(err)
			continue
		}
		taskList = append(taskList, task)
	}
	return taskList
}

func getTask(tx *sql.Tx, taskId []byte, nodeId string) *task_pb.Task {
	rows, err := tx.Query("SELECT ID::bytes,CREATION,TYPE,FILE_ID::bytes,FILE_HASH,FILE_SIZE,BLOCK_HASH,BLOCK_SIZE,OPPOSITE_ID,PROOF_ID::bytes FROM TASK where ID=$2 and PROVIDER_ID=$1 and REMOVED=false and FINISHED=false and (EXPIRE_TIME is null or now()<EXPIRE_TIME)", nodeId, bytesToUuid(taskId))
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		task, err := buildTask(rows)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		return task
	}
	return nil
}

const (
	TASK_TYPE_REMOVE    = "REMOVE"
	TASK_TYPE_PROVE     = "PROVE"
	TASK_TYPE_SEND      = "SEND"
	TASK_TYPE_REPLICATE = "REPLICATE"
)

func GetTask(taskId []byte, nodeId string) (task *task_pb.Task) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	task = getTask(tx, taskId, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func taskFinish(tx *sql.Tx, taskId []byte, nodeId string, finishedTime uint64, success bool, remark string) {
	stmt, err := tx.Prepare("update TASK set FINISHED=true,FINISHED_TIME=$3,SUCCESS=$4,REMARK=$5 where ID=$2 and PROVIDER_ID=$1 and FINISHED=false")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(nodeId, bytesToUuid(taskId), time.Unix(int64(finishedTime), 0), success, remark)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func TaskFinish(taskId []byte, nodeId string, finishedTime uint64, success bool, remark string, fileId []byte, fileHash []byte, blockHash []byte, blockSize uint64, storeNodeId string, isRemove bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	taskFinish(tx, taskId, nodeId, finishedTime, success, remark)
	if success {
		blockHashStr := base64.StdEncoding.EncodeToString(blockHash)
		fileUpdateBlockNodeId(tx, fileId, base64.StdEncoding.EncodeToString(fileHash), blockHashStr, blockSize, storeNodeId, isRemove)
		if isRemove {
			removeBlock(tx, fileId, blockHashStr, nodeId, time.Unix(int64(finishedTime), 0))
		} else {
			if !restoreBlock(tx, fileId, blockHashStr, storeNodeId) {
				saveBlock(tx, fileId, time.Unix(int64(finishedTime), 0), blockHashStr, blockSize, storeNodeId)
			}
		}
	}
	checkErr(tx.Commit())
	commit = true
	return
}

func taskUpdateProofId(tx *sql.Tx, taskId []byte, proofId []byte) bool {
	stmt, err := tx.Prepare("update TASK set PROOF_ID=$2 where ID=$1 and type='PROVE' and PROOF_ID is null and REMOVED=false and FINISHED=false")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(bytesToUuid(taskId), bytesToUuid(proofId))
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	return cnt == 1
}

func taskRemove(tx *sql.Tx, taskId []byte, nodeId string, typeStr string) bool {
	stmt, err := tx.Prepare("update TASK set REMOVED=true where ID=$1 and PROVIDER_ID=$2 and type=$3 and REMOVED=false and FINISHED=false")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(bytesToUuid(taskId), nodeId, typeStr)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	return cnt == 1
}

func TaskRemove(taskId []byte, nodeId string, typeStr string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	taskRemove(tx, taskId, nodeId, typeStr)
	checkErr(tx.Commit())
	commit = true
	return
}

// func TaskTest() {
// 	tx, commit := beginTx()
// 	defer rollback(tx, &commit)
// 	a := taskTest1a(tx)
// 	taskTest1b(tx, a)
// 	b := taskTest2a(tx)
// 	taskTest2b(tx, b)
// 	checkErr(tx.Commit())
// 	commit = true
// 	return
// }

// func taskTest1a(tx *sql.Tx) []byte {
// 	rows, err := tx.Query("SELECT ID FROM TASK limit 1")
// 	checkErr(err)
// 	defer rows.Close()
// 	for rows.Next() {
// 		var id []byte
// 		err = rows.Scan(&id)
// 		checkErr(err)
// 		fmt.Printf("taskTest1: %d\n", len(id))
// 		return id
// 	}
// 	return nil
// }

// func taskTest2a(tx *sql.Tx) []byte {
// 	rows, err := tx.Query("SELECT ID::bytes FROM TASK limit 1")
// 	checkErr(err)
// 	defer rows.Close()
// 	for rows.Next() {
// 		var id []byte
// 		err = rows.Scan(&id)
// 		checkErr(err)
// 		fmt.Printf("taskTest2: %d\n", len(id))
// 		return id
// 	}
// 	return nil
// }

// func taskTest1b(tx *sql.Tx, id []byte) {
// 	rows, err := tx.Query("SELECT ID FROM TASK where id=$1", id)
// 	checkErr(err)
// 	defer rows.Close()
// 	for rows.Next() {
// 		var id []byte
// 		err = rows.Scan(&id)
// 		checkErr(err)
// 		fmt.Printf("taskTest1: found\n")
// 	}
// }

// func taskTest2b(tx *sql.Tx, id []byte) {
// 	rows, err := tx.Query("SELECT ID::bytes FROM TASK where id=$1", bytesToUuid(id))
// 	checkErr(err)
// 	defer rows.Close()
// 	for rows.Next() {
// 		var id []byte
// 		err = rows.Scan(&id)
// 		checkErr(err)
// 		fmt.Printf("taskTest2: found\n")
// 	}
// }
