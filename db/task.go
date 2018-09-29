package db

import (
	"database/sql"
	"errors"
	"fmt"

	task_pb "github.com/samoslab/nebula/tracker/task/pb"
)

func GetTasksByProviderId(nodeId string) (res []*task_pb.Task) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	res = getTasksByProviderId(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func buildTask(rows *sql.Rows) (task *task_pb.Task, err error) {
	task = &task_pb.Task{}
	var typeStr string
	var oppositeId NullStrSlice
	err = rows.Scan(&task.Id, &task.Creation, &typeStr, &task.FileId, &task.FileHash, &task.FileSize, &task.BlockHash, &task.BlockSize, &oppositeId, &task.ProofId)
	checkErr(err)
	t, ok := task_pb.TaskType_value[typeStr]
	if !ok {
		return nil, fmt.Errorf("wront task type: %s, id: %x", typeStr, task.Id)
	}
	task.Type = task_pb.TaskType(t)
	if oppositeId.Valid {
		task.OppositeId = oppositeId.StrSlice
	}
	return
}

func getTasksByProviderId(tx *sql.Tx, nodeId string) []*task_pb.Task {
	rows, err := tx.Query("SELECT ID,CREATION,TYPE,FILE_ID,FILE_HASH,FILE_SIZE,BLOCK_HASH,BLOCK_SIZE,OPPOSITE_ID,PROOF_ID FROM TASK where PROVIDER_ID=$1 and REMOVED=false and FINISHED=false and (EXPIRE_TIME is null or now()<EXPIRE_TIME)", nodeId)
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
	rows, err := tx.Query("SELECT ID,CREATION,TYPE,FILE_ID,FILE_HASH,FILE_SIZE,BLOCK_HASH,BLOCK_SIZE,OPPOSITE_ID,PROOF_ID FROM TASK where ID=$2 and PROVIDER_ID=$1 and REMOVED=false and FINISHED=false and (EXPIRE_TIME is null or now()<EXPIRE_TIME)", nodeId, taskId)
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

func GetTask(taskId []byte, nodeId string) (task *task_pb.Task) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	task = getTask(tx, taskId, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func taskFinish(tx *sql.Tx, taskId []byte, nodeId string, finishedTime uint64, success bool, remark string) {
	stmt, err := tx.Prepare("update TASK set FINISHED=true,FINISHED_TIME=$3,SUCCESS=$4,REMARK=$5 where ID=$2 and PROVIDER_ID=$1 and REMOVED=false and FINISHED=false")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(nodeId, taskId, finishedTime, success, remark)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func TaskFinish(taskId []byte, nodeId string, finishedTime uint64, success bool, remark string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	taskFinish(tx, taskId, nodeId, finishedTime, success, remark)
	checkErr(tx.Commit())
	commit = true
	return
}
