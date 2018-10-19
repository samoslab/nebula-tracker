package db

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"
)

func getProofInfo(tx *sql.Tx, id []byte) (chunkSize uint32, seq []uint32, randomNum [][]byte) {
	rows, err := tx.Query("select m.CHUNK_SIZE,r.CHUNK_SEQ,r.RANDOM_NUM_DATA,r.RANDOM_NUM_LENGTH from PROOF_METADATA m,PROOF_RECORD r where r.ID=$1 and r.FILE_ID=m.FILE_ID and r.BLOCK_HASH=m.HASH", bytesToUuid(id))
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var seqSlice NullUint32Slice
		var randomLength NullUint64Slice
		var randomData []byte
		err = rows.Scan(&chunkSize, &seqSlice, &randomData, &randomLength)
		checkErr(err)
		if seqSlice.Valid {
			seq = seqSlice.Uint32Slice
		}
		if randomLength.Valid {
			randomNum = make([][]byte, len(randomLength.Uint64Slice))
			var start, end int
			for i, bs := range randomLength.Uint64Slice {
				end = start + int(bs)
				randomNum[i] = randomData[start:end]
				start = end
			}
		}
		return
	}
	return 0, nil, nil
}

func GetProofInfo(id []byte) (chunkSize uint32, seq []uint32, randomNum [][]byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	chunkSize, seq, randomNum = getProofInfo(tx, id)
	checkErr(tx.Commit())
	commit = true
	return
}

func saveProofInfo(tx *sql.Tx, providerId string, fileId []byte, blockHash []byte, blockSize uint64, chunkSeq []uint32, randomNum [][]byte) (id []byte) {
	args := make([]interface{}, 5, len(chunkSeq)+len(randomNum)+5)
	args[0], args[1], args[2], args[3], args[4] = providerId, bytesToUuid(fileId), base64.StdEncoding.EncodeToString(blockHash), blockSize, bytes.Join(randomNum, []byte{})
	for _, el := range randomNum {
		args = append(args, len(el))
	}
	for _, el := range chunkSeq {
		args = append(args, el)
	}

	err := tx.QueryRow("insert into PROOF_RECORD(CREATION,PROVIDER_ID,FILE_ID,BLOCK_HASH,BLOCK_SIZE,RANDOM_NUM_DATA,RANDOM_NUM_LENGTH,CHUNK_SEQ) values(now(),$1,$2,$3,$4,$5,"+arrayClause(len(randomNum), 6)+","+arrayClause(len(chunkSeq), len(randomNum)+6)+") RETURNING ID::bytes", args...).Scan(&id)
	checkErr(err)
	return
}

func SaveProofInfo(taskId []byte, providerId string, fileId []byte, blockHash []byte, blockSize uint64, chunkSeq []uint32, randomNum [][]byte) (id []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	id = saveProofInfo(tx, providerId, fileId, blockHash, blockSize, chunkSeq, randomNum)
	if !taskUpdateProofId(tx, taskId, id) {
		panic("update task proof id failed, task id: " + hex.EncodeToString(taskId))
	}
	checkErr(tx.Commit())
	commit = true
	return
}

func proofFinish(tx *sql.Tx, proofId []byte, nodeId string, finished time.Time, pass bool, remark string, result []byte) {
	stmt, err := tx.Prepare("update PROOF_RECORD set FINISHED=true,FINISHED_TIME=$3,PASS=$4,REMARK=$5,PROVE_RESULT=$6 where ID=$2 and PROVIDER_ID=$1 and FINISHED=false")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(nodeId, bytesToUuid(proofId), finished, pass, remark, result)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func ProofFinish(taskId []byte, nodeId string, fileId []byte, blockHash string, finishedTime uint64, pass bool, remark string, proofId []byte, result []byte) {
	finished := time.Unix(int64(finishedTime), 0)
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	proofFinish(tx, proofId, nodeId, finished, pass, remark, result)
	updateBlockLastProved(tx, fileId, blockHash, nodeId, finished)
	taskFinish(tx, taskId, nodeId, finishedTime, pass, remark)
	checkErr(tx.Commit())
	commit = true
	return
}
