package db

import (
	"database/sql"
	"time"
)

func savePrivateAliveRecord(tx *sql.Tx, nodeId string, timestamp uint64, total uint64, maxFileSize uint64, version uint32) {
	stmt, err := tx.Prepare("insert into PRIVATE_ALIVE_RECORD(PROVIDER_ID,TIMESTAMP,TOTAL_FREE_VOLUME,AVAIL_FILE_SIZE,VERSION) values ($1,$2,$3,$4,$5)")
	checkErr(err)
	defer stmt.Close()
	_, err = stmt.Exec(nodeId, time.Unix(0, int64(timestamp)), total, maxFileSize, version)
	checkErr(err)
}

func SavePrivateAliveRecord(nodeId string, timestamp uint64, total uint64, maxFileSize uint64, version uint32) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	savePrivateAliveRecord(tx, nodeId, timestamp, total, maxFileSize, version)
	checkErr(tx.Commit())
	commit = true
	return
}
