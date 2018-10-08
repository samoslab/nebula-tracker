package db

import (
	"database/sql"
	"time"
)

func SaveNaRecord(nodeId string, start time.Time, end time.Time) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveNaRecord(tx, nodeId, start, end)
	checkErr(tx.Commit())
	commit = true
}

func saveNaRecord(tx *sql.Tx, nodeId string, start time.Time, end time.Time) (id []byte) {
	err := tx.QueryRow("insert into NA_RECORD(PROVIDER_ID,CHECK_START,CHECK_END) values ($1,$2,$3) RETURNING ID::bytes", nodeId, start, end).Scan(&id)
	checkErr(err)
	return
}
