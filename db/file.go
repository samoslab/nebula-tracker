package db

import (
	"database/sql"
	"errors"
)

func FileCheckExist(hash string) (exist bool, active bool, removed bool, done bool, size uint64) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	exist, active, removed, done, size = fileCheckExist(tx, hash)
	checkErr(tx.Commit())
	commit = true
	return
}

func fileCheckExist(tx *sql.Tx, hash string) (exist bool, active bool, removed bool, done bool, size uint64) {
	rows, err := tx.Query("SELECT ACTIVE,REMOVED,DONE,SIZE FROM FILE where HASH=$1", hash)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&active, &removed, &done, &size)
		checkErr(err)
		exist = true
		return
	}
	exist = false
	return
}

func incrementRefCount(tx *sql.Tx, hash string) {
	stmt, err := tx.Prepare("update FILE set REF_COUNT=REF_COUNT+1,REMOVED=false,LAST_MODIFIED=now() where HASH=$1")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(hash)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}
