package db

import (
	"database/sql"
	"time"
)

func savePrivateAliveRecord(tx *sql.Tx, nodeId string, timestamp uint64, total uint64, maxFileSize uint64, version uint32) {
	stmt, err := tx.Prepare("insert into PRIVATE_ALIVE_RECORD(PROVIDER_ID,TIMESTAMP,TOTAL_FREE_VOLUME,AVAIL_FILE_SIZE,VERSION) values ($1,$2,$3,$4,$5)")
	checkErr(err)
	defer stmt.Close()
	_, err = stmt.Exec(nodeId, time.Unix(int64(timestamp), 0), total, maxFileSize, version)
	checkErr(err)
}

func SavePrivateAliveRecord(nodeId string, timestamp uint64, total uint64, maxFileSize uint64, version uint32) {
	m := map[string]*timeAndVersion{nodeId: &timeAndVersion{Time: time.Unix(int64(timestamp), 0),
		Version: version}}
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	savePrivateAliveRecord(tx, nodeId, timestamp, total, maxFileSize, version)
	if maxFileSize > giga && total > giga {
		providerUpdateLastAvail(tx, m)
	} else {
		providerUpdateLastConn(tx, m)
	}
	checkErr(tx.Commit())
	commit = true
	return
}
