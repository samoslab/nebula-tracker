package db

import (
	"database/sql"
)

func updateClientUsageAmount(tx *sql.Tx, nodeId string, fileVolume uint64) {
	stmt, err := tx.Prepare("insert into CLIENT_USAGE_AMOUNT(NODE_ID,CREATION,LAST_MODIFIED,VOLUME) values ($1,now(),now(),$2) ON CONFLICT (NODE_ID) DO UPDATE SET LAST_MODIFIED=now(),VOLUME=CLIENT_USAGE_AMOUNT.VOLUME + excluded.VOLUME")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(nodeId, fileVolume)
}
