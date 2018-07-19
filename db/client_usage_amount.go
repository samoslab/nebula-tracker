package db

import (
	"database/sql"
	"time"
)

func updateClientUsageAmount(tx *sql.Tx, nodeId string, fileVolume uint64) {
	stmt, err := tx.Prepare("insert into CLIENT_USAGE_AMOUNT(NODE_ID,CREATION,LAST_MODIFIED,VOLUME) values ($1,now(),now(),$2) ON CONFLICT (NODE_ID) DO UPDATE SET LAST_MODIFIED=now(),VOLUME=CLIENT_USAGE_AMOUNT.VOLUME + excluded.VOLUME")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(nodeId, fileVolume)
}

func getClientUsageAmount(tx *sql.Tx, nodeId string) (volume uint32, netflow uint32, upNetflow uint32, downNetflow uint32, lastUpdated time.Time) {
	rows, err := tx.Query("select (VOLUME/1048576)::INT,(NETFLOW/1048576)::INT,(UP_NETFLOW/1048576)::INT,(DOWN_NETFLOW/1048576)::INT,LAST_MODIFIED from CLIENT_USAGE_AMOUNT where NODE_ID=$1", nodeId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&volume, &netflow, &upNetflow, &downNetflow, &lastUpdated)
		checkErr(err)
		return
	}
	return
}
