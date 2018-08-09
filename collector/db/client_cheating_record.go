package db

import (
	"database/sql"
)

func saveClientCheatingRecord(tx *sql.Tx, batch ...*CheatingRecord) {
	if len(batch) == 0 {
		return
	}
	stmt, err := tx.Prepare("insert into CLIENT_CHEATING_RECORD(NODE_ID,ACTION_TIME,CREATION,TYPE,CONFIRM,TICKET,REMARK) values($1,$2,now(),$3,$4,$5,$6)")
	defer stmt.Close()
	checkErr(err)
	for _, cr := range batch {
		_, err = stmt.Exec(cr.NodeId, cr.ActionTime, cr.Type, cr.Confirm, cr.Ticket, cr.Remark)
		checkErr(err)
	}
}
