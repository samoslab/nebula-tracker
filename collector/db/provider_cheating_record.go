package db

import (
	"database/sql"
	"time"
)

const (
	CHEATING_TYPE_WRONG_TICKET = "WRONG_TICKET"
)

type CheatingRecord struct {
	NodeId     string
	ActionTime time.Time
	Type       string
	Confirm    bool
	Ticket     string
	Remark     string
}

func saveProviderCheatingRecord(tx *sql.Tx, batch ...*CheatingRecord) {
	if len(batch) == 0 {
		return
	}
	stmt, err := tx.Prepare("insert into PROVIDER_CHEATING_RECORD(NODE_ID,ACTION_TIME,CREATION,TYPE,CONFIRM,TICKET,REMARK) values($1,$2,now(),$3,$4,$5,$6)")
	defer stmt.Close()
	checkErr(err)
	for _, cr := range batch {
		_, err = stmt.Exec(cr.NodeId, cr.ActionTime, cr.Type, cr.Confirm, cr.Ticket, cr.Remark)
		checkErr(err)
	}
}
