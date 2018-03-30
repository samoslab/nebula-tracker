package db

import (
	"database/sql"
)

func ClientSave(nodeId string, pubKey []byte, contactEmail string, randomCode string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	doClientSave(tx, nodeId, pubKey, contactEmail, randomCode)
	checkErr(tx.Commit())
	commit = true
}

func doClientSave(tx *sql.Tx, nodeId string, pubKey []byte, contactEmail string, randomCode string) {
	stmt, err := tx.Prepare("insert into CLIENT(NODE_ID,PUBLIC_KEY,CONTACT_EMAIL,EMAIL_VERIFIED,CREATION,LAST_MODIFIED,RANDOM_CODE,SEND_TIME,ACTIVE,REMOVED) values ($1, $2, $3, false, now(), now(), $4, now(), false, false)")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(nodeId, pubKey, contactEmail, randomCode)
	checkErr(err)
}

func doClientExistsNodeId(tx *sql.Tx, nodeId string) bool {
	rows, err := tx.Query("SELECT EMAIL_VERIFIED,ACTIVE,REMOVED FROM CLIENT where NODE_ID=$1", nodeId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		return true
	}
	return false
}

func doClientExistsContactEmail(tx *sql.Tx, contactEmail string) bool {
	rows, err := tx.Query("SELECT EMAIL_VERIFIED,ACTIVE,REMOVED FROM CLIENT where CONTACT_EMAIL=$1", contactEmail)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		return true
	}
	return false
}
