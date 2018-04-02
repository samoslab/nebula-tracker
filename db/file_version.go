package db

import "database/sql"

func saveFileVersion(tx *sql.Tx, ownerId []byte, nodeId string, hash string) {
	stmt, err := tx.Prepare("insert into FILE_VERSION(CREATION,OWNER_ID,NODE_ID,HASH) values (now(),$1,$2,$3)")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(ownerId, nodeId, hash)
	checkErr(err)
}
