package db

import "database/sql"

func saveFileVersion(tx *sql.Tx, ownerId []byte, nodeId string, hash string) []byte {
	var lastInsertId []byte
	err := tx.QueryRow("insert into FILE_VERSION(CREATION,OWNER_ID,NODE_ID,HASH) values (now(),$1,$2,$3) RETURNING ID", ownerId, nodeId, hash).Scan(&lastInsertId)
	checkErr(err)
	return lastInsertId
}
