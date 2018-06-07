package db

import "database/sql"

func SaveClientPubKey(nodeId string, key []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveClientPubKey(tx, nodeId, key)
	checkErr(tx.Commit())
	commit = true

}
func GetClientPubKey(nodeId string) (key []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	key = getClientPubKey(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func saveClientPubKey(tx *sql.Tx, nodeId string, key []byte) {
	stmt, err := tx.Prepare("insert into CLIENT_PUB_KEY(NODE_ID,PUBLIC_KEY,CREATION) values ($1, $2, now())")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(nodeId, key)
	checkErr(err)
}

func getClientPubKey(tx *sql.Tx, nodeId string) []byte {
	rows, err := tx.Query("SELECT PUBLIC_KEY FROM CLIENT_PUB_KEY where NODE_ID=$1", nodeId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var pubKey []byte
		err = rows.Scan(&pubKey)
		checkErr(err)
		return pubKey
	}
	return nil
}
