package db

import "database/sql"

func SaveProviderPubKey(nodeId string, key []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveProviderPubKey(tx, nodeId, key)
	checkErr(tx.Commit())
	commit = true

}
func GetProviderPubKey(nodeId string) (key []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	key = getProviderPubKey(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func saveProviderPubKey(tx *sql.Tx, nodeId string, key []byte) {
	stmt, err := tx.Prepare("insert into PROVIDER_PUB_KEY(NODE_ID,PUBLIC_KEY,CREATION) values ($1, $2, now())")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(nodeId, key)
	checkErr(err)
}

func getProviderPubKey(tx *sql.Tx, nodeId string) []byte {
	rows, err := tx.Query("SELECT PUBLIC_KEY FROM PROVIDER_PUB_KEY where NODE_ID=$1", nodeId)
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
