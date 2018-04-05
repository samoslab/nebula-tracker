package db

import (
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"errors"
	"time"

	cache "github.com/patrickmn/go-cache"
)

func ClientRegister(nodeId string, pubKeyBytes []byte, pubKey *rsa.PublicKey, contactEmail string, randomCode string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveClient(tx, nodeId, pubKeyBytes, contactEmail, randomCode)
	checkErr(tx.Commit())
	commit = true
	pubKeyCache.Set(nodeId, pubKey, cache.DefaultExpiration)
}

func ClientExistsNodeId(nodeId string) bool {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	res := existsNodeId(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return res
}

func ClientExistsContactEmail(contactEmail string) bool {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	res := existsContactEmail(tx, contactEmail)
	checkErr(tx.Commit())
	commit = true
	return res
}

func getPublicKeyBytes(nodeId string) []byte {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	res := getPubKeyBytes(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return res
}

func getPubKeyBytes(tx *sql.Tx, nodeId string) []byte {
	rows, err := tx.Query("SELECT PUBLIC_KEY FROM CLIENT where NODE_ID=$1 and REMOVED=false", nodeId)
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

func saveClient(tx *sql.Tx, nodeId string, pubKeyBytes []byte, contactEmail string, randomCode string) {
	stmt, err := tx.Prepare("insert into CLIENT(NODE_ID,PUBLIC_KEY,CONTACT_EMAIL,EMAIL_VERIFIED,CREATION,LAST_MODIFIED,RANDOM_CODE,SEND_TIME,ACTIVE,REMOVED) values ($1, $2, $3, false, now(), now(), $4, now(), false, false)")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(nodeId, pubKeyBytes, contactEmail, randomCode)
	checkErr(err)
}

func existsNodeId(tx *sql.Tx, nodeId string) bool {
	rows, err := tx.Query("SELECT EMAIL_VERIFIED,ACTIVE,REMOVED FROM CLIENT where NODE_ID=$1", nodeId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		return true
	}
	return false
}

func existsContactEmail(tx *sql.Tx, contactEmail string) bool {
	rows, err := tx.Query("SELECT EMAIL_VERIFIED,ACTIVE,REMOVED FROM CLIENT where CONTACT_EMAIL=$1 and REMOVED=false", contactEmail)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		return true
	}
	return false
}

func ClientGetRandomCode(nodeId string) (found bool, email string, emailVerified bool, randomCode string, sendTime time.Time) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	found, email, emailVerified, randomCode, sendTime = getRandomCode(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func getRandomCode(tx *sql.Tx, nodeId string) (found bool, email string, emailVerified bool, randomCode string, sendTime time.Time) {
	rows, err := tx.Query("SELECT EMAIL_VERIFIED,CONTACT_EMAIL,RANDOM_CODE,SEND_TIME FROM CLIENT where NODE_ID=$1 and REMOVED=false", nodeId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var sendTimeNullable NullTime
		var randomCodeNullable sql.NullString
		err = rows.Scan(&emailVerified, &email, &randomCodeNullable, &sendTimeNullable)
		checkErr(err)
		if randomCodeNullable.Valid {
			randomCode = randomCodeNullable.String
		}
		if sendTimeNullable.Valid {
			sendTime = sendTimeNullable.Time
		}
		found = true
	}
	return
}

func ClientUpdateEmailVerified(nodeId string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	updateEmailVerified(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
}

func updateEmailVerified(tx *sql.Tx, nodeId string) {
	stmt, err := tx.Prepare("update CLIENT set EMAIL_VERIFIED=true,LAST_MODIFIED=now(),SEND_TIME=NULL,RANDOM_CODE=NULL where NODE_ID=$1 and REMOVED=false")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(nodeId)
	checkErr(err)
}

func ClientUpdateVerifyCode(nodeId string, randomCode string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	updateVerifyCode(tx, nodeId, randomCode)
	checkErr(tx.Commit())
	commit = true
}

func updateVerifyCode(tx *sql.Tx, nodeId string, randomCode string) {
	stmt, err := tx.Prepare("update CLIENT set LAST_MODIFIED=now(),SEND_TIME=now(),RANDOM_CODE=$2 where NODE_ID=$1 and REMOVED=false and EMAIL_VERIFIED=false")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(nodeId, randomCode)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

var pubKeyCache = cache.New(20*time.Minute, 10*time.Minute)

func ClientGetPubKey(nodeId []byte) *rsa.PublicKey {
	nodeIdStr := base64.StdEncoding.EncodeToString(nodeId)
	pubKey, found := pubKeyCache.Get(nodeIdStr)
	if found {
		b, ok := pubKey.(*rsa.PublicKey)
		if !ok {
			panic(errors.New("Error type get from cache"))
		}
		return b
	} else {
		pubKeyBytes := getPublicKeyBytes(nodeIdStr)
		pubKey, err := x509.ParsePKCS1PublicKey(pubKeyBytes)
		if err != nil {
			panic(err)
		}
		pubKeyCache.Set(nodeIdStr, pubKey, cache.DefaultExpiration)
		return pubKey
	}
}
