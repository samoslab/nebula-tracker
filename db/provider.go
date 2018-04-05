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

func ProviderRegister(nodeId string, pubKeyBytes []byte, pubKey *rsa.PublicKey, contactEmail string, encryptKey []byte, randomCode string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveProvider(tx, nodeId, pubKeyBytes, contactEmail, encryptKey, randomCode)
	checkErr(tx.Commit())
	commit = true
	pubKeyCache.Set(nodeId, pubKey, cache.DefaultExpiration)
}

func ProviderExistsNodeId(nodeId string) bool {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	res := existsProviderNodeId(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return res
}

func ProviderExistsBillEmail(billEmail string) bool {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	res := existsBillEmail(tx, billEmail)
	checkErr(tx.Commit())
	commit = true
	return res
}

func getProviderPublicKeyBytes(nodeId string) []byte {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	res := getPubKeyBytes(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return res
}

func getProviderPubKeyBytes(tx *sql.Tx, nodeId string) []byte {
	rows, err := tx.Query("SELECT PUBLIC_KEY FROM PROVIDER where NODE_ID=$1 and REMOVED=false", nodeId)
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

func saveProvider(tx *sql.Tx, nodeId string, pubKeyBytes []byte, contactEmail string, encryptKey []byte, randomCode string) {
	stmt, err := tx.Prepare("insert into PROVIDER(NODE_ID,PUBLIC_KEY,BILL_EMAIL,EMAIL_VERIFIED,ENCRYPT_KEY,CREATION,LAST_MODIFIED,RANDOM_CODE,SEND_TIME,ACTIVE,REMOVED) values ($1, $2, $3, false, $4, now(), now(), $5, now(), false, false)")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(nodeId, pubKeyBytes, contactEmail, encryptKey, randomCode)
	checkErr(err)
}

func existsProviderNodeId(tx *sql.Tx, nodeId string) bool {
	rows, err := tx.Query("SELECT EMAIL_VERIFIED,ACTIVE,REMOVED FROM PROVIDER where NODE_ID=$1", nodeId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		return true
	}
	return false
}

func existsBillEmail(tx *sql.Tx, billEmail string) bool {
	rows, err := tx.Query("SELECT EMAIL_VERIFIED,ACTIVE,REMOVED FROM PROVIDER where BILL_EMAIL=$1 and REMOVED=false", billEmail)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		return true
	}
	return false
}

func ProviderGetRandomCode(nodeId string) (found bool, email string, emailVerified bool, randomCode string, sendTime time.Time) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	found, email, emailVerified, randomCode, sendTime = getProviderRandomCode(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func getProviderRandomCode(tx *sql.Tx, nodeId string) (found bool, email string, emailVerified bool, randomCode string, sendTime time.Time) {
	rows, err := tx.Query("SELECT EMAIL_VERIFIED,BILL_EMAIL,RANDOM_CODE,SEND_TIME FROM PROVIDER where NODE_ID=$1 and REMOVED=false", nodeId)
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

func ProviderUpdateEmailVerified(nodeId string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	updateProviderEmailVerified(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
}

func updateProviderEmailVerified(tx *sql.Tx, nodeId string) {
	stmt, err := tx.Prepare("update PROVIDER set EMAIL_VERIFIED=true,LAST_MODIFIED=now(),SEND_TIME=NULL,RANDOM_CODE=NULL where NODE_ID=$1 and REMOVED=false")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(nodeId)
	checkErr(err)
}

func ProviderUpdateVerifyCode(nodeId string, randomCode string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	updateProviderVerifyCode(tx, nodeId, randomCode)
	checkErr(tx.Commit())
	commit = true
}

func updateProviderVerifyCode(tx *sql.Tx, nodeId string, randomCode string) {
	stmt, err := tx.Prepare("update PROVIDER set LAST_MODIFIED=now(),SEND_TIME=now(),RANDOM_CODE=$2 where NODE_ID=$1 and REMOVED=false and EMAIL_VERIFIED=false")
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

var providerCache = cache.New(20*time.Minute, 10*time.Minute)

func ProviderGetPubKey(nodeId []byte) *rsa.PublicKey {
	nodeIdStr := base64.StdEncoding.EncodeToString(nodeId)
	pubKey, found := providerCache.Get(nodeIdStr)
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
		providerCache.Set(nodeIdStr, pubKey, cache.DefaultExpiration)
		return pubKey
	}
}
