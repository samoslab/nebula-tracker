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

func ProviderRegister(nodeId string, pubKeyBytes []byte, pubKey *rsa.PublicKey, contactEmail string,
	encryptKey []byte, walletAddress string, storageVolume []uint64, upBandwidth uint64,
	downBandwidth uint64, testUpBandwidth uint64, testDownBandwidth uint64, availability float64,
	port uint32, host string, dynamicDomain string, randomCode string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveProvider(tx, nodeId, pubKeyBytes, contactEmail, encryptKey, walletAddress, storageVolume, upBandwidth,
		downBandwidth, testUpBandwidth, testDownBandwidth, availability,
		port, host, dynamicDomain, randomCode)
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
	res := getProviderPubKeyBytes(tx, nodeId)
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

func saveProvider(tx *sql.Tx, nodeId string, pubKeyBytes []byte, contactEmail string, encryptKey []byte,
	walletAddress string, storageVolume []uint64, upBandwidth uint64,
	downBandwidth uint64, testUpBandwidth uint64, testDownBandwidth uint64, availability float64,
	port uint32, host string, dynamicDomain string, randomCode string) {
	stmt, err := tx.Prepare("insert into PROVIDER(NODE_ID,PUBLIC_KEY,BILL_EMAIL,EMAIL_VERIFIED,ENCRYPT_KEY,WALLET_ADDRESS,CREATION,LAST_MODIFIED,RANDOM_CODE,SEND_TIME,ACTIVE,REMOVED,UP_BANDWIDTH,DOWN_BANDWIDTH,TEST_UP_BANDWIDTH,TEST_DOWN_BANDWIDTH,AVAILABILITY,PORT,HOST,DYNAMIC_DOMAIN,STORAGE_VOLUME) values ($1, $2, $3, false, $4, $5, now(), now(), $6, now(), true, false,$7,$8,$9,$10,$11,$12,$13,$14," + arrayClause(len(storageVolume), 15) + ")")
	defer stmt.Close()
	checkErr(err)
	args := make([]interface{}, 14, len(storageVolume)+14)
	args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10],
		args[11], args[12], args[13] = nodeId, pubKeyBytes, contactEmail, encryptKey, walletAddress,
		randomCode, upBandwidth, downBandwidth, testUpBandwidth, testDownBandwidth, availability,
		port, host, dynamicDomain
	for _, val := range storageVolume {
		args = append(args, val)
	}
	_, err = stmt.Exec(args...)
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

// var providerCache = cache.New(20*time.Minute, 10*time.Minute)

func ProviderGetPubKey(nodeId []byte) *rsa.PublicKey {
	nodeIdStr := base64.StdEncoding.EncodeToString(nodeId)
	// pubKey, found := providerCache.Get(nodeIdStr)
	// if found {
	// 	b, ok := pubKey.(*rsa.PublicKey)
	// 	if !ok {
	// 		panic(errors.New("Error type get from cache"))
	// 	}
	// 	return b
	// } else {
	pubKeyBytes := getProviderPublicKeyBytes(nodeIdStr)
	pubKey, err := x509.ParsePKCS1PublicKey(pubKeyBytes)
	if err != nil {
		panic(err)
	}
	// providerCache.Set(nodeIdStr, pubKey, cache.DefaultExpiration)
	return pubKey
	// }
}

type ProviderInfo struct {
	NodeId            string
	NodeIdBytes       []byte
	PublicKey         []byte
	BillEmail         string
	EncryptKey        []byte
	WalletAddress     string
	UpBandwidth       uint64
	DownBandwidth     uint64
	TestUpBandwidth   uint64
	TestDownBandwidth uint64
	Availability      float64
	Port              uint32
	Host              string
	DynamicDomain     string
	StorageVolume     []uint64
}

func (self ProviderInfo) Server() string {
	if self.Host != "" {
		return self.Host
	} else {
		return self.DynamicDomain
	}
}

func ProviderFindOne(nodeId string) (p *ProviderInfo) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	p = providerFindOne(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func providerFindOne(tx *sql.Tx, nodeId string) *ProviderInfo {
	rows, err := tx.Query("SELECT NODE_ID,PUBLIC_KEY,BILL_EMAIL,ENCRYPT_KEY,WALLET_ADDRESS,UP_BANDWIDTH,DOWN_BANDWIDTH,TEST_UP_BANDWIDTH,TEST_DOWN_BANDWIDTH,AVAILABILITY,PORT,HOST,DYNAMIC_DOMAIN,STORAGE_VOLUME from PROVIDER where NODE_ID=$1", nodeId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		return scanProviderInfo(rows)
	}
	return nil
}

func scanProviderInfo(rows *sql.Rows) *ProviderInfo {
	var pi ProviderInfo
	var host, dynamicDomain sql.NullString
	var storageVolume NullUint64Slice
	err := rows.Scan(&pi.NodeId, &pi.PublicKey, &pi.BillEmail, &pi.EncryptKey, &pi.WalletAddress, &pi.UpBandwidth, &pi.DownBandwidth, &pi.TestUpBandwidth, &pi.TestDownBandwidth,
		&pi.Availability, &pi.Port, &host, &dynamicDomain, &storageVolume)
	checkErr(err)
	if host.Valid {
		pi.Host = host.String
	}
	if dynamicDomain.Valid {
		pi.DynamicDomain = dynamicDomain.String
	}
	if storageVolume.Valid {
		pi.StorageVolume = storageVolume.Uint64Slice
	}
	pi.NodeIdBytes, err = base64.StdEncoding.DecodeString(pi.NodeId)
	if err != nil {
		panic(err)
	}
	return &pi
}

func ProviderFindAll() (slice []ProviderInfo) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	slice = providerFindAll(tx)
	checkErr(tx.Commit())
	commit = true
	return
}

func providerFindAll(tx *sql.Tx) []ProviderInfo {
	rows, err := tx.Query("SELECT NODE_ID,PUBLIC_KEY,BILL_EMAIL,ENCRYPT_KEY,WALLET_ADDRESS,UP_BANDWIDTH,DOWN_BANDWIDTH,TEST_UP_BANDWIDTH,TEST_DOWN_BANDWIDTH,AVAILABILITY,PORT,HOST,DYNAMIC_DOMAIN,STORAGE_VOLUME from PROVIDER where REMOVED=false and EMAIL_VERIFIED=true and ACTIVE=true")
	checkErr(err)
	defer rows.Close()
	res := make([]ProviderInfo, 0, 16)
	for rows.Next() {
		res = append(res, *scanProviderInfo(rows))
	}
	return res
}
