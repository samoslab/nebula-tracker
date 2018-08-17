package db

import (
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"nebula-tracker/config"
	"time"

	cache "github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
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
	address, checksum := allocateAddress(tx)
	if address != "" {
		fillRechargeAddress(tx, nodeId, address, checksum)
	}
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

func ClientGetPubKey(nodeIdStr string) *rsa.PublicKey {
	pubKey, found := pubKeyCache.Get(nodeIdStr)
	if found {
		b, ok := pubKey.(*rsa.PublicKey)
		if !ok {
			panic(errors.New("Error type get from cache"))
		}
		return b
	} else {
		pubKeyBytes := getPublicKeyBytes(nodeIdStr)
		if len(pubKeyBytes) == 0 {
			return nil
		}
		pubKey, err := x509.ParsePKCS1PublicKey(pubKeyBytes)
		if err != nil {
			panic(err)
		}
		pubKeyCache.Set(nodeIdStr, pubKey, cache.DefaultExpiration)
		return pubKey
	}
}

func ClientGetPubKeyBytes(nodeId []byte) []byte {
	return getPublicKeyBytes(base64.StdEncoding.EncodeToString(nodeId))
}

func ClientGetPubKeyBytesByNodeId(nodeId string) []byte {
	return getPublicKeyBytes(nodeId)
}

func ClientAllPubKeyBytes() (m map[string][]byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	m = clientAllPubKeyBytes(tx)
	checkErr(tx.Commit())
	commit = true
	return
}

func clientAllPubKeyBytes(tx *sql.Tx) map[string][]byte {
	rows, err := tx.Query("SELECT NODE_ID,PUBLIC_KEY FROM CLIENT where REMOVED=false")
	checkErr(err)
	defer rows.Close()
	m := make(map[string][]byte, 16)
	for rows.Next() {
		var nodeId string
		var pubKey []byte
		err = rows.Scan(&nodeId, &pubKey)
		checkErr(err)
		m[nodeId] = pubKey
	}
	return m
}

func clientDeposit(tx *sql.Tx, address string, amount uint64) {
	stmt, err := tx.Prepare("update CLIENT set BALANCE=BALANCE+$2,LAST_MODIFIED=now() where RECHARGE_ADDRESS=$1")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(address, amount)
	checkErr(err)
	rowsAffected, err := rs.RowsAffected()
	checkErr(err)
	if rowsAffected == 0 {
		panic(fmt.Errorf("not found address: %s", address))
	}
}

func fillRechargeAddress(tx *sql.Tx, nodeId string, address string, checksum string) {
	stmt, err := tx.Prepare("update CLIENT set RECHARGE_ADDRESS=$2,ADDRESS_CHECKSUM=$3,LAST_MODIFIED=now() where NODE_ID=$1")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(nodeId, address, checksum)
	checkErr(err)
	rowsAffected, err := rs.RowsAffected()
	checkErr(err)
	if rowsAffected == 0 {
		panic(fmt.Errorf("not found nodeId: %s", nodeId))
	}
}
func GetRechargeAddress(nodeId string) (address string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	address = getRechargeAddress(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func getRechargeAddress(tx *sql.Tx, nodeId string) (address string) {
	rows, err := tx.Query("SELECT RECHARGE_ADDRESS,ADDRESS_CHECKSUM FROM CLIENT where NODE_ID=$1 and RECHARGE_ADDRESS is not null and ADDRESS_CHECKSUM is not null", nodeId)
	checkErr(err)
	defer rows.Close()
	var checksum string
	addressChecksumToken := config.GetTrackerConfig().AddressChecksumToken
	for rows.Next() {
		err = rows.Scan(&address, &checksum)
		checkErr(err)
		if verifyChecksum(address, checksum, addressChecksumToken) {
			return
		} else {
			err := fmt.Errorf("client recharge address checksum error, nodeId: %s, address: %s, checksum: %s", nodeId, address, checksum)
			log.Error(err)
			panic(err)
		}
	}

	address, checksum = allocateAddress(tx)
	if address != "" {
		fillRechargeAddress(tx, nodeId, address, checksum)
		return address
	} else {
		err := fmt.Errorf("no available address")
		log.Error(err)
		panic(err)
	}
}

func GetBalance(nodeId string) (balance uint64) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	balance = getBalance(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func getBalance(tx *sql.Tx, nodeId string) (balance uint64) {
	rows, err := tx.Query("SELECT BALANCE FROM CLIENT where NODE_ID=$1", nodeId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&balance)
		checkErr(err)
		return
	}
	panic("no record found for nodeId: " + nodeId)
}

func reduceBalanceToPayOrder(tx *sql.Tx, nodeId string, amount uint64) {
	stmt, err := tx.Prepare("update CLIENT set BALANCE=BALANCE-$2,LAST_MODIFIED=now() where NODE_ID=$1 and BALANCE>=$3")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(nodeId, amount, amount)
	checkErr(err)
	rowsAffected, err := rs.RowsAffected()
	checkErr(err)
	if rowsAffected == 0 {
		panic(fmt.Errorf("pay order failed, nodeId: %s, amount: %d", nodeId, amount))
	}
}

func getCurrentPackage(tx *sql.Tx, nodeId string) (inService bool, emailVerified bool, packageId int64, volume uint32, netflow uint32, upNetflow uint32, downNetflow uint32, endTime time.Time) {
	rows, err := tx.Query("SELECT EMAIL_VERIFIED,PACKAGE_ID,VOLUME,NETFLOW,UP_NETFLOW,DOWN_NETFLOW,END_TIME FROM CLIENT where NODE_ID=$1", nodeId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var packageIdNullable, volumeNullable, netflowNullable, upNetflowNullable, downNetflowNullable sql.NullInt64
		var endTimeNullable NullTime
		err = rows.Scan(&emailVerified, &packageIdNullable, &volumeNullable, &netflowNullable, &upNetflowNullable, &downNetflowNullable, &endTimeNullable)
		checkErr(err)
		if endTimeNullable.Valid && endTimeNullable.Time.Unix() > time.Now().Unix() {
			inService = true
			endTime = endTimeNullable.Time
			if packageIdNullable.Valid {
				packageId = packageIdNullable.Int64
			}
			if volumeNullable.Valid {
				volume = uint32(volumeNullable.Int64)
			}
			if netflowNullable.Valid {
				netflow = uint32(netflowNullable.Int64)
			}
			if upNetflowNullable.Valid {
				upNetflow = uint32(upNetflowNullable.Int64)
			}
			if downNetflowNullable.Valid {
				downNetflow = uint32(downNetflowNullable.Int64)
			}
		}
		return
	}
	return
}

func GetCurrentPackage(nodeId string) (inService bool, emailVerified bool, packageId int64, volume uint32, netflow uint32, upNetflow uint32, downNetflow uint32, endTime time.Time) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	inService, emailVerified, packageId, volume, netflow, upNetflow, downNetflow, endTime = getCurrentPackage(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func updateCurrentPackage(tx *sql.Tx, nodeId string, packageId int64, volume uint32, netflow uint32, upNetflow uint32, downNetflow uint32, endTime time.Time, inService bool) {
	var sqlStr string
	if inService {
		sqlStr = "update CLIENT set PACKAGE_ID=$2,VOLUME=$3,NETFLOW=NETFLOW+$4,UP_NETFLOW=UP_NETFLOW+$5,DOWN_NETFLOW=DOWN_NETFLOW+$6,END_TIME=$7,LAST_MODIFIED=now() where NODE_ID=$1"
	} else {
		sqlStr = "update CLIENT set PACKAGE_ID=$2,VOLUME=$3,NETFLOW=$4,UP_NETFLOW=$5,DOWN_NETFLOW=$6,END_TIME=$7,LAST_MODIFIED=now() where NODE_ID=$1"
	}
	stmt, err := tx.Prepare(sqlStr)
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(nodeId, packageId, volume, netflow, upNetflow, downNetflow, endTime)
	checkErr(err)
	rowsAffected, err := rs.RowsAffected()
	checkErr(err)
	if rowsAffected == 0 {
		panic(fmt.Errorf("update current package failed, nodeId: %s, packageId: %d, volume: %d", nodeId, packageId, volume))
	}
}

func UsageAmount(nodeId string) (inService bool, emailVerified bool, packageId int64, volume uint32, netflow uint32, upNetflow uint32,
	downNetflow uint32, usageVolume uint32, usageNetflow uint32, usageUpNetflow uint32, usageDownNetflow uint32, endTime time.Time) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	inService, emailVerified, packageId, volume, netflow, upNetflow, downNetflow, endTime = getCurrentPackage(tx, nodeId)
	volume, netflow, upNetflow, downNetflow = volume*1024, netflow*1024, upNetflow*1024, downNetflow*1024
	usageVolume, usageNetflow, usageUpNetflow, usageDownNetflow, _ = getClientUsageAmount(tx, nodeId)
	serviceSeq := getServiceSeq(tx, nodeId, time.Now())
	ud, uu := getClientNetflow(tx, nodeId, serviceSeq)
	usageUpNetflow, usageDownNetflow = uint32(uu/1048576), uint32(ud/1048576)
	usageNetflow = usageUpNetflow + usageDownNetflow
	checkErr(tx.Commit())
	commit = true
	return
}

func usageAmount(tx *sql.Tx, nodeId string) (inService bool, emailVerified bool, packageId int64, volume uint32, netflow uint32, upNetflow uint32,
	downNetflow uint32, usageVolume uint32, usageNetflow uint32, usageUpNetflow uint32, usageDownNetflow uint32, endTime time.Time) {
	rows, err := tx.Query("SELECT c.EMAIL_VERIFIED,c.PACKAGE_ID,c.VOLUME,c.NETFLOW,c.UP_NETFLOW,c.DOWN_NETFLOW,c.END_TIME,a.VOLUME,a.NETFLOW,a.UP_NETFLOW,a.DOWN_NETFLOW FROM CLIENT c LEFT OUTER JOIN CLIENT_USAGE_AMOUNT a on c.NODE_ID=a.NODE_ID where c.NODE_ID=$1 and c.END_TIME is not null and now()<c.END_TIME", nodeId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		inService = true
		var usageVolumeNullable, usageNetflowNullable, usageUpNetflowNullable, usageDownNetflowNullable sql.NullInt64
		err = rows.Scan(&emailVerified, &packageId, &volume, &netflow, &upNetflow, &downNetflow, &endTime, &usageVolumeNullable, &usageNetflowNullable, &usageUpNetflowNullable, &usageDownNetflowNullable)
		if usageVolumeNullable.Valid {
			usageVolume = uint32(usageVolumeNullable.Int64)
		}
		if usageNetflowNullable.Valid {
			usageNetflow = uint32(usageNetflowNullable.Int64)
		}
		if usageUpNetflowNullable.Valid {
			usageUpNetflow = uint32(usageUpNetflowNullable.Int64)
		}
		if usageDownNetflowNullable.Valid {
			usageDownNetflow = uint32(usageDownNetflowNullable.Int64)
		}
		checkErr(err)
		return
	}
	return
}

func resetClientUsageAmountNetflow(tx *sql.Tx, nodeId string) {
	stmt, err := tx.Prepare("update CLIENT_USAGE_AMOUNT set NETFLOW=0,UP_NETFLOW=0,DOWN_NETFLOW=0,LAST_MODIFIED=now() where NODE_ID=$1")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(nodeId)
	checkErr(err)
}
