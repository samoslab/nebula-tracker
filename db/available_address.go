package db

import (
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/rand"
	"nebula-tracker/config"
	"time"

	log "github.com/sirupsen/logrus"
)

type PreparedAddress struct {
	Address   string `json:"address"`
	Checksum  string `json:"checksum"`
	Creation  time.Time
	Used      bool
	UsageTime time.Time
}

func CountAvailableAddress() (count int) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	count = countAvailableAddress(tx)
	checkErr(tx.Commit())
	commit = true
	return
}

func AddAvailableAddress(batch []*PreparedAddress) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	addAvailableAddress(tx, batch)
	checkErr(tx.Commit())
	commit = true
}

func countAvailableAddress(tx *sql.Tx) (count int) {
	rows, err := tx.Query("SELECT count(*) FROM AVAILABLE_ADDRESS where USED=false")
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&count)
		checkErr(err)
		return
	}
	return 0
}

func addAvailableAddress(tx *sql.Tx, batch []*PreparedAddress) {
	if len(batch) == 0 {
		return
	}
	addressChecksumToken := config.GetApiForTellerConfig().AddressChecksumToken
	stmt, err := tx.Prepare("INSERT INTO AVAILABLE_ADDRESS (ADDRESS,CHECKSUM,CREATION,USED) VALUES ($1,$2,now(),false)")
	defer stmt.Close()
	checkErr(err)
	for _, pa := range batch {
		if !verifyChecksum(pa.Address, pa.Checksum, addressChecksumToken) {
			err := fmt.Errorf("verify checksum failed, address: %s, checksum: %s", pa.Address, pa.Checksum)
			log.Error(err)
			panic(err)
		}
		_, err = stmt.Exec(pa.Address, pa.Checksum)
		checkErr(err)
	}
}

func allocateAddress(tx *sql.Tx) (address string, checksum string) {
	addressChecksumToken := config.GetTrackerConfig().AddressChecksumToken
	for {
		rows, err := tx.Query("SELECT ADDRESS,CHECKSUM FROM AVAILABLE_ADDRESS where USED=false limit 100")
		checkErr(err)
		defer rows.Close()
		addrs := make([][]string, 0, 100)
		for rows.Next() {
			var addr, cs string
			err = rows.Scan(&addr, &cs)
			checkErr(err)
			addrs = append(addrs, []string{addr, cs})
		}
		if len(addrs) == 0 {
			return "", ""
		}
		idx := randomInt(len(addrs))
		address = addrs[idx][0]
		checksum = addrs[idx][1]
		if !verifyChecksum(address, checksum, addressChecksumToken) {
			log.Errorf("verify checksum failed, address: %s, checksum: %s", address, checksum)
			continue
		}
		stmt, err := tx.Prepare("update AVAILABLE_ADDRESS set USED=true,USAGE_TIME=now() where ADDRESS=$1 and USED=false")
		defer stmt.Close()
		checkErr(err)
		rs, err := stmt.Exec(address)
		checkErr(err)
		rowsAffected, err := rs.RowsAffected()
		checkErr(err)
		if rowsAffected == 0 {
			log.Errorf("address is already used, address: %s", address)
			continue
		}
		return
	}
}

var random = rand.New(rand.NewSource(time.Now().Unix()))

func randomInt(limit int) int {
	return random.Intn(limit)
}

func verifyChecksum(address string, checksum string, addressChecksumToken string) bool {
	hash := hmac.New(sha256.New, []byte(addressChecksumToken))
	hash.Write([]byte(address))
	return checksum == hex.EncodeToString(hash.Sum(nil))
}
