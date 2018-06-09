package db

import (
	"database/sql"
	"time"
)

type DepositRecord struct {
	Id              uint64
	Creation        time.Time
	Address         string `json:"address"`
	Seq             int64  `json:"seq"`
	TransactionTime int64  `json:"update_at"`
	TransactionId   string `json:"txid"`
	Amount          uint64 `json:"amount"`
	Height          uint64 `json:"height"`
}

func SaveDepositRecord(batch []*DepositRecord) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveDepositRecord(tx, batch)
	for _, dr := range batch {
		clientDeposit(tx, dr.Address, dr.Amount)
	}
	checkErr(tx.Commit())
	commit = true
	return
}

func saveDepositRecord(tx *sql.Tx, batch []*DepositRecord) {
	if len(batch) == 0 {
		return
	}
	stmt, err := tx.Prepare("INSERT INTO DEPOSIT_RECORD (CREATION,ADDRESS,SEQ,TRANSACTION_TIME,TRANSACTION_ID,AMOUNT,HEIGHT) VALUES (now(),$1,$2,$3,$4,$5,$6)")
	defer stmt.Close()
	checkErr(err)
	for _, dr := range batch {
		_, err = stmt.Exec(dr.Address, dr.Seq, dr.TransactionTime, dr.TransactionId, dr.Amount, dr.Height)
		checkErr(err)
	}
}
