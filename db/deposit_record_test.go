package db

import (
	"database/sql"
	"nebula-tracker/config"
	"testing"
)

func TestDepositRecord(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	count := countDepositRecord(tx)
	saveDepositRecord(tx, []*DepositRecord{&DepositRecord{Address: "addr-1", Seq: 10000001, TransactionTime: 1528546008, TransactionId: "txasdfsopse1", Amount: 123000000, Height: 12312}, &DepositRecord{Address: "addr-2", Seq: 10000002, TransactionTime: 1528546012, TransactionId: "txasdfsopse2", Amount: 128000000, Height: 12313}})
	if countDepositRecord(tx) != count+2 {
		t.Errorf("Failed.")
	}
}

func countDepositRecord(tx *sql.Tx) (count int) {
	rows, err := tx.Query("SELECT count(*) FROM DEPOSIT_RECORD")
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&count)
		checkErr(err)
		return
	}
	return 0
}
