package db

import (
	"database/sql"

	"github.com/shopspring/decimal"
)

func getPackageDiscount(tx *sql.Tx, id int64) map[uint32]*decimal.Decimal {
	rows, err := tx.Query("select QUANTITY,DISCOUNT from PACKAGE_DISCOUNT where PACKAGE_ID=$1 and QUANTITY>0", id)
	checkErr(err)
	defer rows.Close()
	m := make(map[uint32]*decimal.Decimal, 8)
	for rows.Next() {
		var dec decimal.Decimal
		var qua uint32
		err = rows.Scan(&qua, &dec)
		checkErr(err)
		m[qua] = &dec
	}
	return m
}

func GetPackageDiscount(id int64) (m map[uint32]*decimal.Decimal) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	m = getPackageDiscount(tx, id)
	checkErr(tx.Commit())
	commit = true
	return
}
