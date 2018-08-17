package db

import (
	"database/sql"
)

func getKvStore(tx *sql.Tx, name string) (intVal int64, strVal string, found bool) {
	rows, err := tx.Query("SELECT INT_VAL,STR_VAL FROM KV_STORE where NAME=$1", name)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var intValNullable sql.NullInt64
		var strValNullable sql.NullString
		err = rows.Scan(&intValNullable, &strValNullable)
		checkErr(err)
		var intVal int64
		if intValNullable.Valid {
			intVal = intValNullable.Int64
		}
		var strVal string
		if strValNullable.Valid {
			strVal = strValNullable.String
		}
		return intVal, strVal, true
	}
	return 0, "", false
}

func saveKvStore(tx *sql.Tx, name string, intVal int64, strVal string) {
	stmt, err := tx.Prepare("update KV_STORE set INT_VAL=$2, STR_VAL=$3 where NAME=$1")
	checkErr(err)
	defer stmt.Close()
	res, err := stmt.Exec(name, intVal, strVal)
	checkErr(err)
	rowCnt, err := res.RowsAffected()
	checkErr(err)
	if rowCnt == 1 {
		return
	} else if rowCnt > 1 {
		panic("duplicate record of: " + name)
	}
	stmt, err = tx.Prepare("insert into KV_STORE(NAME,INT_VAL,STR_VAL) values ($1, $2, $3)")
	res, err = stmt.Exec(name, intVal, strVal)
	checkErr(err)
}

func GetKvStore(name string) (intVal int64, strVal string, found bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	intVal, strVal, found = getKvStore(tx, name)
	checkErr(tx.Commit())
	commit = true
	return
}
func SaveKvStore(name string, intVal int64, strVal string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveKvStore(tx, name, intVal, strVal)
	checkErr(tx.Commit())
	commit = true
}

func saveKvStoreCheckOldValue(tx *sql.Tx, name string, intVal int64, oldIntVal int64, strVal string, oldStrVal string) bool {
	stmt, err := tx.Prepare("update KV_STORE set INT_VAL=$2, STR_VAL=$3 where NAME=$1 and INT_VAL=$4 and STR_VAL=$5")
	checkErr(err)
	defer stmt.Close()
	res, err := stmt.Exec(name, intVal, strVal, oldIntVal, oldStrVal)
	checkErr(err)
	rowCnt, err := res.RowsAffected()
	checkErr(err)
	if rowCnt == 1 {
		return true
	} else {
		return false
	}
}
