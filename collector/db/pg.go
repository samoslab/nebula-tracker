package db

import (
	"database/sql"
	"fmt"

	"nebula-tracker/collector/config"

	_ "github.com/lib/pq"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

var db *sql.DB

func OpenDb(dbConfig *config.Db) *sql.DB {
	//	conn_str := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", dbConfig.Host, dbConfig.Port, dbConfig.User, dbConfig.Password, dbConfig.Name, dbConfig.SslMode)
	var conn_str string
	if dbConfig.SslMode == "disable" {
		conn_str = fmt.Sprintf("postgresql://%s@%s:%d/%s?application_name=%s&sslmode=%s", dbConfig.User, dbConfig.Host, dbConfig.Port, dbConfig.Name, dbConfig.ApplicationName, dbConfig.SslMode)
	} else {
		conn_str = fmt.Sprintf("postgresql://%s@%s:%d/%s?sslmode=%s&sslrootcert=%s&sslcert=%s&sslkey=%s", dbConfig.User, dbConfig.Host, dbConfig.Port, dbConfig.Name, dbConfig.SslMode, dbConfig.SslRootCert, dbConfig.SslCert, dbConfig.SslKey)
	}
	//	fmt.Println(conn_str)
	var err error
	db, err = sql.Open("postgres", conn_str)
	checkErr(err)
	db.SetMaxOpenConns(dbConfig.MaxOpenConns)
	db.SetMaxIdleConns(dbConfig.MaxIdleConns)
	db.Ping()
	return db
}

func CloseDb() {
	db.Close()
}

func beginTx() (*sql.Tx, bool) {
	tx, err := db.Begin()
	checkErr(err)
	return tx, false
}

func rollback(tx *sql.Tx, commit *bool) {
	if !*commit {
		tx.Rollback()
	}
}
