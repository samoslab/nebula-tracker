package db

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"time"

	"nebula-tracker/config"

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
	conn_str := fmt.Sprintf("postgresql://%s@%s:%d/%s?application_name=%s&sslmode=%s", dbConfig.User, dbConfig.Host, dbConfig.Port, dbConfig.Name, dbConfig.ApplicationName, dbConfig.SslMode)

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

func inClause(count int, first int) string {
	if count == 1 {
		return fmt.Sprintf("($%d)", first)
	} else if count == 0 {
		panic("count can't be zero")
	}
	var buffer bytes.Buffer
	buffer.WriteString("($")
	buffer.WriteString(strconv.Itoa(first))
	for i := 1; i < count; i++ {
		buffer.WriteString(", $")
		first++
		buffer.WriteString(strconv.Itoa(first))
	}
	buffer.WriteString(")")
	return buffer.String()
}

type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
func (self *NullTime) Scan(value interface{}) error {
	var t time.Time
	t, self.Valid = value.(time.Time)
	self.Time = time.Time(t)
	return nil
}

// Value implements the driver Valuer interface.
func (self NullTime) Value() (driver.Value, error) {
	if !self.Valid {
		return nil, nil
	}
	return self.Time, nil
}
