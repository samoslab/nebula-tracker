package db

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

func arrayClause(count int, first int) string {
	if count == 1 {
		return fmt.Sprintf("ARRAY[$%d]", first)
	} else if count == 0 {
		panic("count can't be zero")
	}
	var buffer bytes.Buffer
	buffer.WriteString("ARRAY[$")
	buffer.WriteString(strconv.Itoa(first))
	for i := 1; i < count; i++ {
		buffer.WriteString(", $")
		first++
		buffer.WriteString(strconv.Itoa(first))
	}
	buffer.WriteString("]")
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
	self.Time = t
	return nil
}

// Value implements the driver Valuer interface.
func (self NullTime) Value() (driver.Value, error) {
	if !self.Valid {
		return nil, nil
	}
	return self.Time, nil
}

type NullStrSlice struct {
	StrSlice []string
	Valid    bool
}

// Scan implements the Scanner interface.
func (self *NullStrSlice) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	var t []byte
	t, self.Valid = value.([]byte)
	if len(t) < 4 {
		return nil
	}
	str := string(t)

	l := len(str)
	if str[0:2] != `{"` || str[l-2:l] != `"}` {
		panic(errors.New(str + " format wrong"))
	}
	self.StrSlice = strings.Split(str[2:l-2], `","`)
	for i, _ := range self.StrSlice {
		self.StrSlice[i] = strings.Replace(self.StrSlice[i], `\"`, `"`, -1)
		self.StrSlice[i] = strings.Replace(self.StrSlice[i], `\\`, `\`, -1)
	}
	return nil
}

// Value implements the driver Valuer interface.
func (self NullStrSlice) Value() (driver.Value, error) {
	if !self.Valid {
		return nil, nil
	}
	return self.StrSlice, nil
}

type NullUint64Slice struct {
	Uint64Slice []uint64
	Valid       bool
}

// Scan implements the Scanner interface.
func (self *NullUint64Slice) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	var t []byte
	t, self.Valid = value.([]byte)
	if len(t) < 2 {
		return nil
	}
	str := string(t)
	l := len(str)
	if str[0:1] != `{` || str[l-1:l] != `}` {
		panic(errors.New(str + " format wrong"))
	}
	strSlice := strings.Split(str[1:l-1], `,`)
	self.Uint64Slice = make([]uint64, len(strSlice))
	var err error
	for i, _ := range strSlice {
		self.Uint64Slice[i], err = strconv.ParseUint(strSlice[i], 10, 0)
		if err != nil {
			panic(err)
		}
	}
	return nil
}
