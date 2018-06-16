package db

import (
	"nebula-tracker/config"
	"testing"
)

func TestPackage(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	count := len(allPackageInfo(tx))
	var lastInsertId int64
	err := tx.QueryRow("insert into PACKAGE(NAME,PRICE,CREATION,LAST_MODIFIED,VOLUME,NETFLOW,UP_NETFLOW,DOWN_NETFLOW,VALID_DAYS) values('test basic package',15000000,now(),now(),1024,6144,3072,3072,30) RETURNING ID").Scan(&lastInsertId)
	checkErr(err)
	if lastInsertId == 0 {
		t.Error("failed")
	}
	err = tx.QueryRow("insert into PACKAGE(NAME,PRICE,CREATION,LAST_MODIFIED,VOLUME,NETFLOW,UP_NETFLOW,DOWN_NETFLOW,VALID_DAYS) values('test professional package',40000000,now(),now(),3072,18432,9216,9216,30) RETURNING ID").Scan(&lastInsertId)
	checkErr(err)
	if lastInsertId == 0 {
		t.Error("failed")
	}
	if len(allPackageInfo(tx)) != count+2 {
		t.Error("failed")
	}
	pi := getPackageInfo(tx, lastInsertId)
	if pi == nil {
		t.Error("failed")
	}
	if pi.ValidDays != 30 {
		t.Error("failed")
	}
}
