package db

import (
	"nebula-tracker/config"
	"testing"
)

func TestPackageDiscount(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	var packageId int64
	err := tx.QueryRow("insert into PACKAGE(NAME,PRICE,CREATION,LAST_MODIFIED,VOLUME,NETFLOW,UP_NETFLOW,DOWN_NETFLOW,VALID_DAYS) values('test basic package',15000000,now(),now(),1024,6144,3072,3072,30) RETURNING ID").Scan(&packageId)
	checkErr(err)
	if packageId == 0 {
		t.Error("failed")
	}
	count := len(getPackageDiscount(tx, packageId))
	var discountId1 int64
	err = tx.QueryRow("insert into PACKAGE_DISCOUNT(PACKAGE_ID,QUANTITY,DISCOUNT) values($1,3,'0.9') RETURNING ID", packageId).Scan(&discountId1)
	checkErr(err)
	if discountId1 == 0 {
		t.Error("failed")
	}

	var discountId2 int64
	err = tx.QueryRow("insert into PACKAGE_DISCOUNT(PACKAGE_ID,QUANTITY,DISCOUNT) values($1,5,'0.8') RETURNING ID", packageId).Scan(&discountId2)
	checkErr(err)
	if discountId2 == 0 {
		t.Error("failed")
	}
	m := getPackageDiscount(tx, packageId)
	if len(m) != count+2 {
		t.Error("failed")
	}
	if m[3].String() != "0.9" {
		t.Error("failed")
	}
	if m[5].String() != "0.8" {
		t.Error("failed")
	}
}
