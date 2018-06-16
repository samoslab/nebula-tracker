package db

import (
	"encoding/base64"
	"nebula-tracker/config"
	"testing"
	"time"
)

func TestClientOrder(t *testing.T) {
	conf := config.GetTrackerConfig()
	dbo := OpenDb(&conf.Db)
	defer dbo.Close()
	tx, _ := dbo.Begin()
	defer tx.Rollback()
	var lastInsertId int64
	err := tx.QueryRow("insert into PACKAGE(NAME,PRICE,CREATION,LAST_MODIFIED,VOLUME,NETFLOW,UP_NETFLOW,DOWN_NETFLOW,VALID_DAYS) values('test basic package',15000000,now(),now(),1024,6144,3072,3072,30) RETURNING ID").Scan(&lastInsertId)
	checkErr(err)
	if lastInsertId == 0 {
		t.Error("failed")
	}
	pi := getPackageInfo(tx, lastInsertId)
	nodeId := base64.StdEncoding.EncodeToString(sha1Sum([]byte("test node id")))
	email := "testemail@testemail.com"
	saveClient(tx, nodeId, []byte("test-public-key"), email, "random")
	if !existsNodeId(tx, nodeId) {
		t.Errorf("Failed.")
	}
	count := len(myAllOrder(tx, nodeId, false))
	orderId := buyPackage(tx, nodeId, pi, 3, false, time.Now(), false, 0, 0)
	if len(orderId) == 0 {
		t.Error("failed")
	}
	oi := getOrderInfo(tx, nodeId, orderId)
	if oi == nil {
		t.Error("failed")
	}
	if len(myAllOrder(tx, nodeId, false)) != count+1 {
		t.Errorf("Failed.")
	}
	myAllOrder(tx, nodeId, true)
}
