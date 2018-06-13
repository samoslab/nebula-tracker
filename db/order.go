package db

import (
	"database/sql"
)

type OrderInfo struct {
	Id           []byte
	Removed      bool
	Creation     uint64
	LastModified uint64
	NodeId       string
	PackageId    int64
	Package      *PackageInfo
	Quanlity     uint32
	TotalAmount  uint64
	Upgraded     bool
	Discount     float32
	Volume       uint32
	Netflow      uint32
	UpNetflow    uint32
	DownNetflow  uint32
	ValidDays    uint32
	StartTime    uint64
	EndTime      uint64
	PayTime      uint64
	Remark       string
}

func MyAllOrder(nodeId string, onlyNotExpired bool) (res []*OrderInfo) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	res = myAllOrder(tx, nodeId, onlyNotExpired)
	checkErr(tx.Commit())
	commit = true
	return
}

func GetOrderInfo(nodeId string, id []byte) (oi *OrderInfo) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	oi = getOrderInfo(tx, nodeId, id)
	checkErr(tx.Commit())
	commit = true
	return
}

func myAllOrder(tx *sql.Tx, nodeId string, onlyNotExpired bool) []*OrderInfo {
	sqlStr := "select o.ID,o.REMOVED,o.CREATION,o.LAST_MODIFIED,o.NODE_ID,o.PACKAGE_ID,o.QUANTITY,o.TOTAL_AMOUNT,o.UPGRADED,o.DISCOUNT,o.VOLUME,o.NETFLOW,o.UP_NETFLOW,o.DOWN_NETFLOW,o.VALID_DAYS,o.START_TIME,o.END_TIME,o.PAY_TIME,o.REMARK,p.ID,p.NAME,p.LEVEL,p.PRICE,p.CREATION,p.LAST_MODIFIED,p.REMOVED,p.VOLUME,p.NETFLOW,p.UP_NETFLOW,p.DOWN_NETFLOW,p.VALID_DAYS,p.REMARK from CLIENT_ORDER o,PACKAGE p where o.NODE_ID=$1 and o.PACKAGE_ID=p.ID and o.REMOVED=false"
	if onlyNotExpired {
		sqlStr += " and END_TIME>now()"
	}
	rows, err := tx.Query(sqlStr, nodeId)
	checkErr(err)
	defer rows.Close()
	res := make([]*OrderInfo, 0, 8)
	for rows.Next() {
		res = append(res, buildOrderInfo(rows))
	}
	return res
}

func getOrderInfo(tx *sql.Tx, nodeId string, id []byte) (oi *OrderInfo) {
	rows, err := tx.Query("select o.ID,o.REMOVED,o.CREATION,o.LAST_MODIFIED,o.NODE_ID,o.PACKAGE_ID,o.QUANTITY,o.TOTAL_AMOUNT,o.UPGRADED,o.DISCOUNT,o.VOLUME,o.NETFLOW,o.UP_NETFLOW,o.DOWN_NETFLOW,o.VALID_DAYS,o.START_TIME,o.END_TIME,o.PAY_TIME,o.REMARK,p.ID,p.NAME,p.LEVEL,p.PRICE,p.CREATION,p.LAST_MODIFIED,p.REMOVED,p.VOLUME,p.NETFLOW,p.UP_NETFLOW,p.DOWN_NETFLOW,p.VALID_DAYS,p.REMARK from CLIENT_ORDER o,PACKAGE p where o.ID=$2 and o.NODE_ID=$1 and o.PACKAGE_ID=p.ID and o.REMOVED=false", nodeId, id)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		return buildOrderInfo(rows)
	}
	return nil
}

func buildOrderInfo(rows *sql.Rows) *OrderInfo {
	oi := OrderInfo{}
	pi := PackageInfo{}
	err := rows.Scan(&oi.Id, &oi.Removed, &oi.Creation, &oi.LastModified, &oi.NodeId, &oi.PackageId, &oi.Quanlity, &oi.TotalAmount,
		&oi.Upgraded, &oi.Discount, &oi.Volume, &oi.Netflow, &oi.UpNetflow, &oi.DownNetflow, &oi.ValidDays, &oi.StartTime, &oi.EndTime,
		&oi.PayTime, &oi.Remark, &pi.Id, &pi.Name, &pi.Level, &pi.Price, &pi.Creation, &pi.LastModified, &pi.Removed, &pi.Volume,
		&pi.Netflow, &pi.UpNetflow, &pi.DownNetflow, &pi.ValidDays, &pi.Remark)
	checkErr(err)
	oi.Package = &pi
	return &oi
}

func BuyPackage(nodeId string, packageId int64, quanlity uint32, cancelUnpaid bool) (oi *OrderInfo) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	if cancelUnpaid {
		cancelUnpaidOrder(tx, nodeId)
	}
	pi := getPackageInfo(tx, packageId)
	id := buyPackage(tx, nodeId, pi, quanlity)
	oi = getOrderInfo(tx, nodeId, id)
	checkErr(tx.Commit())
	commit = true
	return
}

func cancelUnpaidOrder(tx *sql.Tx, nodeId string) {
	stmt, err := tx.Prepare("update CLIENT_ORDER set REMOVED=true,LAST_MODIFIED=now() where NODE_ID=$1 and REMOVED=false and PAY_TIME is null")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(nodeId)
	checkErr(err)
	_, err = rs.RowsAffected()
	checkErr(err)
}

func buyPackage(tx *sql.Tx, nodeId string, pi *PackageInfo, quanlity uint32) []byte {
	var lastInsertId []byte
	// TODO
	err := tx.QueryRow("insert into CLIENT_ORDER(REMOVED,CREATION,LAST_MODIFIED,NODE_ID,PACKAGE_ID,QUANTITY,TOTAL_AMOUNT,UPGRADED,DISCOUNT,VOLUME,NETFLOW,UP_NETFLOW,DOWN_NETFLOW,VALID_DAYS,START_TIME,END_TIME) values (false,now(),now(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING ID",
		nodeId, pi.Id, quanlity, uint64(quanlity)*pi.Price, false, 1, pi.Volume, quanlity*pi.Netflow, quanlity*pi.UpNetflow, quanlity*pi.DownNetflow, quanlity*pi.ValidDays, nil, nil).Scan(&lastInsertId)
	checkErr(err)
	return lastInsertId
}
