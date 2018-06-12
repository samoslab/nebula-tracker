package db

import "database/sql"

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

func MyAllOrder(nodeId string) (res []*OrderInfo) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	res = myAllOrder(tx, nodeId)
	checkErr(tx.Commit())
	commit = true
	return
}

func GetOrderInfo(nodeId string, id string) (oi *OrderInfo) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	oi = getOrderInfo(tx, nodeId, id)
	checkErr(tx.Commit())
	commit = true
	return
}

func myAllOrder(tx *sql.Tx, nodeId string) []*OrderInfo {
	rows, err := tx.Query("select o.ID,o.REMOVED,o.CREATION,o.LAST_MODIFIED,o.NODE_ID,o.PACKAGE_ID,o.QUANTITY,o.TOTAL_AMOUNT,o.UPGRADED,o.DISCOUNT,o.VOLUME,o.NETFLOW,o.UP_NETFLOW,o.DOWN_NETFLOW,o.VALID_DAYS,o.START_TIME,o.END_TIME,o.PAY_TIME,o.REMARK,p.ID,p.NAME,p.LEVEL,p.PRICE,p.CREATION,p.LAST_MODIFIED,p.REMOVED,p.VOLUME,p.NETFLOW,p.UP_NETFLOW,p.DOWN_NETFLOW,p.VALID_DAYS,p.REMARK from CLIENT_ORDER o,PACKAGE p where o.NODE_ID=$1 and o.PACKAGE_ID=p.ID and o.REMOVED=false", nodeId)
	checkErr(err)
	defer rows.Close()
	res := make([]*OrderInfo, 0, 8)
	for rows.Next() {
		res = append(res, buildOrderInfo(rows))
	}
	return res
}

func getOrderInfo(tx *sql.Tx, nodeId string, id string) (oi *OrderInfo) {
	rows, err := tx.Query("select o.ID,o.REMOVED,o.CREATION,o.LAST_MODIFIED,o.NODE_ID,o.PACKAGE_ID,o.QUANTITY,o.TOTAL_AMOUNT,o.UPGRADED,o.DISCOUNT,o.VOLUME,o.NETFLOW,o.UP_NETFLOW,o.DOWN_NETFLOW,o.VALID_DAYS,o.START_TIME,o.END_TIME,o.PAY_TIME,o.REMARK,p.ID,p.NAME,p.LEVEL,p.PRICE,p.CREATION,p.LAST_MODIFIED,p.REMOVED,p.VOLUME,p.NETFLOW,p.UP_NETFLOW,p.DOWN_NETFLOW,p.VALID_DAYS,p.REMARK from CLIENT_ORDER o,PACKAGE p where o.ID=$2 and o.NODE_ID=$1 and o.PACKAGE_ID=p.ID and o.REMOVED=false", nodeId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		return buildOrderInfo(rows)
	}
	return nil
}

func buildOrderInfo(rows *sql.Rows) *OrderInfo {

	return nil
}
