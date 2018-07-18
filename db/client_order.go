package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

type OrderInfo struct {
	Id           []byte
	Removed      bool
	Creation     time.Time
	LastModified time.Time
	NodeId       string
	PackageId    int64
	Package      *PackageInfo
	Quanlity     uint32
	TotalAmount  uint64
	Upgraded     bool
	Discount     decimal.Decimal
	Volume       uint32
	Netflow      uint32
	UpNetflow    uint32
	DownNetflow  uint32
	ValidDays    uint32
	StartTime    uint64
	EndTime      uint64
	Paid         bool
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

func RemoveOrder(nodeId string, id []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	removeOrder(tx, nodeId, id)
	checkErr(tx.Commit())
	commit = true
}

func removeOrder(tx *sql.Tx, nodeId string, id []byte) {
	stmt, err := tx.Prepare("update CLIENT_ORDER set REMOVED=true,LAST_MODIFIED=now() where NODE_ID=$1 and ID=$2 and REMOVED=false and PAY_TIME is null")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(nodeId, id)
	checkErr(err)
	_, err = rs.RowsAffected()
	checkErr(err)
}

func myAllOrder(tx *sql.Tx, nodeId string, onlyNotExpired bool) []*OrderInfo {
	sqlStr := "select o.ID,o.REMOVED,o.CREATION,o.LAST_MODIFIED,o.NODE_ID,o.PACKAGE_ID,o.QUANTITY,o.TOTAL_AMOUNT,o.UPGRADED,o.DISCOUNT,o.VOLUME,o.NETFLOW,o.UP_NETFLOW,o.DOWN_NETFLOW,o.VALID_DAYS,o.START_TIME,o.END_TIME,o.PAY_TIME,o.REMARK,p.ID,p.NAME,p.PRICE,p.CREATION,p.LAST_MODIFIED,p.REMOVED,p.VOLUME,p.NETFLOW,p.UP_NETFLOW,p.DOWN_NETFLOW,p.VALID_DAYS,p.REMARK from CLIENT_ORDER o,PACKAGE p where o.NODE_ID=$1 and o.PACKAGE_ID=p.ID and o.REMOVED=false"
	if onlyNotExpired {
		sqlStr += " and (END_TIME is null or END_TIME>now())"
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
	rows, err := tx.Query("select o.ID,o.REMOVED,o.CREATION,o.LAST_MODIFIED,o.NODE_ID,o.PACKAGE_ID,o.QUANTITY,o.TOTAL_AMOUNT,o.UPGRADED,o.DISCOUNT,o.VOLUME,o.NETFLOW,o.UP_NETFLOW,o.DOWN_NETFLOW,o.VALID_DAYS,o.START_TIME,o.END_TIME,o.PAY_TIME,o.REMARK,p.ID,p.NAME,p.PRICE,p.CREATION,p.LAST_MODIFIED,p.REMOVED,p.VOLUME,p.NETFLOW,p.UP_NETFLOW,p.DOWN_NETFLOW,p.VALID_DAYS,p.REMARK from CLIENT_ORDER o,PACKAGE p where o.ID=$2 and o.NODE_ID=$1 and o.PACKAGE_ID=p.ID and o.REMOVED=false", nodeId, id)
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
	var startTime, endTime, payTime NullTime
	var orderRemarkNullable, packageRemarkNullable sql.NullString
	err := rows.Scan(&oi.Id, &oi.Removed, &oi.Creation, &oi.LastModified, &oi.NodeId, &oi.PackageId, &oi.Quanlity, &oi.TotalAmount,
		&oi.Upgraded, &oi.Discount, &oi.Volume, &oi.Netflow, &oi.UpNetflow, &oi.DownNetflow, &oi.ValidDays, &startTime, &endTime,
		&payTime, &orderRemarkNullable, &pi.Id, &pi.Name, &pi.Price, &pi.Creation, &pi.LastModified, &pi.Removed, &pi.Volume,
		&pi.Netflow, &pi.UpNetflow, &pi.DownNetflow, &pi.ValidDays, &packageRemarkNullable)
	checkErr(err)
	if startTime.Valid {
		oi.StartTime = uint64(startTime.Time.Unix())
	}
	if endTime.Valid {
		oi.EndTime = uint64(endTime.Time.Unix())
	}
	if payTime.Valid {
		oi.Paid = true
		oi.PayTime = uint64(payTime.Time.Unix())
	}
	if orderRemarkNullable.Valid {
		oi.Remark = orderRemarkNullable.String
	}
	if packageRemarkNullable.Valid {
		pi.Remark = packageRemarkNullable.String
	}
	oi.Package = &pi
	return &oi
}

func BuyPackage(nodeId string, packageId int64, quanlity uint32, cancelUnpaid bool, renew bool, endTime time.Time, upgrade bool, oldPackageId int64) (oi *OrderInfo) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	if cancelUnpaid {
		cancelUnpaidOrder(tx, nodeId)
	}
	pi := getPackageInfo(tx, packageId)
	var priceOffset uint64
	if upgrade {
		oldPi := getPackageInfo(tx, oldPackageId)
		if pi.Price > oldPi.Price {
			priceOffset = pi.Price - oldPi.Price
		}
	}
	discount := getPackageQuantityDiscount(tx, pi.Id, quanlity)
	id := buyPackage(tx, nodeId, pi, quanlity, discount, renew, endTime, upgrade, oldPackageId, priceOffset)
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

func updatePayTime(tx *sql.Tx, nodeId string, id []byte, startTime time.Time, endTime time.Time, payTime time.Time) {
	stmt, err := tx.Prepare("update CLIENT_ORDER set START_TIME=$3,END_TIME=$4,PAY_TIME=$5,LAST_MODIFIED=$6 where NODE_ID=$1 and ID=$2 and PAY_TIME is null")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(nodeId, id, startTime, endTime, payTime, payTime)
	checkErr(err)
	rowsAffected, err := rs.RowsAffected()
	checkErr(err)
	if rowsAffected == 0 {
		panic(fmt.Errorf("update order pay time failed, nodeId: %s, id: %x", nodeId, id))
	}
}

func buyPackage(tx *sql.Tx, nodeId string, pi *PackageInfo, quanlity uint32, discount decimal.Decimal, renew bool, endTime time.Time, upgrade bool, oldPackageId int64, priceOffset uint64) (id []byte) {
	totalAmount := uint64(decimal.New(int64(uint64(quanlity)*pi.Price), 0).Mul(discount).IntPart())
	remark := ""
	if upgrade {
		now := time.Now().UTC()
		offset := endTime.Sub(now)
		offsetAmount := uint64(offset.Hours()) * priceOffset / 30 / 24
		totalAmount += offsetAmount
		remark = fmt.Sprintf("%s~%s upgrade price: %d", now.Format("2006-01-02 15:04:05 -0700"), endTime.Format("2006-01-02 15:04:05 -0700"), offsetAmount)

	}
	err := tx.QueryRow("insert into CLIENT_ORDER(REMOVED,CREATION,LAST_MODIFIED,NODE_ID,PACKAGE_ID,QUANTITY,TOTAL_AMOUNT,UPGRADED,DISCOUNT,VOLUME,NETFLOW,UP_NETFLOW,DOWN_NETFLOW,VALID_DAYS,REMARK) values (false,now(),now(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING ID",
		nodeId, pi.Id, quanlity, totalAmount, upgrade, discount, pi.Volume, quanlity*pi.Netflow, quanlity*pi.UpNetflow, quanlity*pi.DownNetflow, quanlity*pi.ValidDays, remark).Scan(&id)
	checkErr(err)
	return
}

func PayOrder(nodeId string, orderId []byte, amount uint64, validDays uint32, packageId int64, volume uint32, netflow uint32, upNetflow uint32, downNetflow uint32) {
	payTime := time.Now().UTC()
	startTime := payTime
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	reduceBalanceToPayOrder(tx, nodeId, amount)
	inService, _, _, _, _, _, _, endServiceTime := getCurrentPackage(tx, nodeId)
	if inService {
		startTime = endServiceTime
	}
	dd, _ := time.ParseDuration(strconv.Itoa(24*int(validDays)) + "h")
	endTime := startTime.Add(dd)
	updatePayTime(tx, nodeId, orderId, startTime, endTime, payTime)
	updateCurrentPackage(tx, nodeId, packageId, volume, netflow, upNetflow, downNetflow, endTime, inService)
	if !inService {
		resetClientUsageAmountNetflow(tx, nodeId)
	}
	// TODO remove other upgrade order
	checkErr(tx.Commit())
	commit = true
	return
}
