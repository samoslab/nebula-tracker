package db

import (
	"database/sql"
	"time"
)

type PackageInfo struct {
	Id           int64
	Name         string
	Price        uint64
	Creation     time.Time
	LastModified time.Time
	Removed      bool
	Volume       uint32
	Netflow      uint32
	UpNetflow    uint32
	DownNetflow  uint32
	ValidDays    uint32
	Remark       string
}

func AllPackageInfo() (all []*PackageInfo) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	all = allPackageInfo(tx)
	checkErr(tx.Commit())
	commit = true
	return
}
func GetPackageInfo(id int64) (pi *PackageInfo) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	pi = getPackageInfo(tx, id)
	checkErr(tx.Commit())
	commit = true
	return
}

func allPackageInfo(tx *sql.Tx) []*PackageInfo {
	rows, err := tx.Query("select ID,NAME,PRICE,CREATION,LAST_MODIFIED,REMOVED,VOLUME,NETFLOW,UP_NETFLOW,DOWN_NETFLOW,VALID_DAYS,REMARK from PACKAGE where REMOVED=false")
	checkErr(err)
	defer rows.Close()
	res := make([]*PackageInfo, 0, 8)
	for rows.Next() {
		res = append(res, buildPackageInfo(rows))
	}
	return res
}

func buildPackageInfo(rows *sql.Rows) *PackageInfo {
	pi := PackageInfo{}
	var remarkNullable sql.NullString
	err := rows.Scan(&pi.Id, &pi.Name, &pi.Price, &pi.Creation, &pi.LastModified, &pi.Removed, &pi.Volume, &pi.Netflow, &pi.UpNetflow, &pi.DownNetflow, &pi.ValidDays, &remarkNullable)
	checkErr(err)
	if remarkNullable.Valid {
		pi.Remark = remarkNullable.String
	}
	return &pi
}

func getPackageInfo(tx *sql.Tx, id int64) *PackageInfo {
	rows, err := tx.Query("select ID,NAME,PRICE,CREATION,LAST_MODIFIED,REMOVED,VOLUME,NETFLOW,UP_NETFLOW,DOWN_NETFLOW,VALID_DAYS,REMARK from PACKAGE where REMOVED=false and ID=$1", id)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		return buildPackageInfo(rows)
	}
	return nil
}
