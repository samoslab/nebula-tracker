package db

import (
	"database/sql"
	pb "nebula-tracker/api/check-availability/pb"
	"time"
)

func saveCheckAvailRecord(tx *sql.Tx, locality string, ps ...*pb.ProviderStatus) {
	stmt, err := tx.Prepare("insert into CHECK_AVAIL_RECORD(PROVIDER_ID,CHECK_TIME,LATENCY_NS,TOTAL_FREE_VOLUME,AVAIL_FILE_SIZE,CHECK_FROM,VERSION) values ($1,$2,$3,$4,$5,$6,$7)")
	checkErr(err)
	defer stmt.Close()
	for _, s := range ps {
		_, err := stmt.Exec(s.NodeId, time.Unix(0, int64(s.CheckTime)), s.LatencyNs, s.TotalFreeVolume, s.AvailFileSize, locality, s.Version)
		checkErr(err)
	}
}

func getProviderOfCheckAvailRecord(tx *sql.Tx, start time.Time, end time.Time) []string {
	rows, err := tx.Query("SELECT distinct PROVIDER_ID FROM CHECK_AVAIL_RECORD where CHECK_TIME between $1 and $2", start, end)
	checkErr(err)
	defer rows.Close()
	ps := make([]string, 0, 32)
	for rows.Next() {
		var pid string
		err = rows.Scan(&pid)
		checkErr(err)
		ps = append(ps, pid)
	}
	return ps
}

func GetProviderOfCheckAvailRecord(start time.Time, end time.Time) (ps []string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	ps = getProviderOfCheckAvailRecord(tx, start, end)
	checkErr(tx.Commit())
	commit = true
	return
}

func getByProviderAndCheckTimeBetween(tx *sql.Tx, providerId string, start time.Time, end time.Time) []int64 {
	rows, err := tx.Query("SELECT CHECK_TIME FROM CHECK_AVAIL_RECORD where PROVIDER_ID=$1 and CHECK_TIME between $2 and $3 order by CHECK_TIME asc", providerId, start, end)
	checkErr(err)
	defer rows.Close()
	res := make([]int64, 0, 64)
	for rows.Next() {
		var t time.Time
		err = rows.Scan(&t)
		checkErr(err)
		res = append(res, t.Unix())
	}
	return res
}

func getLastCheckAvailRecord(tx *sql.Tx, providerId string) (found bool, last time.Time) {
	weekAgo, _ := time.ParseDuration("-168h")
	now := time.Now()
	zeroClock := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	rows, err := tx.Query("SELECT CHECK_TIME FROM CHECK_AVAIL_RECORD where PROVIDER_ID=$1 and CHECK_TIME>$2 order by CHECK_TIME desc limit 1", providerId, zeroClock.Add(weekAgo))
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var t time.Time
		err = rows.Scan(&t)
		checkErr(err)
		return true, t
	}
	return
}

func GetLastCheckAvailRecord(providerId string) (found bool, last time.Time) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	found, last = getLastCheckAvailRecord(tx, providerId)
	checkErr(tx.Commit())
	commit = true
	return
}
