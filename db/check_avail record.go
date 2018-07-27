package db

import (
	"database/sql"
	pb "nebula-tracker/api/check-availability/pb"
	"time"
)

func saveCheckAvailRecord(tx *sql.Tx, locality string, ps ...*pb.ProviderStatus) {
	stmt, err := tx.Prepare("insert into CHECK_AVAIL_RECORD(PROVIDER_ID,CHECK_TIME,LATENCY_NS,TOTAL_FREE_VOLUME,AVAIL_FILE_SIZE,CHECK_FROM) values ($1,$2,$3,$4,$5,$6)")
	checkErr(err)
	defer stmt.Close()
	for _, s := range ps {
		_, err := stmt.Exec(s.NodeId, time.Unix(0, int64(s.CheckTime)), s.LatencyNs, s.TotalFreeVolume, s.AvailFileSize, locality)
		checkErr(err)
	}
}
