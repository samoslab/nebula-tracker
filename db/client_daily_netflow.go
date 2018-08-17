package db

import (
	"database/sql"
	pb "nebula-tracker/api/collector/pb"
	"time"
)

func saveClientDailyNetflow(tx *sql.Tx, start time.Time, cis []*pb.ClientItem) {
	stmt, err := tx.Prepare("insert into CLIENT_DAILY_NETFLOW(NODE_ID,DAY,SERVICE_SEQ,UPSTREAM,CREATION,LAST_MODIFIED,NETFLOW) values ($1,$2,$3,$4,now(),now(),$5) ON CONFLICT (NODE_ID,DAY,SERVICE_SEQ,UPSTREAM) DO UPDATE SET LAST_MODIFIED=now(),NETFLOW=CLIENT_DAILY_NETFLOW.NETFLOW+excluded.NETFLOW")
	defer stmt.Close()
	checkErr(err)
	serviceSeqMap := make(map[string]uint32, 16)
	for _, ci := range cis {
		serviceSeq, ok := serviceSeqMap[ci.NodeId]
		if !ok {
			serviceSeq = getServiceSeq(tx, ci.NodeId, start)
			serviceSeqMap[ci.NodeId] = serviceSeq
		}
		_, err = stmt.Exec(ci.NodeId, ci.Day, serviceSeq, ci.Upstream, ci.Netflow)
		checkErr(err)
	}
}

func getClientNetflow(tx *sql.Tx, nodeId string, serviceSeq uint32) (downNetflow uint64, upNetflow uint64) {
	rows, err := tx.Query("SELECT UPSTREAM,sum(NETFLOW+FIX_OFFSET) FROM CLIENT_DAILY_NETFLOW where NODE_ID=$1 and SERVICE_SEQ=$2 group by UPSTREAM", nodeId, serviceSeq)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var upstream bool
		var netflow uint64
		err = rows.Scan(&upstream, &netflow)
		checkErr(err)
		if upstream {
			upNetflow = netflow
		} else {
			downNetflow = netflow
		}
	}
	return
}

func SaveHourlySummary(keyName string, hs *pb.HourlySummary) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	if !saveKvStoreCheckOldValue(tx, keyName, hs.Start, hs.Last, "", "") {
		return
	}
	start := time.Unix(hs.Start, 0)
	saveClientDailyNetflow(tx, start, hs.ClientItem)
	saveProviderDailyNetflow(tx, hs.ProviderItem)
	checkErr(tx.Commit())
	commit = true
}
