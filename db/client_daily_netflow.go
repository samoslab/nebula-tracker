package db

import (
	"database/sql"
	pb "nebula-tracker/api/collector/pb"
	"strconv"
	"time"
)

const DateAndHourTimeFormat = "2006-01-02 15"

func saveClientDailyNetflow(tx *sql.Tx, cis []*pb.ClientItem) {
	stmt, err := tx.Prepare("insert into CLIENT_DAILY_NETFLOW(NODE_ID,DAY,SERVICE_SEQ,UPSTREAM,CREATION,LAST_MODIFIED,NETFLOW) values ($1,$2,$3,$4,now(),now(),$5) ON CONFLICT (NODE_ID,DAY,SERVICE_SEQ,UPSTREAM) DO UPDATE SET LAST_MODIFIED=now(),NETFLOW=CLIENT_DAILY_NETFLOW.NETFLOW+excluded.NETFLOW")
	defer stmt.Close()
	checkErr(err)
	serviceSeqMap := make(map[string]uint32, 16)
	for _, ci := range cis {
		key := ci.NodeId + " " + ci.Day + " " + strconv.Itoa(int(ci.Hour))
		serviceSeq, ok := serviceSeqMap[key]
		if !ok {
			hourStart, err := time.Parse(DateAndHourTimeFormat, ci.Day+" "+strconv.Itoa(int(ci.Hour)))
			checkErr(err)
			serviceSeq = getServiceSeq(tx, ci.NodeId, hourStart)
			serviceSeqMap[key] = serviceSeq
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
	if !saveKvStoreCheckOldValue(tx, keyName, hs.NextStart, hs.Start, "", "") {
		return
	}
	saveClientDailyNetflow(tx, hs.ClientItem)
	saveProviderDailyNetflow(tx, hs.ProviderItem)
	checkErr(tx.Commit())
	commit = true
}
