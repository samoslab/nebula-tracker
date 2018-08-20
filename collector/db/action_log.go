package db

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	pb "nebula-tracker/api/collector/pb"

	tcc_pb "github.com/samoslab/nebula/tracker/collector/client/pb"
	tcp_pb "github.com/samoslab/nebula/tracker/collector/provider/pb"
)

func saveFromProvider(tx *sql.Tx, nodeId string, timestamp uint64, als []*tcp_pb.ActionLog) {
	stmt, err := tx.Prepare("insert into ACTION_LOG(PVD_FIRST,CREATION,LAST_MODIFIED,TICKET,TICKET_CLIENT_ID,TICKET_PROVIDER_ID,PVD_NODE_ID,PVD_TYPE,PVD_TIMESTAMP," +
		"PVD_SUCCESS,PVD_FILE_HASH,PVD_FILE_SIZE,PVD_BLOCK_HASH,PVD_BLOCK_SIZE,PVD_BEGIN_TIME,PVD_END_TIME," +
		"PVD_TRANSPORT_SIZE,PVD_ERROR_INFO) values (true,now(),now(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) " +
		"ON CONFLICT (TICKET) DO UPDATE SET LAST_MODIFIED=now(),PVD_NODE_ID=$4,PVD_TYPE=$5,PVD_TIMESTAMP=$6,PVD_SUCCESS=$7," +
		"PVD_FILE_HASH=$8,PVD_FILE_SIZE=$9,PVD_BLOCK_HASH=$10,PVD_BLOCK_SIZE=$11,PVD_BEGIN_TIME=$12," +
		"PVD_END_TIME=$13,PVD_TRANSPORT_SIZE=$14,PVD_ERROR_INFO=$15")
	defer stmt.Close()
	checkErr(err)
	crs := make([]*CheatingRecord, 0, 4)
	for _, al := range als {
		pass, cltId, prvId := parseAndCheck(al.Ticket)
		if !pass {
			crs = append(crs, &CheatingRecord{NodeId: nodeId,
				ActionTime: time.Unix(0, int64(al.EndTime)),
				Type:       CHEATING_TYPE_WRONG_TICKET,
				Confirm:    true,
				Ticket:     al.Ticket})
			continue
		}
		if len(al.Info) > 250 {
			al.Info = al.Info[:250]
		}
		_, err = stmt.Exec(al.Ticket, cltId, prvId, nodeId, al.Type, time.Unix(0, int64(timestamp)), al.Success,
			base64.StdEncoding.EncodeToString(al.FileHash), al.FileSize, base64.StdEncoding.EncodeToString(al.BlockHash), al.BlockSize, time.Unix(0, int64(al.BeginTime)), time.Unix(0, int64(al.EndTime)),
			al.TransportSize, al.Info)
		checkErr(err)
	}
	saveProviderCheatingRecord(tx, crs...)
}

func saveFromClient(tx *sql.Tx, nodeId string, timestamp uint64, als []*tcc_pb.ActionLog) {
	stmt, err := tx.Prepare("insert into ACTION_LOG(PVD_FIRST,CREATION,LAST_MODIFIED,TICKET,TICKET_CLIENT_ID,TICKET_PROVIDER_ID,CLT_NODE_ID,CLT_TYPE,CLT_TIMESTAMP," +
		"CLT_SUCCESS,CLT_FILE_HASH,CLT_FILE_SIZE,CLT_BLOCK_HASH,CLT_BLOCK_SIZE,CLT_BEGIN_TIME,CLT_END_TIME," +
		"CLT_TRANSPORT_SIZE,CLT_ERROR_INFO,PARTITION_SEQ,CHECKSUM,BLOCK_SEQ) values (false,now(),now(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) " +
		"ON CONFLICT (TICKET) DO UPDATE SET LAST_MODIFIED=now(),CLT_NODE_ID=$4,CLT_TYPE=$5,CLT_TIMESTAMP=$6,CLT_SUCCESS=$7," +
		"CLT_FILE_HASH=$8,CLT_FILE_SIZE=$9,CLT_BLOCK_HASH=$10,CLT_BLOCK_SIZE=$11,CLT_BEGIN_TIME=$12," +
		"CLT_END_TIME=$13,CLT_TRANSPORT_SIZE=$14,CLT_ERROR_INFO=$15,PARTITION_SEQ=$16,CHECKSUM=$17,BLOCK_SEQ=$18")
	defer stmt.Close()
	checkErr(err)
	crs := make([]*CheatingRecord, 0, 4)
	for _, al := range als {
		pass, cltId, prvId := parseAndCheck(al.Ticket)
		if !pass {
			crs = append(crs, &CheatingRecord{NodeId: nodeId,
				ActionTime: time.Unix(0, int64(al.EndTime)),
				Type:       CHEATING_TYPE_WRONG_TICKET,
				Confirm:    true,
				Ticket:     al.Ticket})
			continue
		}
		if len(al.Info) > 250 {
			al.Info = al.Info[:250]
		}
		_, err = stmt.Exec(al.Ticket, cltId, prvId, nodeId, al.Type, time.Unix(0, int64(timestamp)), al.Success,
			base64.StdEncoding.EncodeToString(al.FileHash), al.FileSize, base64.StdEncoding.EncodeToString(al.BlockHash), al.BlockSize, time.Unix(0, int64(al.BeginTime)), time.Unix(0, int64(al.EndTime)),
			al.TransportSize, al.Info, al.PartitionSeq, al.Checksum, al.BlockSeq)
		checkErr(err)
	}
	saveClientCheatingRecord(tx, crs...)
}

func parseAndCheck(ticket string) (pass bool, clientId string, providerId string) {
	arr := strings.Split(ticket, "-")
	if len(arr) != 3 {
		return false, "", ""
	} else {
		return true, arr[0], arr[1]
	}
}

func SaveFromProvider(nodeId string, timestamp uint64, als []*tcp_pb.ActionLog) (err error) {
	defer func() {
		if er := recover(); er != nil {
			err = fmt.Errorf("db error: %s", er)
		}
	}()
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveFromProvider(tx, nodeId, timestamp, als)
	checkErr(tx.Commit())
	commit = true
	return
}

func SaveFromClient(nodeId string, timestamp uint64, als []*tcc_pb.ActionLog) (err error) {
	defer func() {
		if er := recover(); er != nil {
			err = fmt.Errorf("db error: %s", er)
		}
	}()
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveFromClient(tx, nodeId, timestamp, als)
	checkErr(tx.Commit())
	commit = true
	return
}

func hourySummarizeClient(tx *sql.Tx, start time.Time, end time.Time) []*pb.ClientItem {
	rows, err := tx.Query("select COALESCE(CLT_TYPE,PVD_TYPE)=1,TICKET_CLIENT_ID,COALESCE(CLT_END_TIME,PVD_END_TIME)::date,extract('hour',COALESCE(CLT_END_TIME,PVD_END_TIME)),(sum(COALESCE(PVD_TRANSPORT_SIZE,CLT_TRANSPORT_SIZE)))::int from action_log where CREATION between $1 and $2 group by COALESCE(CLT_TYPE,PVD_TYPE)=1,TICKET_CLIENT_ID,COALESCE(CLT_END_TIME,PVD_END_TIME)::date,extract('hour',COALESCE(CLT_END_TIME,PVD_END_TIME)) having sum(COALESCE(PVD_TRANSPORT_SIZE,CLT_TRANSPORT_SIZE))>0", start, end)
	checkErr(err)
	defer rows.Close()
	res := make([]*pb.ClientItem, 0, 32)
	for rows.Next() {
		ci := &pb.ClientItem{}
		var day time.Time
		err = rows.Scan(&ci.Upstream, &ci.NodeId, &day, &ci.Hour, &ci.Netflow)
		ci.Day = day.UTC().Format("2006-01-02")
		checkErr(err)
		res = append(res, ci)
	}
	return res
}

func hourySummarizeProvider(tx *sql.Tx, start time.Time, end time.Time) []*pb.ProviderItem {
	rows, err := tx.Query("select COALESCE(PVD_TYPE,CLT_TYPE),TICKET_PROVIDER_ID,COALESCE(PVD_END_TIME,CLT_END_TIME)::date,(sum(COALESCE(PVD_TRANSPORT_SIZE,CLT_TRANSPORT_SIZE)))::int from action_log where CREATION between $1 and $2 group by COALESCE(PVD_TYPE,CLT_TYPE),TICKET_PROVIDER_ID,COALESCE(PVD_END_TIME,CLT_END_TIME)::date having sum(COALESCE(PVD_TRANSPORT_SIZE,CLT_TRANSPORT_SIZE))>0", start, end)
	checkErr(err)
	defer rows.Close()
	res := make([]*pb.ProviderItem, 0, 32)
	for rows.Next() {
		ci := &pb.ProviderItem{}
		var day time.Time
		err = rows.Scan(&ci.Type, &ci.NodeId, &day, &ci.Netflow)
		ci.Day = day.UTC().Format("2006-01-02")
		checkErr(err)
		res = append(res, ci)
	}
	return res
}

func HouryNaSummarize(startTimestamp int64, nextStartTimestamp int64) (hs *pb.HourlySummary) {
	start := time.Unix(startTimestamp, 0)
	nextStart := time.Unix(nextStartTimestamp, 0)
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	pis := hourySummarizeProvider(tx, start, nextStart)
	cis := hourySummarizeClient(tx, start, nextStart)
	hs = &pb.HourlySummary{Start: startTimestamp, NextStart: nextStartTimestamp, ClientItem: cis, ProviderItem: pis}
	checkErr(tx.Commit())
	commit = true
	return
}
