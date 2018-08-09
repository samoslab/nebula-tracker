package db

import (
	"database/sql"
	"encoding/base64"
	"strings"
	"time"

	tcc_pb "github.com/samoslab/nebula/tracker/collector/client/pb"
	tcp_pb "github.com/samoslab/nebula/tracker/collector/provider/pb"
)

func saveFromProvider(tx *sql.Tx, nodeId string, timestamp uint64, als []*tcp_pb.ActionLog) {
	stmt, err := tx.Prepare("insert into ACTION_LOG(CREATION,TICKET,TICKET_CLIENT_ID,PVD_NODE_ID,PVD_TYPE,PVD_TIMESTAMP," +
		"PVD_SUCCESS,PVD_FILE_HASH,PVD_FILE_SIZE,PVD_BLOCK_HASH,PVD_BLOCK_SIZE,PVD_BEGIN_TIME,PVD_END_TIME," +
		"PVD_TRANSPORT_SIZE,PVD_ERROR_INFO) values (now(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) " +
		"ON CONFLICT (TICKET) DO UPDATE SET LAST_MODIFIED=now(),PVD_NODE_ID=$3,PVD_TYPE=$4,PVD_TIMESTAMP=$5,PVD_SUCCESS=$6," +
		"PVD_FILE_HASH=$7,PVD_FILE_SIZE=$8,PVD_BLOCK_HASH=$9,PVD_BLOCK_SIZE=$10,PVD_BEGIN_TIME=$11," +
		"PVD_END_TIME=$12,PVD_TRANSPORT_SIZE=$13,PVD_ERROR_INFO=$14")
	defer stmt.Close()
	checkErr(err)
	crs := make([]*CheatingRecord, 0, 4)
	for _, al := range als {
		pass, cltId := parseAndCheck(al.Ticket)
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
		_, err = stmt.Exec(al.Ticket, cltId, nodeId, al.Type, time.Unix(0, int64(timestamp)), al.Success,
			base64.StdEncoding.EncodeToString(al.FileHash), al.FileSize, base64.StdEncoding.EncodeToString(al.BlockHash), al.BlockSize, time.Unix(0, int64(al.BeginTime)), time.Unix(0, int64(al.EndTime)),
			al.TransportSize, al.Info)
		checkErr(err)
	}
	saveProviderCheatingRecord(tx, crs...)
}

func saveFromClient(tx *sql.Tx, nodeId string, timestamp uint64, als []*tcc_pb.ActionLog) {
	stmt, err := tx.Prepare("insert into ACTION_LOG(CREATION,TICKET,TICKET_CLIENT_ID,CLT_NODE_ID,CLT_OPPOSITE_NODE_ID,CLT_TYPE,CLT_TIMESTAMP," +
		"CLT_SUCCESS,CLT_FILE_HASH,CLT_FILE_SIZE,CLT_BLOCK_HASH,CLT_BLOCK_SIZE,CLT_BEGIN_TIME,CLT_END_TIME," +
		"CLT_TRANSPORT_SIZE,CLT_ERROR_INFO,PARTITION_SEQ,CHECKSUM,BLOCK_SEQ) values (now(),$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) " +
		"ON CONFLICT (TICKET) DO UPDATE SET LAST_MODIFIED=now(),CLT_NODE_ID=$3,CLT_OPPOSITE_NODE_ID=$4,CLT_TYPE=$5,CLT_TIMESTAMP=$6,CLT_SUCCESS=$7," +
		"CLT_FILE_HASH=$8,CLT_FILE_SIZE=$9,CLT_BLOCK_HASH=$10,CLT_BLOCK_SIZE=$11,CLT_BEGIN_TIME=$12," +
		"CLT_END_TIME=$13,CLT_TRANSPORT_SIZE=$14,CLT_ERROR_INFO=$15,PARTITION_SEQ=$16,CHECKSUM=$17,BLOCK_SEQ=$18")
	defer stmt.Close()
	checkErr(err)
	crs := make([]*CheatingRecord, 0, 4)
	for _, al := range als {
		pass, cltId := parseAndCheck(al.Ticket)
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
		_, err = stmt.Exec(al.Ticket, cltId, nodeId, al.OppositeNodeId, al.Type, time.Unix(0, int64(timestamp)), al.Success,
			base64.StdEncoding.EncodeToString(al.FileHash), al.FileSize, base64.StdEncoding.EncodeToString(al.BlockHash), al.BlockSize, time.Unix(0, int64(al.BeginTime)), time.Unix(0, int64(al.EndTime)),
			al.TransportSize, al.Info, al.PartitionSeq, al.Checksum, al.BlockSeq)
		checkErr(err)
	}
	saveClientCheatingRecord(tx, crs...)
}

func parseAndCheck(ticket string) (pass bool, nodeId string) {
	arr := strings.Split(ticket, "-")
	if len(arr) == 0 {
		return false, ""
	} else if len(arr) > 2 {
		return false, arr[0]
	} else {
		// if len(arr[0])==27||len(arr[1])==22{
		return true, arr[0]
		// }else{
		// 	return false,arr[0]
		// }
	}
}

func SaveFromProvider(nodeId string, timestamp uint64, als []*tcp_pb.ActionLog) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveFromProvider(tx, nodeId, timestamp, als)
	checkErr(tx.Commit())
	commit = true
}

func SaveFromClient(nodeId string, timestamp uint64, als []*tcc_pb.ActionLog) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	saveFromClient(tx, nodeId, timestamp, als)
	checkErr(tx.Commit())
	commit = true
}
