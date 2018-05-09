package db

import (
	"database/sql"

	tcc_pb "github.com/samoslab/nebula/tracker/collector/client/pb"
	tcp_pb "github.com/samoslab/nebula/tracker/collector/provider/pb"
)

func saveFromProvider(tx *sql.Tx, nodeId string, timestamp uint64, als []tcp_pb.ActionLog) {
	stmt, err := tx.Prepare("insert into ACTION_LOG(TICKET,TICKET_CLIENT_ID,PVD_NODE_ID,PVD_TYPE,PVD_TIMESTAMP,PVD_SUCCESS,PVD_FILE_HASH,PVD_FILE_SIZE,PVD_BLOCK_HASH,PVD_BLOCK_SIZE,PVD_BEGIN_TIME,PVD_END_TIME,PVD_TRANSPORT_SIZE,PVD_ERROR_INFO) values ()")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec()
}

func saveFromClient(tx *sql.Tx, nodeId string, timestamp uint64, als []tcc_pb.ActionLog) {

}
