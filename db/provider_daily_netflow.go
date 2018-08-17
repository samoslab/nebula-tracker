package db

import (
	"database/sql"
	pb "nebula-tracker/api/collector/pb"
)

func saveProviderDailyNetflow(tx *sql.Tx, pis []*pb.ProviderItem) {
	stmt, err := tx.Prepare("insert into PROVIDER_DAILY_NETFLOW(NODE_ID,DAY,TYPE,CREATION,LAST_MODIFIED,NETFLOW) values ($1,$2,$3,now(),now(),$4) ON CONFLICT (NODE_ID,DAY,TYPE) DO UPDATE SET LAST_MODIFIED=now(),NETFLOW=PROVIDER_DAILY_NETFLOW.NETFLOW+excluded.NETFLOW")
	defer stmt.Close()
	checkErr(err)
	for _, pi := range pis {
		_, err = stmt.Exec(pi.NodeId, pi.Day, pi.Type, pi.Netflow)
		checkErr(err)
	}
}
