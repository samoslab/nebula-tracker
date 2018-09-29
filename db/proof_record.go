package db

import (
	"database/sql"
)

func getProofInfo(tx *sql.Tx, id []byte) (chunkSize uint32, seq []uint32, randomNum [][]byte) {
	rows, err := tx.Query("select m.CHUNK_SIZE,r.CHUNK_SEQ,r.RANDOM_NUM from PROOF_METADATA m,PROOF_RECORD r where r.ID=$1 and r.FILE_ID=m.FILE_ID and r.BLOCK_HASH=m.HASH", id)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {

	}
	return 0, nil, nil
}
