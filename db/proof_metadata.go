package db

import (
	"database/sql"
	"encoding/base64"
	"time"

	pb "github.com/samoslab/nebula/tracker/metadata/pb"
)

func saveProofMetadata(tx *sql.Tx, fileId []byte, creation time.Time, partitions []*pb.StorePartition) {
	for _, sp := range partitions {
		for _, block := range sp.Block {
			stmt, err := tx.Prepare("insert into PROOF_METADATA(HASH,SIZE,FILE_ID,CREATION,REMOVED,CHUNK_SIZE,PARAM_STR,GENERATOR,PUB_KEY,RANDOM,PHI) values($1,$2,$3,$4,false,$5,$6,$7,$8,$9," + arrayClause(len(block.Phi), 10))
			checkErr(err)
			defer stmt.Close()
			_, err = stmt.Exec(base64.StdEncoding.EncodeToString(block.Hash), block.Size, fileId, creation, block.ChunkSize, block.ParamStr, block.Generator, block.PubKey, block.Random, block.Phi)
			checkErr(err)
		}
	}
}
