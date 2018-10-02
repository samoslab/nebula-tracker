package db

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"time"

	pb "github.com/samoslab/nebula/tracker/metadata/pb"
)

func saveProofMetadata(tx *sql.Tx, fileId []byte, creation time.Time, partitions []*pb.StorePartition) {
	for _, sp := range partitions {
		for _, block := range sp.Block {
			stmt, err := tx.Prepare("insert into PROOF_METADATA(HASH,SIZE,FILE_ID,CREATION,REMOVED,CHUNK_SIZE,PARAM_STR,GENERATOR,PUB_KEY,RANDOM,PHI_DATA,PHI_LENGTH) values($1,$2,$3,$4,false,$5,$6,$7,$8,$9,$10," + arrayClause(len(block.Phi), 11) + ")")
			checkErr(err)
			defer stmt.Close()
			args := make([]interface{}, 10, len(block.Phi)+10)
			args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9] = base64.StdEncoding.EncodeToString(block.Hash), block.Size, fileId, creation, block.ChunkSize, block.ParamStr, block.Generator, block.PubKey, block.Random, bytes.Join(block.Phi, []byte{})
			for _, el := range block.Phi {
				args = append(args, len(el))
			}
			_, err = stmt.Exec(args...)
			checkErr(err)
		}
	}
}

func getProofMetadata(tx *sql.Tx, fileId []byte, hash string) (chunkSize uint32, paramStr string, generator []byte, pubKey []byte, random []byte, phi [][]byte) {
	rows, err := tx.Query("select CHUNK_SIZE,PARAM_STR,GENERATOR,PUB_KEY,RANDOM,PHI_LENGTH,PHI_DATA from PROOF_METADATA where FILE_ID=$1 and HASH=$2 and REMOVED=false", fileId, hash)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var phiLength NullUint64Slice
		var phiData []byte
		err = rows.Scan(&chunkSize, &paramStr, &generator, &pubKey, &random, &phiLength, &phiData)
		checkErr(err)
		if phiLength.Valid {
			phi = make([][]byte, len(phiLength.Uint64Slice))
			var start, end int
			for i, bs := range phiLength.Uint64Slice {
				end = start + int(bs)
				phi[i] = phiData[start:end]
				start = end
			}
		}
		return
	}
	return
}

func GetProofMetadata(fileId []byte, hash string) (chunkSize uint32, paramStr string, generator []byte, pubKey []byte, random []byte, phi [][]byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	chunkSize, paramStr, generator, pubKey, random, phi = getProofMetadata(tx, fileId, hash)
	checkErr(tx.Commit())
	commit = true
	return
}
