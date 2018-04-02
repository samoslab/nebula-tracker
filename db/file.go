package db

import (
	"database/sql"
	"errors"
)

func FileCheckExist(hash string) (exist bool, active bool, removed bool, done bool, size uint64) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	exist, active, removed, done, size = fileCheckExist(tx, hash)
	checkErr(tx.Commit())
	commit = true
	return
}

func fileCheckExist(tx *sql.Tx, hash string) (exist bool, active bool, removed bool, done bool, size uint64) {
	rows, err := tx.Query("SELECT ACTIVE,REMOVED,DONE,SIZE FROM FILE where HASH=$1", hash)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&active, &removed, &done, &size)
		checkErr(err)
		exist = true
		return
	}
	exist = false
	return
}

func incrementRefCount(tx *sql.Tx, hash string) {
	stmt, err := tx.Prepare("update FILE set REF_COUNT=REF_COUNT+1,REMOVED=false,LAST_MODIFIED=now() where HASH=$1")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(hash)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func FileReuse(nodeId string, hash string, name string, size uint64, modTime uint64, parentId []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	incrementRefCount(tx, hash)
	ownerId := saveFileOwner(tx, nodeId, false, name, parentId, modTime, hash, size)
	saveFileVersion(tx, ownerId, nodeId, hash)
	checkErr(tx.Commit())
	commit = true
}

func fileSave(tx *sql.Tx, nodeId string, hash string, size uint64, fileData []byte, done bool, storeVolume uint64) {
	stmt, err := tx.Prepare("insert into FILE(HASH,CREATION,LAST_MODIFIED,ACTIVE,REMOVED,SIZE,DATA,REF_COUNT,BLOCKS,DONE,STORE_VOLUME,CREATOR_NODE_ID) values ($1,now(),now(),true,false,$2,$3,1,NULL,$4,$5,$6)")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(hash, size, fileData, done, storeVolume, nodeId)
	checkErr(err)
}

func FileSaveTiny(nodeId string, hash string, fileData []byte, name string, size uint64, modTime uint64, parentId []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	fileSave(tx, nodeId, hash, size, fileData, true, size*3)
	ownerId := saveFileOwner(tx, nodeId, false, name, parentId, modTime, hash, size)
	saveFileVersion(tx, ownerId, nodeId, hash)
	checkErr(tx.Commit())
	commit = true
}

func FileSaveStep1(nodeId string, hash string, size uint64, storeVolume uint64) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	fileSave(tx, nodeId, hash, size, nil, false, storeVolume)
	checkErr(tx.Commit())
	commit = true
}
