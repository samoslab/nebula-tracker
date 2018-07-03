package db

import (
	"database/sql"
	"errors"
	"time"
)

func FileCheckExist(nodeId string, hash string, doneExpSecs int) (exist bool, active bool, done bool, fileType string, size uint64, selfCreate bool, doneExpired bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	var removed bool
	var creatorNodeId string
	var lastModified time.Time
	exist, active, removed, done, fileType, size, creatorNodeId, lastModified = fileCheckExist(tx, hash)
	if exist {
		if creatorNodeId == nodeId {
			selfCreate = true
			if removed {
				fileChangeRemoved(tx, hash, false)
			}
		} else {
			if time.Now().Unix()-lastModified.Unix() > int64(doneExpSecs) {
				doneExpired = true
				fileChangeCreatorNodeId(tx, hash, nodeId)
			}
		}
	}
	checkErr(tx.Commit())
	commit = true
	return
}

func fileChangeCreatorNodeId(tx *sql.Tx, hash string, nodeId string) {
	stmt, err := tx.Prepare("update FILE set CREATOR_NODE_ID=$2,REMOVED=false,LAST_MODIFIED=now() where HASH=$1")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(hash, nodeId)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func fileChangeRemoved(tx *sql.Tx, hash string, removed bool) {
	stmt, err := tx.Prepare("update FILE set REMOVED=$2,LAST_MODIFIED=now() where HASH=$1 and REMOVED=$3")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(hash, removed, !removed)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func fileCheckExist(tx *sql.Tx, hash string) (exist bool, active bool, removed bool, done bool, fileType string, size uint64, creatorNodeId string, lastModified time.Time) {
	rows, err := tx.Query("SELECT ACTIVE,REMOVED,DONE,TYPE,SIZE,CREATOR_NODE_ID,LAST_MODIFIED FROM FILE where HASH=$1", hash)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var typeNullable sql.NullString
		err = rows.Scan(&active, &removed, &done, &typeNullable, &size, &creatorNodeId, &lastModified)
		checkErr(err)
		exist = true
		if typeNullable.Valid {
			fileType = typeNullable.String
		}
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

func FileReuse(existId []byte, nodeId string, hash string, name string, size uint64, modTime uint64, spaceNo uint32, parentId []byte, fileType string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	incrementRefCount(tx, hash)
	if len(existId) > 0 {
		updateFileOwnerNewVersion(tx, existId, nodeId, modTime, hash, size)
	} else {
		existId = saveFileOwner(tx, nodeId, false, name, spaceNo, parentId, fileType, modTime, &sql.NullString{Valid: true, String: hash}, size)
	}
	saveFileVersion(tx, existId, nodeId, hash, fileType)
	checkErr(tx.Commit())
	commit = true
}

func fileSave(tx *sql.Tx, nodeId string, hash string, encryptKey interface{}, fileType string, size uint64, fileData []byte, done bool, storeVolume uint64) {
	stmt, err := tx.Prepare("insert into FILE(HASH,CREATION,LAST_MODIFIED,ACTIVE,REMOVED,ENCRYPT_KEY,TYPE,SIZE,DATA,REF_COUNT,DONE,STORE_VOLUME,CREATOR_NODE_ID) values ($1,now(),now(),true,false,$2,$3,$4,$5,1,$6,$7,$8)")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(hash, encryptKey, fileType, size, fileData, done, storeVolume, nodeId)
	checkErr(err)
}

func FileSaveTiny(existId []byte, nodeId string, hash string, fileData []byte, name string, size uint64, modTime uint64, spaceNo uint32, parentId []byte, fileType string, encryptKey []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	fileSave(tx, nodeId, hash, encryptKey, fileType, size, fileData, true, size*3)
	if len(existId) > 0 {
		updateFileOwnerNewVersion(tx, existId, nodeId, modTime, hash, size)
	} else {
		existId = saveFileOwner(tx, nodeId, false, name, spaceNo, parentId, fileType, modTime, &sql.NullString{Valid: true, String: hash}, size)
	}
	saveFileVersion(tx, existId, nodeId, hash, fileType)
	checkErr(tx.Commit())
	commit = true
}

func FileSaveStep1(nodeId string, hash string, fileType string, size uint64, storeVolume uint64) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	fileSave(tx, nodeId, hash, nil, fileType, size, nil, false, storeVolume)
	checkErr(tx.Commit())
	commit = true
}

func fileSaveDone(tx *sql.Tx, hash string, partitionCount int, blocks []string, storeVolume uint64, fileType string, encryptKey interface{}) {
	stmt, err := tx.Prepare("update FILE set PARTITION_COUNT=$2,BLOCKS=" + arrayClause(len(blocks), 6) + ",DONE=true,LAST_MODIFIED=now(),STORE_VOLUME=$3,TYPE=$4,ENCRYPT_KEY=$5 where HASH=$1")
	defer stmt.Close()
	checkErr(err)
	args := make([]interface{}, 5, len(blocks)+5)
	args[0], args[1], args[2], args[3], args[4] = hash, partitionCount, storeVolume, fileType, encryptKey
	for _, str := range blocks {
		args = append(args, str)
	}
	rs, err := stmt.Exec(args...)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func FileSaveDone(existId []byte, nodeId string, hash string, name string, fileType string, size uint64, modTime uint64, spaceNo uint32, parentId []byte, partitionCount int, blocks []string, storeVolume uint64, encryptKey []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	fileSaveDone(tx, hash, partitionCount, blocks, storeVolume, fileType, encryptKey)
	if len(existId) > 0 {
		updateFileOwnerNewVersion(tx, existId, nodeId, modTime, hash, size)
	} else {
		existId = saveFileOwner(tx, nodeId, false, name, spaceNo, parentId, fileType, modTime, &sql.NullString{Valid: true, String: hash}, size)
	}
	saveFileVersion(tx, existId, nodeId, hash, fileType)
	checkErr(tx.Commit())
	commit = true
}

func fileRetrieve(tx *sql.Tx, hash string) (exist bool, active bool, fileData []byte, partitionCount int, blocks []string, size uint64, fileType string, encryptKey []byte) {
	rows, err := tx.Query("SELECT ACTIVE,DATA,PARTITION_COUNT,BLOCKS,DONE,SIZE,TYPE,ENCRYPT_KEY FROM FILE where HASH=$1", hash)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var done bool
		var blockSlice NullStrSlice
		var typeNullable sql.NullString
		err = rows.Scan(&active, &fileData, &partitionCount, &blockSlice, &done, &size, &typeNullable, encryptKey)
		checkErr(err)
		exist = done
		if blockSlice.Valid {
			blocks = blockSlice.StrSlice
		}
		if typeNullable.Valid {
			fileType = typeNullable.String
		}
		return
	}
	exist = false
	return
}

func FileRetrieve(hash string) (exist bool, active bool, fileData []byte, partitionCount int, blocks []string, size uint64, fileType string, encryptKey []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	exist, active, fileData, partitionCount, blocks, size, fileType, encryptKey = fileRetrieve(tx, hash)
	checkErr(tx.Commit())
	commit = true
	return
}
