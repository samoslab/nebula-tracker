package db

import (
	"database/sql"
	"strings"
	"time"
)

const slash = "/"

func FileOwnerIdOfFilePath(nodeId string, path string) (found bool, id []byte) {
	paths := strings.Split(path[1:], slash)
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	for _, p := range paths {
		id = queryId(tx, nodeId, id, p)
		if id == nil {
			return false, nil
		}
	}
	checkErr(tx.Commit())
	commit = true
	return true, id
}

func queryId(tx *sql.Tx, nodeId string, parent []byte, folderName string) []byte {
	rows, err := tx.Query("SELECT ID FROM FILE_OWNER where NODE_ID=$1 and PARENT_ID=$2 and NAME=$3 and FOLDER=true", nodeId, parent, folderName)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var id []byte
		err = rows.Scan(&id)
		checkErr(err)
		return id
	}
	return nil
}

func FileOwnerFileExists(nodeId string, parent []byte, name string) (id []byte, isFolder bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	id, isFolder = fileOwnerFileExists(tx, nodeId, parent, name)
	checkErr(tx.Commit())
	commit = true
	return
}

func fileOwnerFileExists(tx *sql.Tx, nodeId string, parent []byte, name string) (id []byte, isFolder bool) {
	rows, err := tx.Query("SELECT ID,FOLDER FROM FILE_OWNER where NODE_ID=$1 and PARENT_ID=$2 and NAME=$3", nodeId, parent, name)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&id, &isFolder)
		checkErr(err)
		return
	}
	return nil, false
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

func saveFileOwner(tx *sql.Tx, nodeId string, isFolder bool, name string, parentId []byte, modTime uint64, hash string, size uint64) []byte {
	var lastInsertId []byte
	err := tx.QueryRow("insert into FILE_OWNER(REMOVED,CREATION,LAST_MODIFIED,NODE_ID,FOLDER,NAME,PARENT_ID,MOD_TIME,HASH,SIZE) values (false,now(),now(),$1,$2,$3,$4,$5,$6,$7) RETURNING ID", nodeId, isFolder, name, parentId, time.Unix(int64(modTime), 0), hash, size).Scan(&lastInsertId)
	checkErr(err)
	return lastInsertId
}
