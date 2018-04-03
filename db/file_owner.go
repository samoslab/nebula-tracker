package db

import (
	"database/sql"
	"strconv"
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

func saveFileOwner(tx *sql.Tx, nodeId string, isFolder bool, name string, parentId []byte, modTime uint64, hash string, size uint64) []byte {
	var lastInsertId []byte
	err := tx.QueryRow("insert into FILE_OWNER(REMOVED,CREATION,LAST_MODIFIED,NODE_ID,FOLDER,NAME,PARENT_ID,MOD_TIME,HASH,SIZE) values (false,now(),now(),$1,$2,$3,$4,$5,$6,$7) RETURNING ID", nodeId, isFolder, name, parentId, time.Unix(int64(modTime), 0), hash, size).Scan(&lastInsertId)
	checkErr(err)
	return lastInsertId
}

func FileOwnerMkFolders(nodeId string, parent []byte, folders []string) (firstDuplicationName string, err error) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	modTime := uint64(time.Now().Unix())
	for _, folder := range folders {
		saveFileOwner(tx, nodeId, true, folder, parent, modTime, "", 0) // TODO duplication of name
	}
	checkErr(tx.Commit())
	commit = true
	return "", nil
}

func fileOwnerListOfPathCount(tx *sql.Tx, nodeId string, parentId []byte) (total uint32) {
	rows, err := tx.Query("SELECT count(1) FROM FILE_OWNER where NODE_ID=$1 and PARENT_ID=$2 and REMOVED=false", nodeId, parentId)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&total)
		checkErr(err)
		return
	}
	return 0
}

func fileOwnerListOfPath(tx *sql.Tx, nodeId string, parentId []byte, pageSize uint32, pageNum uint32, sortField string, asc bool) []*Fof {
	sql := "SELECT FOLDER,NAME,MOD_TIME,HASH,SIZE FROM FILE_OWNER where NODE_ID=$1 and PARENT_ID=$2 and REMOVED=false order by FOLDER desc, "
	sql += sortField
	if asc {
		sql += " asc"
	} else {
		sql += " desc"
	}
	sql += " LIMIT "
	sql += strconv.Itoa(int(pageSize))
	sql += " OFFSET "
	sql += strconv.Itoa(int(pageNum*pageSize - pageSize))
	rows, err := tx.Query(sql, nodeId, parentId)
	checkErr(err)
	defer rows.Close()
	res := make([]*Fof, 0, pageSize)
	for rows.Next() {
		var isFolder bool
		var modTime, size uint64
		var hash []byte
		var name string
		err = rows.Scan(&isFolder, &name, &modTime, &hash, &size)
		checkErr(err)
		res = append(res, &Fof{IsFolder: isFolder, Name: name, ModTime: modTime, FileHash: hash, FileSize: size})
	}
	return res
}

type Fof struct {
	IsFolder bool
	Name     string
	ModTime  uint64
	FileHash []byte
	FileSize uint64
}

func FileOwnerListOfPath(nodeId string, parentId []byte, pageSize uint32, pageNum uint32, sortField string, asc bool) (total uint32, fofs []*Fof) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	if pageNum == 0 {
		pageNum = 1
	}
	total = fileOwnerListOfPathCount(tx, nodeId, parentId)
	if total == 0 || (pageNum-1)*pageSize >= total {
		return
	}
	fofs = fileOwnerListOfPath(tx, nodeId, parentId, pageSize, pageNum, sortField, asc)
	checkErr(tx.Commit())
	commit = true
	return
}
