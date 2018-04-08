package db

import (
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const slash = "/"

func FileOwnerIdOfFilePath(nodeId string, path string) (found bool, id []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	found, id = queryIdRecursion(tx, nodeId, path)
	checkErr(tx.Commit())
	commit = true
	return
}

func queryIdRecursion(tx *sql.Tx, nodeId string, path string) (found bool, id []byte) {
	paths := strings.Split(path[1:], slash)
	for _, p := range paths {
		id = queryId(tx, nodeId, id, p)
		if id == nil {
			return false, nil
		}
	}
	return true, id
}

func queryId(tx *sql.Tx, nodeId string, parent []byte, folderName string) []byte {
	var rows *sql.Rows
	var err error
	sqlStr := "SELECT ID FROM FILE_OWNER where NODE_ID=$1 and NAME=$2 and PARENT_ID%s and FOLDER=true"
	if parent == nil || len(parent) == 0 {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, " is null"), nodeId, folderName)
	} else {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, "=$3"), nodeId, folderName, parent)
	}
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
	var rows *sql.Rows
	var err error
	sqlStr := "SELECT ID,FOLDER FROM FILE_OWNER where NODE_ID=$1 and NAME=$2 and PARENT_ID%s"
	if parent == nil || len(parent) == 0 {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, " is null"), nodeId, name)
	} else {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, "=$3"), nodeId, name, parent)
	}
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&id, &isFolder)
		checkErr(err)
		return
	}
	return nil, false
}

func saveFileOwner(tx *sql.Tx, nodeId string, isFolder bool, name string, parent interface{}, modTime uint64, hash *sql.NullString, size uint64) []byte {
	var lastInsertId []byte
	err := tx.QueryRow("insert into FILE_OWNER(REMOVED,CREATION,LAST_MODIFIED,NODE_ID,FOLDER,NAME,PARENT_ID,MOD_TIME,HASH,SIZE) values (false,now(),now(),$1,$2,$3,$4,$5,$6,$7) RETURNING ID", nodeId, isFolder, name, parent, time.Unix(int64(modTime), 0), hash, size).Scan(&lastInsertId)
	checkErr(err)
	return lastInsertId
}

func FileOwnerMkFolders(nodeId string, parent []byte, folders []string) (firstDuplicationName string) {
	modTime := uint64(time.Now().Unix())
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	firstDuplicationName = fileOwnerMkFolders(tx, nodeId, parent, folders, modTime)
	checkErr(tx.Commit())
	commit = true
	return
}

func fileOwnerMkFolders(tx *sql.Tx, nodeId string, parent []byte, folders []string, modTime uint64) (firstDuplicationName string) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); !ok || strings.Index(e.Error(), "violates unique constraint") == -1 {
				panic(r)
			}
		}
	}()
	hash := &sql.NullString{}
	for _, folder := range folders {
		firstDuplicationName = folder
		saveFileOwner(tx, nodeId, true, folder, parent, modTime, hash, 0)
	}
	return ""
}

func fileOwnerListOfPathCount(tx *sql.Tx, nodeId string, parent []byte) (total uint32) {
	var rows *sql.Rows
	var err error
	sqlStr := "SELECT count(1) FROM FILE_OWNER where NODE_ID=$1 and PARENT_ID%s and REMOVED=false"
	if parent == nil || len(parent) == 0 {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, " is null"), nodeId)
	} else {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, "=$2"), nodeId, parent)
	}
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&total)
		checkErr(err)
		return
	}
	return 0
}

func fileOwnerListOfPath(tx *sql.Tx, nodeId string, parent []byte, pageSize uint32, pageNum uint32, sortField string, asc bool) []*Fof {
	var args []interface{}
	sqlStr := "SELECT FOLDER,NAME,MOD_TIME,HASH,SIZE FROM FILE_OWNER where NODE_ID=$1 and PARENT_ID%s"
	if parent == nil || len(parent) == 0 {
		sqlStr = fmt.Sprintf(sqlStr, " is null")
		args = []interface{}{nodeId}
	} else {
		sqlStr = fmt.Sprintf(sqlStr, "=$2")
		args = []interface{}{nodeId, parent}
	}
	sqlStr += " and REMOVED=false order by FOLDER desc, "
	sqlStr += sortField
	if asc {
		sqlStr += " asc"
	} else {
		sqlStr += " desc"
	}
	sqlStr += " LIMIT "
	sqlStr += strconv.Itoa(int(pageSize))
	sqlStr += " OFFSET "
	sqlStr += strconv.Itoa(int(pageNum*pageSize - pageSize))
	rows, err := tx.Query(sqlStr, args...)
	checkErr(err)
	defer rows.Close()
	res := make([]*Fof, 0, pageSize)
	for rows.Next() {
		var isFolder bool
		var size uint64
		var modTime time.Time
		var hashStr sql.NullString
		var name string
		err = rows.Scan(&isFolder, &name, &modTime, &hashStr, &size)
		checkErr(err)
		var hash []byte
		if hashStr.Valid {
			hash, err = base64.StdEncoding.DecodeString(hashStr.String)
			if err != nil {
				panic(err)
			}
		}
		res = append(res, &Fof{IsFolder: isFolder, Name: name, ModTime: uint64(modTime.Unix()), FileHash: hash, FileSize: size})
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

func FileOwnerRemove(nodeId string, pathId []byte, recursive bool) (res bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	if recursive || fileOwnerListOfPathCount(tx, nodeId, pathId) == 0 {
		fileOwnerRemove(tx, nodeId, pathId)
		res = true
	}
	checkErr(tx.Commit())
	commit = true
	return
}

func fileOwnerRemove(tx *sql.Tx, nodeId string, pathId []byte) {
	stmt, err := tx.Prepare("update FILE_OWNER set REMOVED=true where ID=$1 and NODE_ID=$2")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(pathId, nodeId)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}
