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

func FileOwnerIdOfFilePath(nodeId string, path string, spaceNo uint32) (found bool, id []byte, isFolder bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	found, id, isFolder = queryIdRecursion(tx, nodeId, path, spaceNo)
	checkErr(tx.Commit())
	commit = true
	return
}

func queryIdRecursion(tx *sql.Tx, nodeId string, path string, spaceNo uint32) (found bool, id []byte, isFolder bool) {
	if path[len(path)-1] == '/' {
		path = path[0 : len(path)-1]
	}
	paths := strings.Split(path[1:], slash)
	for _, p := range paths {
		found, id, isFolder = queryId(tx, nodeId, id, p, spaceNo)
		if !found {
			return
		}
	}
	return
}

func queryId(tx *sql.Tx, nodeId string, parent []byte, folderName string, spaceNo uint32) (found bool, id []byte, isFolder bool) {
	var rows *sql.Rows
	var err error
	sqlStr := "SELECT ID,FOLDER FROM FILE_OWNER where NODE_ID=$1 and NAME=$2 and SPACE_NO=$3 and PARENT_ID%s and FOLDER=true and REMOVED=false"
	if parent == nil || len(parent) == 0 {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, " is null"), nodeId, folderName, spaceNo)
	} else {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, "=$4"), nodeId, folderName, spaceNo, parent)
	}
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&id, &isFolder)
		checkErr(err)
		return true, id, isFolder
	}
	return false, nil, false
}

func FileOwnerFileExists(nodeId string, parent []byte, name string) (id []byte, isFolder bool, hash string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	id, isFolder, hash = fileOwnerFileExists(tx, nodeId, parent, name)
	checkErr(tx.Commit())
	commit = true
	return
}

func fileOwnerFileExists(tx *sql.Tx, nodeId string, parent []byte, name string) (id []byte, isFolder bool, hash string) {
	var rows *sql.Rows
	var err error
	sqlStr := "SELECT ID,FOLDER,HASH FROM FILE_OWNER where NODE_ID=$1 and NAME=$2 and PARENT_ID%s and REMOVED=false"
	if parent == nil || len(parent) == 0 {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, " is null"), nodeId, name)
	} else {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, "=$3"), nodeId, name, parent)
	}
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		var hashNullable sql.NullString
		err = rows.Scan(&id, &isFolder, &hashNullable)
		if hashNullable.Valid {
			hash = hashNullable.String
		}
		checkErr(err)
		return
	}
	return nil, false, ""
}

func fileOwnerBatchFileExists(tx *sql.Tx, nodeId string, spaceNo uint32, parent []byte, names []string) map[string]bool {
	sqlStr := "SELECT NAME,FOLDER FROM FILE_OWNER where NODE_ID=$1 and SPACE_NO=$2 and NAME in " + inClause(len(names), 3) + " and PARENT_ID%s and REMOVED=false"
	if len(parent) == 0 {
		sqlStr = fmt.Sprintf(sqlStr, " is null")
	} else {
		sqlStr = fmt.Sprintf(sqlStr, "=$"+strconv.Itoa(len(names)+3))
	}
	args := make([]interface{}, 2, len(names)+3)
	args[0], args[1] = nodeId, spaceNo
	for _, name := range names {
		args = append(args, name)
	}
	if len(parent) > 0 {
		args = append(args, parent)
	}
	rows, err := tx.Query(sqlStr, args...)
	checkErr(err)
	defer rows.Close()
	duplicate := make(map[string]bool, len(names))
	for rows.Next() {
		var name string
		var isFolder bool
		err = rows.Scan(&name, &isFolder)
		checkErr(err)
		duplicate[name] = isFolder
	}
	return duplicate

}

func saveFileOwner(tx *sql.Tx, nodeId string, isFolder bool, name string, spaceNo uint32, parent interface{}, modTime uint64, hash *sql.NullString, size uint64) []byte {
	var lastInsertId []byte
	err := tx.QueryRow("insert into FILE_OWNER(REMOVED,CREATION,LAST_MODIFIED,NODE_ID,FOLDER,NAME,SPACE_NO,PARENT_ID,MOD_TIME,HASH,SIZE) values (false,now(),now(),$1,$2,$3,$4,$5,$6,$7,$8) RETURNING ID", nodeId, isFolder, name, spaceNo, parent, time.Unix(int64(modTime), 0), hash, size).Scan(&lastInsertId)
	checkErr(err)
	return lastInsertId
}

func updateFileOwnerNewVersion(tx *sql.Tx, existId []byte, nodeId string, modTime uint64, hash string, size uint64) {
	stmt, err := tx.Prepare("update FILE_OWNER set MOD_TIME=$3,HASH=$4,SIZE=$5 where ID=$1 and NODE_ID=$2 and FOLDER=false")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(existId, nodeId, time.Unix(int64(modTime), 0), hash, size)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func FileOwnerMkFolders(interactive bool, nodeId string, spaceNo uint32, parent []byte, folders []string) (duplicateFileName []string, duplicateFolderName []string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	duplicate := fileOwnerMkFolders(tx, interactive, nodeId, spaceNo, parent, folders)
	checkErr(tx.Commit())
	commit = true
	duplicateFileName = make([]string, 0, len(duplicate))
	duplicateFolderName = make([]string, 0, len(duplicate))
	for k, v := range duplicate {
		if v {
			duplicateFolderName = append(duplicateFolderName, k)
		} else {
			duplicateFileName = append(duplicateFileName, k)
		}
	}
	return
}

func fileOwnerMkFolders(tx *sql.Tx, interactive bool, nodeId string, spaceNo uint32, parent []byte, folders []string) (duplicate map[string]bool) {
	duplicate = fileOwnerBatchFileExists(tx, nodeId, spaceNo, parent, folders)
	if !interactive || len(duplicate) == 0 {
		modTime := uint64(time.Now().Unix())
		hash := &sql.NullString{}
		var parentId interface{} = nil
		if len(parent) > 0 {
			parentId = parent
		}
		for _, name := range folders {
			if _, ok := duplicate[name]; !ok {
				saveFileOwner(tx, nodeId, true, name, spaceNo, parentId, modTime, hash, 0)
			}
		}
	}
	return
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
	sqlStr := "SELECT ID,FOLDER,NAME,MOD_TIME,HASH,SIZE FROM FILE_OWNER where NODE_ID=$1 and PARENT_ID%s"
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
		var id []byte
		var isFolder bool
		var size uint64
		var modTime time.Time
		var hashStr sql.NullString
		var name string
		err = rows.Scan(&id, &isFolder, &name, &modTime, &hashStr, &size)
		checkErr(err)
		var hash []byte
		if hashStr.Valid {
			hash, err = base64.StdEncoding.DecodeString(hashStr.String)
			if err != nil {
				panic(err)
			}
		}
		res = append(res, &Fof{Id: id, IsFolder: isFolder, Name: name, ModTime: uint64(modTime.Unix()), FileHash: hash, FileSize: size})
	}
	return res
}

type Fof struct {
	Id       []byte
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

func fileOwnerCheckId(tx *sql.Tx, id []byte, spaceNo uint32) (nodeId string, isFolder bool) {
	rows, err := tx.Query("SELECT NODE_ID,FOLDER FROM FILE_OWNER where ID=$1 and SPACE_NO=$2 and REMOVED=false", id, spaceNo)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&nodeId, &isFolder)
		checkErr(err)
		return
	}
	return "", false
}

func FileOwnerCheckId(id []byte, spaceNo uint32) (nodeId string, isFolder bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	nodeId, isFolder = fileOwnerCheckId(tx, id, spaceNo)
	checkErr(tx.Commit())
	commit = true
	return
}
