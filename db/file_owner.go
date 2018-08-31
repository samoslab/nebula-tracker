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

func FileOwnerIdOfFilePath(nodeId string, path string, spaceNo uint32) (found bool, parentId []byte, id []byte, isFolder bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	found, parentId, id, isFolder = queryIdRecursion(tx, nodeId, path, spaceNo)
	checkErr(tx.Commit())
	commit = true
	return
}

func queryIdRecursion(tx *sql.Tx, nodeId string, path string, spaceNo uint32) (found bool, parentId []byte, id []byte, isFolder bool) {
	if path[len(path)-1] == '/' {
		path = path[0 : len(path)-1]
	}
	paths := strings.Split(path[1:], slash)
	for _, p := range paths {
		parentId = id
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
	sqlStr := "SELECT ID,FOLDER FROM FILE_OWNER where NODE_ID=$1 and NAME=$2 and SPACE_NO=$3 and PARENT_ID%s and REMOVED=false"
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

const SpaceSysFilename = ".nebula"

func FileOwnerFileExists(nodeId string, spaceNo uint32, parent []byte, name string) (id []byte, isFolder bool, hash string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	id, isFolder, hash = fileOwnerFileExists(tx, nodeId, spaceNo, parent, name)
	checkErr(tx.Commit())
	commit = true
	return
}

func fileOwnerFileExists(tx *sql.Tx, nodeId string, spaceNo uint32, parent []byte, name string) (id []byte, isFolder bool, hash string) {
	var rows *sql.Rows
	var err error
	sqlStr := "SELECT ID,FOLDER,HASH FROM FILE_OWNER where NODE_ID=$1 and NAME=$2 and SPACE_NO=$3 and PARENT_ID%s and REMOVED=false"
	if parent == nil || len(parent) == 0 {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, " is null"), nodeId, name, spaceNo)
	} else {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, "=$4"), nodeId, name, spaceNo, parent)
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

func saveFileOwner(tx *sql.Tx, nodeId string, isFolder bool, name string, spaceNo uint32, parent []byte, fileType string, modTime uint64, hash *sql.NullString, size uint64) []byte {
	var parentId interface{} = nil
	if len(parent) > 0 {
		parentId = parent
	}
	var lastInsertId []byte
	err := tx.QueryRow("insert into FILE_OWNER(REMOVED,CREATION,LAST_MODIFIED,NODE_ID,FOLDER,NAME,SPACE_NO,PARENT_ID,TYPE,MOD_TIME,HASH,SIZE) values (false,now(),now(),$1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING ID", nodeId, isFolder, name, spaceNo, parentId, fileType, time.Unix(int64(modTime), 0), hash, size).Scan(&lastInsertId)
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
		for _, name := range folders {
			if _, ok := duplicate[name]; !ok {
				saveFileOwner(tx, nodeId, true, name, spaceNo, parent, "", modTime, hash, 0)
			}
		}
	}
	return
}

func fileOwnerListOfPathCount(tx *sql.Tx, nodeId string, spaceNo uint32, parent []byte) (total uint32) {
	var rows *sql.Rows
	var err error
	sqlStr := "SELECT count(1) FROM FILE_OWNER where NODE_ID=$1 and SPACE_NO=$2 and PARENT_ID%s and REMOVED=false"
	if parent == nil || len(parent) == 0 {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, " is null and NAME<>'"+SpaceSysFilename+"'"), nodeId, spaceNo)
	} else {
		rows, err = tx.Query(fmt.Sprintf(sqlStr, "=$3"), nodeId, spaceNo, parent)
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

func fileOwnerListOfPath(tx *sql.Tx, nodeId string, spaceNo uint32, parent []byte, pageSize uint32, pageNum uint32, sortField string, asc bool) []*Fof {
	var args []interface{}
	sqlStr := "SELECT ID,FOLDER,NAME,TYPE,MOD_TIME,HASH,SIZE FROM FILE_OWNER where NODE_ID=$1 and SPACE_NO=$2 and PARENT_ID%s"
	if parent == nil || len(parent) == 0 {
		sqlStr = fmt.Sprintf(sqlStr, " is null and NAME<>'"+SpaceSysFilename+"'")
		args = []interface{}{nodeId, spaceNo}
	} else {
		sqlStr = fmt.Sprintf(sqlStr, "=$3")
		args = []interface{}{nodeId, spaceNo, parent}
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
		var hashStr, typeNullable sql.NullString
		var name, fileType string
		err = rows.Scan(&id, &isFolder, &name, &typeNullable, &modTime, &hashStr, &size)
		checkErr(err)
		var hash []byte
		if hashStr.Valid {
			hash, err = base64.StdEncoding.DecodeString(hashStr.String)
			if err != nil {
				panic(err)
			}
		}
		if typeNullable.Valid {
			fileType = typeNullable.String
		}
		res = append(res, &Fof{Id: id, IsFolder: isFolder, Name: name, Type: fileType, ModTime: uint64(modTime.Unix()), FileHash: hash, FileSize: size})
	}
	return res
}

type Fof struct {
	Id       []byte
	IsFolder bool
	Name     string
	Type     string
	ModTime  uint64
	FileHash []byte
	FileSize uint64
}

func FileOwnerListOfPath(nodeId string, spaceNo uint32, parentId []byte, pageSize uint32, pageNum uint32, sortField string, asc bool) (total uint32, fofs []*Fof) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	if pageNum == 0 {
		pageNum = 1
	}
	total = fileOwnerListOfPathCount(tx, nodeId, spaceNo, parentId)
	if total == 0 || (pageNum-1)*pageSize >= total {
		return
	}
	fofs = fileOwnerListOfPath(tx, nodeId, spaceNo, parentId, pageSize, pageNum, sortField, asc)
	checkErr(tx.Commit())
	commit = true
	return
}

func FileOwnerRemove(nodeId string, spaceNo uint32, pathId []byte, recursive bool) (res bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	if recursive || fileOwnerListOfPathCount(tx, nodeId, spaceNo, pathId) == 0 {
		fileOwnerRemove(tx, nodeId, spaceNo, pathId)
		res = true
	}
	checkErr(tx.Commit())
	commit = true
	return
}

func fileOwnerRemove(tx *sql.Tx, nodeId string, spaceNo uint32, pathId []byte) {
	stmt, err := tx.Prepare("update FILE_OWNER set REMOVED=true where ID=$1 and SPACE_NO=$2 and NODE_ID=$3")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(pathId, spaceNo, nodeId)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func fileOwnerCheckId(tx *sql.Tx, id []byte, spaceNo uint32) (nodeId string, parentId []byte, isFolder bool) {
	rows, err := tx.Query("SELECT NODE_ID,PARENT_ID,FOLDER FROM FILE_OWNER where ID=$1 and SPACE_NO=$2 and REMOVED=false", id, spaceNo)
	checkErr(err)
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&nodeId, &parentId, &isFolder)
		checkErr(err)
		return
	}
	return
}

func FileOwnerCheckId(id []byte, spaceNo uint32) (nodeId string, parentId []byte, isFolder bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	nodeId, parentId, isFolder = fileOwnerCheckId(tx, id, spaceNo)
	checkErr(tx.Commit())
	commit = true
	return
}

func FileOwnerRename(nodeId string, id []byte, spaceNo uint32, newName string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	fileOwnerRename(tx, nodeId, id, spaceNo, newName)
	checkErr(tx.Commit())
	commit = true
}

func fileOwnerRename(tx *sql.Tx, nodeId string, id []byte, spaceNo uint32, newName string) {
	stmt, err := tx.Prepare("update FILE_OWNER set NAME=$3 where ID=$1 and SPACE_NO=$2 and NODE_ID=$4")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(id, spaceNo, newName, nodeId)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func FileOwnerMove(nodeId string, id []byte, spaceNo uint32, newId []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	fileOwnerMove(tx, nodeId, id, spaceNo, newId)
	checkErr(tx.Commit())
	commit = true
}

func fileOwnerMove(tx *sql.Tx, nodeId string, id []byte, spaceNo uint32, newId []byte) {
	stmt, err := tx.Prepare("update FILE_OWNER set PARENT_ID=$3 where ID=$1 and SPACE_NO=$2 and NODE_ID=$3")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(id, spaceNo, newId)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}
