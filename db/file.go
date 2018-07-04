package db

import (
	"database/sql"
	"errors"
	"time"
)

func FileCheckExist(nodeId string, hash string, spaceNo uint32, doneExpSecs int) (id []byte, active bool, done bool, fileType string, size uint64, selfCreate bool, doneExpired bool) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	var removed bool
	var creatorNodeId string
	var lastModified time.Time
	id, active, removed, done, fileType, size, creatorNodeId, lastModified = fileCheckExist(tx, nodeId, hash, spaceNo)
	if len(id) > 0 {
		if creatorNodeId == nodeId {
			selfCreate = true
			if removed {
				fileChangeRemoved(tx, id, false)
			}
		} else {
			if time.Now().Unix()-lastModified.Unix() > int64(doneExpSecs) {
				doneExpired = true
				fileChangeCreatorNodeId(tx, id, nodeId)
			}
		}
	}
	checkErr(tx.Commit())
	commit = true
	return
}

func fileChangeCreatorNodeId(tx *sql.Tx, id []byte, nodeId string) {
	stmt, err := tx.Prepare("update FILE set CREATOR_NODE_ID=$2,REMOVED=false,LAST_MODIFIED=now() where ID=$1")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(id, nodeId)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func fileChangeRemoved(tx *sql.Tx, id []byte, removed bool) {
	stmt, err := tx.Prepare("update FILE set REMOVED=$2,LAST_MODIFIED=now() where ID=$1 and REMOVED=$3")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(id, removed, !removed)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func fileCheckExist(tx *sql.Tx, nodeId string, hash string, spaceNo uint32) (id []byte, active bool, removed bool, done bool, fileType string, size uint64, creatorNodeId string, lastModified time.Time) {
	if spaceNo > 0 {
		rows, err := tx.Query("SELECT ID,ACTIVE,REMOVED,DONE,TYPE,SIZE,CREATOR_NODE_ID,LAST_MODIFIED FROM FILE where HASH=$1 and PRIVATE=true and CREATOR_NODE_ID=$2", hash, nodeId)
		checkErr(err)
		defer rows.Close()
		for rows.Next() {
			cr := buildFileCheckRow(rows)
			return cr.fileCheckExistRes()
		}
		return
	} else {
		rows, err := tx.Query("SELECT ID,ACTIVE,REMOVED,DONE,TYPE,SIZE,CREATOR_NODE_ID,LAST_MODIFIED FROM FILE where HASH=$1 and PRIVATE=false and (INVALID=false or (INVALID=true and CREATOR_NODE_ID=$2)) and (SHARE=true or (SHARE=false and CREATOR_NODE_ID=$3))", hash, nodeId, nodeId)
		checkErr(err)
		defer rows.Close()
		crs := make([]*fileCheckRow, 0, 6)
		for rows.Next() {
			cr := buildFileCheckRow(rows)
			crs = append(crs, cr)
		}
		if len(crs) == 0 {
			return
		} else if len(crs) == 1 {
			return crs[0].fileCheckExistRes()
		}
		for _, cr := range crs {
			if cr.creatorNodeId == nodeId {
				return cr.fileCheckExistRes()
			}
		}
		return crs[0].fileCheckExistRes()
	}
}

func buildFileCheckRow(rows *sql.Rows) *fileCheckRow {
	cr := fileCheckRow{}
	err := rows.Scan(&cr.id, &cr.active, &cr.removed, &cr.done, &cr.fileType, &cr.size, &cr.creatorNodeId, &cr.lastModified)
	checkErr(err)
	return &cr
}

type fileCheckRow struct {
	id            []byte
	active        bool
	removed       bool
	done          bool
	fileType      sql.NullString
	size          uint64
	creatorNodeId string
	lastModified  time.Time
}

func (self *fileCheckRow) fileCheckExistRes() (id []byte, active bool, removed bool, done bool, fileType string, size uint64, creatorNodeId string, lastModified time.Time) {
	if self.fileType.Valid {
		fileType = self.fileType.String
	}
	return self.id, self.active, self.removed, self.done, fileType, self.size, self.creatorNodeId, self.lastModified
}

func incrementRefCount(tx *sql.Tx, id []byte) {
	stmt, err := tx.Prepare("update FILE set REF_COUNT=REF_COUNT+1,REMOVED=false,LAST_MODIFIED=now() where ID=$1")
	defer stmt.Close()
	checkErr(err)
	rs, err := stmt.Exec(id)
	checkErr(err)
	cnt, err := rs.RowsAffected()
	checkErr(err)
	if cnt == 0 {
		panic(errors.New("no record found"))
	}
}

func FileReuse(existId []byte, nodeId string, id []byte, hash string, name string, size uint64, modTime uint64, spaceNo uint32, parentId []byte, fileType string) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	incrementRefCount(tx, id)
	if len(existId) > 0 {
		updateFileOwnerNewVersion(tx, existId, nodeId, modTime, hash, size)
	} else {
		existId = saveFileOwner(tx, nodeId, false, name, spaceNo, parentId, fileType, modTime, &sql.NullString{Valid: true, String: hash}, size)
	}
	saveFileVersion(tx, existId, nodeId, hash, fileType)
	checkErr(tx.Commit())
	commit = true
}

func fileSave(tx *sql.Tx, nodeId string, hash string, encryptKey interface{}, fileType string, size uint64, fileData []byte, done bool, storeVolume uint64, private bool) {
	stmt, err := tx.Prepare("insert into FILE(HASH,CREATION,LAST_MODIFIED,ACTIVE,REMOVED,ENCRYPT_KEY,TYPE,SIZE,DATA,REF_COUNT,DONE,STORE_VOLUME,CREATOR_NODE_ID,PRIVATE,SHARE,INVALID) values ($1,now(),now(),true,false,$2,$3,$4,$5,1,$6,$7,$8,$9,$10,false)")
	defer stmt.Close()
	checkErr(err)
	_, err = stmt.Exec(hash, encryptKey, fileType, size, fileData, done, storeVolume, nodeId, private, !private)
	checkErr(err)
}

func FileSaveTiny(existId []byte, nodeId string, hash string, fileData []byte, name string, size uint64, modTime uint64, spaceNo uint32, parentId []byte, fileType string, encryptKey []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	fileSave(tx, nodeId, hash, encryptKey, fileType, size, fileData, true, size*3, spaceNo > 0)
	if len(existId) > 0 {
		updateFileOwnerNewVersion(tx, existId, nodeId, modTime, hash, size)
	} else {
		existId = saveFileOwner(tx, nodeId, false, name, spaceNo, parentId, fileType, modTime, &sql.NullString{Valid: true, String: hash}, size)
	}
	saveFileVersion(tx, existId, nodeId, hash, fileType)
	checkErr(tx.Commit())
	commit = true
}

func FileSaveStep1(nodeId string, hash string, fileType string, size uint64, storeVolume uint64, spaceNo uint32) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	fileSave(tx, nodeId, hash, nil, fileType, size, nil, false, storeVolume, spaceNo > 0)
	checkErr(tx.Commit())
	commit = true
}

func fileSaveDone(tx *sql.Tx, nodeId string, hash string, partitionCount int, blocks []string, storeVolume uint64, fileType string, encryptKey interface{}) {
	stmt, err := tx.Prepare("update FILE set PARTITION_COUNT=$3,BLOCKS=" + arrayClause(len(blocks), 7) + ",DONE=true,LAST_MODIFIED=now(),STORE_VOLUME=$4,TYPE=$5,ENCRYPT_KEY=$6 where HASH=$1 and CREATOR_NODE_ID=$2 and DONE=false")
	defer stmt.Close()
	checkErr(err)
	args := make([]interface{}, 6, len(blocks)+6)
	args[0], args[1], args[2], args[3], args[4], args[5] = hash, nodeId, partitionCount, storeVolume, fileType, encryptKey
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
	fileSaveDone(tx, nodeId, hash, partitionCount, blocks, storeVolume, fileType, encryptKey)
	if len(existId) > 0 {
		updateFileOwnerNewVersion(tx, existId, nodeId, modTime, hash, size)
	} else {
		existId = saveFileOwner(tx, nodeId, false, name, spaceNo, parentId, fileType, modTime, &sql.NullString{Valid: true, String: hash}, size)
	}
	saveFileVersion(tx, existId, nodeId, hash, fileType)
	checkErr(tx.Commit())
	commit = true
}
func buildFileRetrieveRow(rows *sql.Rows) *fileRetrieveRow {
	rr := fileRetrieveRow{}
	err := rows.Scan(&rr.id, &rr.active, &rr.fileData, &rr.partitionCount, &rr.blocks, &rr.size, &rr.fileType, &rr.encryptKey, &rr.creatorNodeId)
	checkErr(err)
	return &rr
}

type fileRetrieveRow struct {
	id             []byte
	active         bool
	fileData       []byte
	partitionCount int
	blocks         NullStrSlice
	size           uint64
	fileType       sql.NullString
	encryptKey     []byte
	creatorNodeId  string
}

func (self *fileRetrieveRow) fileRetrieveRes() (id []byte, active bool, fileData []byte, partitionCount int, blocks []string, size uint64, fileType string, encryptKey []byte) {
	if self.blocks.Valid {
		blocks = self.blocks.StrSlice
	}
	if self.fileType.Valid {
		fileType = self.fileType.String
	}
	return self.id, self.active, self.fileData, self.partitionCount, blocks, self.size, fileType, self.encryptKey
}
func fileRetrieve(tx *sql.Tx, nodeId string, hash string, spaceNo uint32) (id []byte, active bool, fileData []byte, partitionCount int, blocks []string, size uint64, fileType string, encryptKey []byte) {
	if spaceNo > 0 {
		rows, err := tx.Query("SELECT ID,ACTIVE,DATA,PARTITION_COUNT,BLOCKS,SIZE,TYPE,ENCRYPT_KEY,CREATOR_NODE_ID FROM FILE where HASH=$1 and PRIVATE=true and CREATOR_NODE_ID=$2 and DONE=true", hash, nodeId)
		checkErr(err)
		defer rows.Close()
		for rows.Next() {
			rr := buildFileRetrieveRow(rows)
			return rr.fileRetrieveRes()
		}
		return
	} else {
		rows, err := tx.Query("SELECT ID,ACTIVE,DATA,PARTITION_COUNT,BLOCKS,SIZE,TYPE,ENCRYPT_KEY,CREATOR_NODE_ID FROM FILE where HASH=$1 and PRIVATE=false and DONE=true and (INVALID=false or (INVALID=true and CREATOR_NODE_ID=$2)) and (SHARE=true or (SHARE=false and CREATOR_NODE_ID=$3))", hash, nodeId, nodeId)
		checkErr(err)
		defer rows.Close()
		rrs := make([]*fileRetrieveRow, 0, 6)
		for rows.Next() {
			rr := buildFileRetrieveRow(rows)
			rrs = append(rrs, rr)
		}
		if len(rrs) == 0 {
			return
		} else if len(rrs) == 1 {
			return rrs[0].fileRetrieveRes()
		}
		for _, rr := range rrs {
			if rr.creatorNodeId == nodeId {
				return rr.fileRetrieveRes()
			}
		}
		return rrs[0].fileRetrieveRes()
	}
}

func FileRetrieve(nodeId string, hash string, spaceNo uint32) (exist bool, active bool, fileData []byte, partitionCount int, blocks []string, size uint64, fileType string, encryptKey []byte) {
	tx, commit := beginTx()
	defer rollback(tx, &commit)
	var id []byte
	id, active, fileData, partitionCount, blocks, size, fileType, encryptKey = fileRetrieve(tx, nodeId, hash, spaceNo)
	exist = len(id) > 0
	checkErr(tx.Commit())
	commit = true
	return
}
