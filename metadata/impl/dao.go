package impl

import (
	"crypto/rsa"
	"nebula-tracker/db"
	"time"
)

type dao interface {
	FileOwnerMkFolders(interactive bool, nodeId string, spaceNo uint32, parent []byte, folders []string) (duplicateFileName []string, duplicateFolderName []string)
	ClientGetPubKey(nodeId string) *rsa.PublicKey
	FileOwnerFileExists(nodeId string, spaceNo uint32, parent []byte, name string) (id []byte, isFolder bool, hash string)
	FileCheckExist(nodeId string, hash string, doneExpSecs int) (exist bool, active bool, done bool, fileType string, size uint64, selfCreate bool, doneExpired bool)
	FileReuse(existId []byte, nodeId string, hash string, name string, size uint64, modTime uint64, spaceNo uint32, parentId []byte, fileType string)
	FileSaveTiny(existId []byte, nodeId string, hash string, fileData []byte, name string, size uint64, modTime uint64, spaceNo uint32, parentId []byte, fileType string, encryptKey []byte)
	FileSaveStep1(nodeId string, hash string, fileType string, size uint64, storeVolume uint64)
	FileSaveDone(existId []byte, nodeId string, hash string, name string, fileType string, size uint64, modTime uint64, spaceNo uint32, parentId []byte, partitionCount int, blocks []string, storeVolume uint64, encryptKey []byte)
	FileOwnerListOfPath(nodeId string, spaceNo uint32, parentId []byte, pageSize uint32, pageNum uint32, sortField string, asc bool) (total uint32, fofs []*db.Fof)
	FileRetrieve(hash string) (exist bool, active bool, fileData []byte, partitionCount int, blocks []string, size uint64, fileType string, encryptKey []byte)
	ProviderFindOne(nodeId string) (p *db.ProviderInfo)
	FileOwnerRemove(nodeId string, spaceNo uint32, pathId []byte, recursive bool) (res bool)
	FileOwnerIdOfFilePath(nodeId string, path string, spaceNo uint32) (found bool, parentId []byte, id []byte, isFolder bool)
	FileOwnerCheckId(id []byte, spaceNo uint32) (nodeId string, parentId []byte, isFolder bool)
	FileOwnerRename(id []byte, spaceNo uint32, newName string)
	FileOwnerMove(id []byte, spaceNo uint32, newId []byte)
	UsageAmount(nodeId string) (inService bool, emailVerified bool, packageId int64, volume uint32, netflow uint32, upNetflow uint32,
		downNetflow uint32, usageVolume uint32, usageNetflow uint32, usageUpNetflow uint32, usageDownNetflow uint32, endTime time.Time)
}
type daoImpl struct {
}

func (self *daoImpl) FileOwnerMkFolders(interactive bool, nodeId string, spaceNo uint32, parent []byte, folders []string) (duplicateFileName []string, duplicateFolderName []string) {
	return db.FileOwnerMkFolders(interactive, nodeId, spaceNo, parent, folders)
}
func (self *daoImpl) ClientGetPubKey(nodeId string) *rsa.PublicKey {
	return db.ClientGetPubKey(nodeId)
}
func (self *daoImpl) FileOwnerFileExists(nodeId string, spaceNo uint32, parent []byte, name string) (id []byte, isFolder bool, hash string) {
	return db.FileOwnerFileExists(nodeId, spaceNo, parent, name)
}
func (self *daoImpl) FileCheckExist(nodeId string, hash string, doneExpSecs int) (exist bool, active bool, done bool, fileType string, size uint64, selfCreate bool, doneExpired bool) {
	return db.FileCheckExist(nodeId, hash, doneExpSecs)
}
func (self *daoImpl) FileReuse(existId []byte, nodeId string, hash string, name string, size uint64, modTime uint64, spaceNo uint32, parentId []byte, fileType string) {
	db.FileReuse(existId, nodeId, hash, name, size, modTime, spaceNo, parentId, fileType)
}
func (self *daoImpl) FileSaveTiny(existId []byte, nodeId string, hash string, fileData []byte, name string, size uint64, modTime uint64, spaceNo uint32, parentId []byte, fileType string, encryptKey []byte) {
	db.FileSaveTiny(existId, nodeId, hash, fileData, name, size, modTime, spaceNo, parentId, fileType, encryptKey)
}
func (self *daoImpl) FileSaveStep1(nodeId string, hash string, fileType string, size uint64, storeVolume uint64) {
	db.FileSaveStep1(nodeId, hash, fileType, size, storeVolume)
}
func (self *daoImpl) FileSaveDone(existId []byte, nodeId string, hash string, name string, fileType string, size uint64, modTime uint64, spaceNo uint32, parentId []byte, partitionCount int, blocks []string, storeVolume uint64, encryptKey []byte) {
	db.FileSaveDone(existId, nodeId, hash, name, fileType, size, modTime, spaceNo, parentId, partitionCount, blocks, storeVolume, encryptKey)
}
func (self *daoImpl) FileOwnerListOfPath(nodeId string, spaceNo uint32, parentId []byte, pageSize uint32, pageNum uint32, sortField string, asc bool) (total uint32, fofs []*db.Fof) {
	return db.FileOwnerListOfPath(nodeId, spaceNo, parentId, pageSize, pageNum, sortField, asc)
}
func (self *daoImpl) FileRetrieve(hash string) (exist bool, active bool, fileData []byte, partitionCount int, blocks []string, size uint64, fileType string, encryptKey []byte) {
	return db.FileRetrieve(hash)
}
func (self *daoImpl) ProviderFindOne(nodeId string) (p *db.ProviderInfo) {
	return db.ProviderFindOne(nodeId)
}
func (self *daoImpl) FileOwnerRemove(nodeId string, spaceNo uint32, pathId []byte, recursive bool) (res bool) {
	return db.FileOwnerRemove(nodeId, spaceNo, pathId, recursive)
}
func (self *daoImpl) FileOwnerIdOfFilePath(nodeId string, path string, spaceNo uint32) (found bool, parentId []byte, id []byte, isFolder bool) {
	return db.FileOwnerIdOfFilePath(nodeId, path, spaceNo)
}
func (self *daoImpl) FileOwnerCheckId(id []byte, spaceNo uint32) (nodeId string, parent []byte, isFolder bool) {
	return db.FileOwnerCheckId(id, spaceNo)
}
func (self *daoImpl) FileOwnerRename(id []byte, spaceNo uint32, newName string) {
	db.FileOwnerRename(id, spaceNo, newName)
}
func (self *daoImpl) FileOwnerMove(id []byte, spaceNo uint32, newId []byte) {
	db.FileOwnerMove(id, spaceNo, newId)
}
func (self *daoImpl) UsageAmount(nodeId string) (inService bool, emailVerified bool, packageId int64, volume uint32, netflow uint32, upNetflow uint32,
	downNetflow uint32, usageVolume uint32, usageNetflow uint32, usageUpNetflow uint32, usageDownNetflow uint32, endTime time.Time) {
	return db.UsageAmount(nodeId)
}
