package impl

import (
	"crypto/rsa"
	"nebula-tracker/db"
	"time"
)

type dao interface {
	FileOwnerMkFolders(interactive bool, nodeId string, parent []byte, folders []string) (duplicateFileName []string, duplicateFolderName []string)
	ClientGetPubKey(nodeId []byte) *rsa.PublicKey
	FileOwnerFileExists(nodeId string, parent []byte, name string) (id []byte, isFolder bool, hash string)
	FileCheckExist(nodeId string, hash string, doneExpSecs int) (exist bool, active bool, done bool, size uint64, selfCreate bool, doneExpired bool)
	FileReuse(existId []byte, nodeId string, hash string, name string, size uint64, modTime uint64, parentId []byte)
	FileSaveTiny(existId []byte, nodeId string, hash string, fileData []byte, name string, size uint64, modTime uint64, parentId []byte)
	FileSaveStep1(nodeId string, hash string, size uint64, storeVolume uint64)
	FileSaveDone(existId []byte, nodeId string, hash string, name string, size uint64, modTime uint64, parentId []byte, partitionCount int, blocks []string, storeVolume uint64)
	FileOwnerListOfPath(nodeId string, parentId []byte, pageSize uint32, pageNum uint32, sortField string, asc bool) (total uint32, fofs []*db.Fof)
	FileRetrieve(hash string) (exist bool, active bool, fileData []byte, partitionCount int, blocks []string, size uint64)
	ProviderFindOne(nodeId string) (p *db.ProviderInfo)
	FileOwnerRemove(nodeId string, pathId []byte, recursive bool) (res bool)
	FileOwnerIdOfFilePath(nodeId string, path string) (found bool, id []byte, isFolder bool)
	FileOwnerCheckId(id []byte) (nodeId string, isFolder bool)
	UsageAmount(nodeId string) (inService bool, emailVerified bool, packageId int64, volume uint32, netflow uint32, upNetflow uint32,
		downNetflow uint32, usageVolume uint32, usageNetflow uint32, usageUpNetflow uint32, usageDownNetflow uint32, endTime time.Time)
}
type daoImpl struct {
}

func (self *daoImpl) FileOwnerMkFolders(interactive bool, nodeId string, parent []byte, folders []string) (duplicateFileName []string, duplicateFolderName []string) {
	return db.FileOwnerMkFolders(interactive, nodeId, parent, folders)
}
func (self *daoImpl) ClientGetPubKey(nodeId []byte) *rsa.PublicKey {
	return db.ClientGetPubKey(nodeId)
}
func (self *daoImpl) FileOwnerFileExists(nodeId string, parent []byte, name string) (id []byte, isFolder bool, hash string) {
	return db.FileOwnerFileExists(nodeId, parent, name)
}
func (self *daoImpl) FileCheckExist(nodeId string, hash string, doneExpSecs int) (exist bool, active bool, done bool, size uint64, selfCreate bool, doneExpired bool) {
	return db.FileCheckExist(nodeId, hash, doneExpSecs)
}
func (self *daoImpl) FileReuse(existId []byte, nodeId string, hash string, name string, size uint64, modTime uint64, parentId []byte) {
	db.FileReuse(existId, nodeId, hash, name, size, modTime, parentId)
}
func (self *daoImpl) FileSaveTiny(existId []byte, nodeId string, hash string, fileData []byte, name string, size uint64, modTime uint64, parentId []byte) {
	db.FileSaveTiny(existId, nodeId, hash, fileData, name, size, modTime, parentId)
}
func (self *daoImpl) FileSaveStep1(nodeId string, hash string, size uint64, storeVolume uint64) {
	db.FileSaveStep1(nodeId, hash, size, storeVolume)
}
func (self *daoImpl) FileSaveDone(existId []byte, nodeId string, hash string, name string, size uint64, modTime uint64, parentId []byte, partitionCount int, blocks []string, storeVolume uint64) {
	db.FileSaveDone(existId, nodeId, hash, name, size, modTime, parentId, partitionCount, blocks, storeVolume)
}
func (self *daoImpl) FileOwnerListOfPath(nodeId string, parentId []byte, pageSize uint32, pageNum uint32, sortField string, asc bool) (total uint32, fofs []*db.Fof) {
	return db.FileOwnerListOfPath(nodeId, parentId, pageSize, pageNum, sortField, asc)
}
func (self *daoImpl) FileRetrieve(hash string) (exist bool, active bool, fileData []byte, partitionCount int, blocks []string, size uint64) {
	return db.FileRetrieve(hash)
}
func (self *daoImpl) ProviderFindOne(nodeId string) (p *db.ProviderInfo) {
	return db.ProviderFindOne(nodeId)
}
func (self *daoImpl) FileOwnerRemove(nodeId string, pathId []byte, recursive bool) (res bool) {
	return db.FileOwnerRemove(nodeId, pathId, recursive)
}
func (self *daoImpl) FileOwnerIdOfFilePath(nodeId string, path string) (found bool, id []byte, isFolder bool) {
	return db.FileOwnerIdOfFilePath(nodeId, path)
}
func (self *daoImpl) FileOwnerCheckId(id []byte) (nodeId string, isFolder bool) {
	return db.FileOwnerCheckId(id)
}
func (self *daoImpl) UsageAmount(nodeId string) (inService bool, emailVerified bool, packageId int64, volume uint32, netflow uint32, upNetflow uint32,
	downNetflow uint32, usageVolume uint32, usageNetflow uint32, usageUpNetflow uint32, usageDownNetflow uint32, endTime time.Time) {
	return db.UsageAmount(nodeId)
}
