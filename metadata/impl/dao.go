package impl

import (
	"crypto/rsa"
	"nebula-tracker/db"
)

type dao interface {
	FileOwnerMkFolders(nodeId string, parent []byte, folders []string) (firstDuplicationName string)
	ClientGetPubKey(nodeId []byte) *rsa.PublicKey
	FileOwnerFileExists(nodeId string, parent []byte, name string) (id []byte, isFolder bool)
	FileCheckExist(hash string) (exist bool, active bool, removed bool, done bool, size uint64)
	FileReuse(nodeId string, hash string, name string, size uint64, modTime uint64, parentId []byte)
	FileSaveTiny(nodeId string, hash string, fileData []byte, name string, size uint64, modTime uint64, parentId []byte)
	FileSaveStep1(nodeId string, hash string, size uint64, storeVolume uint64)
	FileSaveDone(nodeId string, hash string, name string, size uint64, modTime uint64, parentId []byte, partitionCount int, blocks []string, storeVolume uint64)
	FileOwnerListOfPath(nodeId string, parentId []byte, pageSize uint32, pageNum uint32, sortField string, asc bool) (total uint32, fofs []*db.Fof)
	FileRetrieve(hash string) (exist bool, active bool, fileData []byte, partitionCount int, blocks []string, size uint64)
	ProviderFindOne(nodeId string) (p *db.ProviderInfo)
	FileOwnerRemove(nodeId string, pathId []byte, recursive bool) (res bool)
	FileOwnerIdOfFilePath(nodeId string, path string) (found bool, id []byte)
}
type daoImpl struct {
}

func (self *daoImpl) FileOwnerMkFolders(nodeId string, parent []byte, folders []string) (firstDuplicationName string) {
	return db.FileOwnerMkFolders(nodeId, parent, folders)
}
func (self *daoImpl) ClientGetPubKey(nodeId []byte) *rsa.PublicKey {
	return db.ClientGetPubKey(nodeId)
}
func (self *daoImpl) FileOwnerFileExists(nodeId string, parent []byte, name string) (id []byte, isFolder bool) {
	return db.FileOwnerFileExists(nodeId, parent, name)
}
func (self *daoImpl) FileCheckExist(hash string) (exist bool, active bool, removed bool, done bool, size uint64) {
	return db.FileCheckExist(hash)
}
func (self *daoImpl) FileReuse(nodeId string, hash string, name string, size uint64, modTime uint64, parentId []byte) {
	db.FileReuse(nodeId, hash, name, size, modTime, parentId)
}
func (self *daoImpl) FileSaveTiny(nodeId string, hash string, fileData []byte, name string, size uint64, modTime uint64, parentId []byte) {
	db.FileSaveTiny(nodeId, hash, fileData, name, size, modTime, parentId)
}
func (self *daoImpl) FileSaveStep1(nodeId string, hash string, size uint64, storeVolume uint64) {
	db.FileSaveStep1(nodeId, hash, size, storeVolume)
}
func (self *daoImpl) FileSaveDone(nodeId string, hash string, name string, size uint64, modTime uint64, parentId []byte, partitionCount int, blocks []string, storeVolume uint64) {
	db.FileSaveDone(nodeId, hash, name, size, modTime, parentId, partitionCount, blocks, storeVolume)
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
func (self *daoImpl) FileOwnerIdOfFilePath(nodeId string, path string) (found bool, id []byte) {
	return db.FileOwnerIdOfFilePath(nodeId, path)
}
