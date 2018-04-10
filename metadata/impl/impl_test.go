package impl

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"testing"
	"time"

	pb "github.com/samoslab/nebula/tracker/metadata/pb"
	util_hash "github.com/samoslab/nebula/util/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMkFolder(t *testing.T) {
	assert := assert.New(t)
	priKey, err := rsa.GenerateKey(rand.Reader, 256*8)
	if err != nil {
		t.Errorf("failed")
	}
	pubKey := &priKey.PublicKey
	pubKeyBytes := x509.MarshalPKCS1PublicKey(pubKey)
	nodeId := util_hash.Sha1(pubKeyBytes)
	nodeIdStr := base64.StdEncoding.EncodeToString(nodeId)
	mockDao := new(daoMock)
	ts := uint64(time.Now().Unix())
	path := "/folder1/folder2"
	folders := []string{"f1", "f2"}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ms := &MatadataService{d: mockDao}
	resp, err := ms.MkFolder(ctx, &pb.MkFolderReq{
		Timestamp: ts,
		Path:      path,
		Folder:    folders})
	assert.Equal(uint32(100), resp.Code)

	resp, err = ms.MkFolder(ctx, &pb.MkFolderReq{NodeId: []byte("test"),
		Timestamp: ts,
		Path:      path,
		Folder:    folders})
	assert.Equal(uint32(101), resp.Code)

	mockDao.On("ClientGetPubKey", nodeId).Return(nil)
	resp, err = ms.MkFolder(ctx, &pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Path:      path,
		Folder:    folders})
	assert.Equal(uint32(102), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	resp, err = ms.MkFolder(ctx, &pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts - 16,
		Path:      path,
		Folder:    folders})

	assert.Equal(uint32(4), resp.Code)
	mockDao.AssertExpectations(t)

	resp, err = ms.MkFolder(ctx, &pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Path:      path,
		Folder:    folders})
	assert.Equal(uint32(5), resp.Code)
	mockDao.AssertExpectations(t)

	req := pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Path:      "aa",
		Folder:    folders}
	req.SignReq(priKey)
	resp, err = ms.MkFolder(ctx, &req)
	assert.Equal(uint32(200), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, path).Return(false, nil)
	req = pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Path:      path,
		Folder:    folders}
	req.SignReq(priKey)
	resp, err = ms.MkFolder(ctx, &req)
	assert.Equal(uint32(201), resp.Code)
	mockDao.AssertExpectations(t)

	parentId := []byte("test-folder-id")
	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, path).Return(true, parentId)
	mockDao.On("FileOwnerMkFolders", nodeIdStr, parentId, folders).Return(folders[1])
	req = pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Path:      path,
		Folder:    folders}
	req.SignReq(priKey)
	resp, err = ms.MkFolder(ctx, &req)
	assert.Equal(uint32(8), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, path).Return(true, parentId)
	mockDao.On("FileOwnerMkFolders", nodeIdStr, parentId, folders).Return("")
	req = pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Path:      path,
		Folder:    folders}
	req.SignReq(priKey)
	resp, err = ms.MkFolder(ctx, &req)
	assert.Equal(uint32(0), resp.Code)
	mockDao.AssertExpectations(t)
}

func TestCheckFileExist(t *testing.T) {
	assert := assert.New(t)
	priKey, err := rsa.GenerateKey(rand.Reader, 256*8)
	if err != nil {
		t.Errorf("failed")
	}
	pubKey := &priKey.PublicKey
	pubKeyBytes := x509.MarshalPKCS1PublicKey(pubKey)
	nodeId := util_hash.Sha1(pubKeyBytes)
	nodeIdStr := base64.StdEncoding.EncodeToString(nodeId)
	ts := uint64(time.Now().Unix())
	path := "/folder1/folder2"
	hash := util_hash.Sha1([]byte("test-file"))
	hashStr := base64.StdEncoding.EncodeToString(hash)
	size := uint64(98234)
	name := "file.txt"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	parentId := []byte("test-folder-id")

	mockDao := new(daoMock)
	ms := &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, path).Return(true, parentId)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true)
	req := pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		FilePath:    path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: true,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err := ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(8), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao.On("FileCheckExist", hashStr).Return(true, false, false, true, size)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		FilePath:    path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(9), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, path).Return(true, parentId)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true)
	mockDao.On("FileCheckExist", hashStr).Return(true, true, false, false, size)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		FilePath:    path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(10), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, path).Return(true, parentId)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true)
	mockDao.On("FileCheckExist", hashStr).Return(true, true, false, true, size)
	mockDao.On("FileReuse", nodeIdStr, hashStr, mock.Anything, size, ts-1000, parentId)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		FilePath:    path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(0), resp.Code)
	mockDao.AssertExpectations(t)

	size = 8000

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, path).Return(true, parentId)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true)
	mockDao.On("FileCheckExist", hashStr).Return(false, true, false, true, size)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		FilePath:    path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(11), resp.Code)
	mockDao.AssertExpectations(t)

	size = uint64(len(hash))
	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, path).Return(true, parentId)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true)
	mockDao.On("FileCheckExist", hashStr).Return(false, true, false, true, size)
	mockDao.On("FileSaveTiny", nodeIdStr, hashStr, hash, mock.Anything, size, ts-1000, parentId).Return(false, true, false, true, size)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		FilePath:    path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(0), resp.Code)
	mockDao.AssertExpectations(t)

	size = 9233320
	mockDao = new(daoMock)
	mockChooser := new(chooserMock)
	ms = &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, path).Return(true, parentId)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true)
	mockDao.On("FileCheckExist", hashStr).Return(false, true, false, true, size)
	mockChooser.On("Count").Return(12)
	mockDao.On("FileSaveStep1", nodeIdStr, hashStr, size, uint64(0))
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		FilePath:    path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(1), resp.Code)
	assert.Equal(uint32(8), resp.DataPieceCount)
	assert.Equal(uint32(4), resp.VerifyPieceCount)
	assert.Equal(pb.FileStoreType_ErasureCode, resp.StoreType)
	mockDao.AssertExpectations(t)
	// TODO FileStoreType_ErasureCode or FileStoreType_MultiReplica
}
