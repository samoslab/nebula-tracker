package impl

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"nebula-tracker/db"
	"testing"
	"time"

	pb "github.com/samoslab/nebula/tracker/metadata/pb"
	util_hash "github.com/samoslab/nebula/util/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	pathStr := "/folder1/folder2"
	path := &pb.FilePath{&pb.FilePath_Path{pathStr}}
	folders := []string{"f1", "f2"}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ms := &MatadataService{d: mockDao}
	resp, err := ms.MkFolder(ctx, &pb.MkFolderReq{
		Timestamp: ts,
		Parent:    path,
		Folder:    folders})
	assert.Equal(uint32(100), resp.Code)

	resp, err = ms.MkFolder(ctx, &pb.MkFolderReq{NodeId: []byte("test"),
		Timestamp: ts,
		Parent:    path,
		Folder:    folders})
	assert.Equal(uint32(101), resp.Code)

	mockDao.On("ClientGetPubKey", nodeId).Return(nil)

	resp, err = ms.MkFolder(ctx, &pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Parent:    path,
		Folder:    folders})
	assert.Equal(uint32(102), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	resp, err = ms.MkFolder(ctx, &pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts - 16,
		Parent:    path,
		Folder:    folders})

	assert.Equal(uint32(4), resp.Code)
	mockDao.AssertExpectations(t)

	resp, err = ms.MkFolder(ctx, &pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Parent:    path,
		Folder:    folders})
	assert.Equal(uint32(5), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("UsageAmount", nodeIdStr).Return(true, true, int64(1212), uint32(1024), uint32(3072), uint32(3072), uint32(3072), uint32(512), uint32(512), uint32(512), uint32(512), time.Now())
	req := pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Parent:    &pb.FilePath{&pb.FilePath_Path{"aa"}},
		Folder:    folders}
	req.SignReq(priKey)
	resp, err = ms.MkFolder(ctx, &req)
	assert.Equal(uint32(200), resp.Code)
	mockDao.AssertExpectations(t)

	req = pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Parent:    &pb.FilePath{&pb.FilePath_Path{"/aa"}}}
	req.SignReq(priKey)
	resp, err = ms.MkFolder(ctx, &req)
	assert.Equal(uint32(6), resp.Code)
	mockDao.AssertExpectations(t)

	req = pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Parent:    &pb.FilePath{&pb.FilePath_Path{"/aa"}},
		Folder:    []string{"bb", ""}}
	req.SignReq(priKey)
	resp, err = ms.MkFolder(ctx, &req)
	assert.Equal(uint32(7), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(false, nil, true)
	req = pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Parent:    path,
		Folder:    folders}
	req.SignReq(priKey)
	resp, err = ms.MkFolder(ctx, &req)
	assert.Equal(uint32(201), resp.Code)
	mockDao.AssertExpectations(t)

	parentId := []byte("test-folder-id")
	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("UsageAmount", nodeIdStr).Return(true, true, int64(1212), uint32(1024), uint32(3072), uint32(3072), uint32(3072), uint32(512), uint32(512), uint32(512), uint32(512), time.Now())
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, false)
	req = pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Parent:    path,
		Folder:    folders}
	req.SignReq(priKey)
	resp, err = ms.MkFolder(ctx, &req)
	assert.Equal(uint32(203), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("UsageAmount", nodeIdStr).Return(true, false, int64(1212), uint32(1024), uint32(3072), uint32(3072), uint32(3072), uint32(512), uint32(512), uint32(512), uint32(512), time.Now())
	req = pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Parent:    path,
		Folder:    folders}
	req.SignReq(priKey)
	resp, err = ms.MkFolder(ctx, &req)
	assert.Equal(uint32(400), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("UsageAmount", nodeIdStr).Return(false, true, int64(1212), uint32(1024), uint32(3072), uint32(3072), uint32(3072), uint32(512), uint32(512), uint32(512), uint32(512), time.Now())
	req = pb.MkFolderReq{NodeId: nodeId,
		Timestamp: ts,
		Parent:    path,
		Folder:    folders}
	req.SignReq(priKey)
	resp, err = ms.MkFolder(ctx, &req)
	assert.Equal(uint32(401), resp.Code)
	mockDao.AssertExpectations(t)

	// mockDao = new(daoMock)
	// ms = &MatadataService{d: mockDao}
	// mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	// mockDao.On("FileOwnerCheckId", parentId).Return("", false)
	// req = pb.MkFolderReq{NodeId: nodeId,
	// 	Timestamp: ts,
	// 	Parent:    &pb.FilePath{&pb.FilePath_Id{parentId}},
	// 	Folder:    folders}
	// req.SignReq(priKey)
	// resp, err = ms.MkFolder(ctx, &req)
	// assert.Equal(uint32(201), resp.Code)
	// mockDao.AssertExpectations(t)

	// mockDao = new(daoMock)
	// ms = &MatadataService{d: mockDao}
	// mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	// mockDao.On("FileOwnerCheckId", parentId).Return("other", false)
	// req = pb.MkFolderReq{NodeId: nodeId,
	// 	Timestamp: ts,
	// 	Parent:    &pb.FilePath{&pb.FilePath_Id{parentId}},
	// 	Folder:    folders}
	// req.SignReq(priKey)
	// resp, err = ms.MkFolder(ctx, &req)
	// assert.Equal(uint32(202), resp.Code)
	// mockDao.AssertExpectations(t)

	// mockDao = new(daoMock)
	// ms = &MatadataService{d: mockDao}
	// mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	// mockDao.On("FileOwnerCheckId", parentId).Return(nodeIdStr, false)
	// req = pb.MkFolderReq{NodeId: nodeId,
	// 	Timestamp: ts,
	// 	Parent:    &pb.FilePath{&pb.FilePath_Id{parentId}},
	// 	Folder:    folders}
	// req.SignReq(priKey)
	// resp, err = ms.MkFolder(ctx, &req)
	// assert.Equal(uint32(203), resp.Code)
	// mockDao.AssertExpectations(t)

	// mockDao = new(daoMock)
	// ms = &MatadataService{d: mockDao}
	// mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	// mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	// mockDao.On("FileOwnerMkFolders", false, nodeIdStr, parentId, folders).Return(nil, []string{folders[1]})
	// req = pb.MkFolderReq{NodeId: nodeId,
	// 	Timestamp: ts,
	// 	Parent:    path,
	// 	Folder:    folders}
	// req.SignReq(priKey)
	// resp, err = ms.MkFolder(ctx, &req)
	// assert.Equal(uint32(0), resp.Code)
	// mockDao.AssertExpectations(t)

	// mockDao = new(daoMock)
	// ms = &MatadataService{d: mockDao}
	// mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	// mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	// mockDao.On("FileOwnerMkFolders", true, nodeIdStr, parentId, folders).Return(nil, []string{folders[1]})
	// req = pb.MkFolderReq{NodeId: nodeId,
	// 	Timestamp:   ts,
	// 	Parent:      path,
	// 	Folder:      folders,
	// 	Interactive: true}
	// req.SignReq(priKey)
	// resp, err = ms.MkFolder(ctx, &req)
	// assert.Equal(uint32(8), resp.Code)
	// mockDao.AssertExpectations(t)

	// mockDao = new(daoMock)
	// ms = &MatadataService{d: mockDao}
	// mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	// mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	// mockDao.On("FileOwnerMkFolders", true, nodeIdStr, parentId, folders).Return([]string{"aaa.txt"}, []string{folders[1]})
	// req = pb.MkFolderReq{NodeId: nodeId,
	// 	Timestamp:   ts,
	// 	Parent:      path,
	// 	Folder:      folders,
	// 	Interactive: true}
	// req.SignReq(priKey)
	// resp, err = ms.MkFolder(ctx, &req)
	// assert.Equal(uint32(10), resp.Code)
	// mockDao.AssertExpectations(t)

	// mockDao = new(daoMock)
	// ms = &MatadataService{d: mockDao}
	// mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	// mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	// mockDao.On("FileOwnerMkFolders", true, nodeIdStr, parentId, folders).Return([]string{"aaa.txt"}, nil)
	// req = pb.MkFolderReq{NodeId: nodeId,
	// 	Timestamp:   ts,
	// 	Parent:      path,
	// 	Folder:      folders,
	// 	Interactive: true}
	// req.SignReq(priKey)
	// resp, err = ms.MkFolder(ctx, &req)
	// assert.Equal(uint32(9), resp.Code)
	// mockDao.AssertExpectations(t)

	// mockDao = new(daoMock)
	// ms = &MatadataService{d: mockDao}
	// mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	// mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	// mockDao.On("FileOwnerMkFolders", false, nodeIdStr, parentId, folders).Return([]string{"aaa.txt"}, nil)
	// req = pb.MkFolderReq{NodeId: nodeId,
	// 	Timestamp:   ts,
	// 	Parent:      path,
	// 	Folder:      folders,
	// 	Interactive: false}
	// req.SignReq(priKey)
	// resp, err = ms.MkFolder(ctx, &req)
	// assert.Equal(uint32(9), resp.Code)
	// mockDao.AssertExpectations(t)

	// mockDao = new(daoMock)
	// ms = &MatadataService{d: mockDao}
	// mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	// mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	// mockDao.On("FileOwnerMkFolders", false, nodeIdStr, parentId, folders).Return(nil, nil)
	// req = pb.MkFolderReq{NodeId: nodeId,
	// 	Timestamp: ts,
	// 	Parent:    path,
	// 	Folder:    folders}
	// req.SignReq(priKey)
	// resp, err = ms.MkFolder(ctx, &req)
	// assert.Equal(uint32(0), resp.Code)
	// mockDao.AssertExpectations(t)

	// var rootPathId []byte
	// mockDao = new(daoMock)
	// ms = &MatadataService{d: mockDao}
	// mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	// mockDao.On("FileOwnerMkFolders", false, nodeIdStr, rootPathId, folders).Return(nil, nil)
	// req = pb.MkFolderReq{NodeId: nodeId,
	// 	Timestamp: ts,
	// 	Parent:    &pb.FilePath{&pb.FilePath_Id{rootPathId}},
	// 	Folder:    folders}
	// req.SignReq(priKey)
	// resp, err = ms.MkFolder(ctx, &req)
	// assert.Equal(uint32(0), resp.Code)
	// mockDao.AssertExpectations(t)
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
	pathStr := "/folder1/folder2"
	path := &pb.FilePath{&pb.FilePath_Path{pathStr}}
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
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), false, "other")
	req := pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
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

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true, "")
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: true,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(12), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao.On("FileCheckExist", nodeIdStr, hashStr, 1800).Return(true, false, true, size, false, false)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
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
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true, "")
	mockDao.On("FileCheckExist", nodeIdStr, hashStr, 1800).Return(true, true, false, size, false, false)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
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
	mockChooser := new(chooserMock)
	ms = &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true, "")
	mockDao.On("FileCheckExist", nodeIdStr, hashStr, 1800).Return(true, true, false, size, true, false)
	mockChooser.On("Count").Return(2)
	mockChooser.On("Choose", uint32(2)).Return(mockProviderInfoSlice(2))
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(1), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	mockChooser = new(chooserMock)
	ms = &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true, "")
	mockDao.On("FileCheckExist", nodeIdStr, hashStr, 1800).Return(true, true, false, size, false, true)
	mockChooser.On("Count").Return(2)
	mockChooser.On("Choose", uint32(2)).Return(mockProviderInfoSlice(2))
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(1), resp.Code)
	mockDao.AssertExpectations(t)

	existId := []byte("exist-id")
	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return(existId, true, "")
	mockDao.On("FileCheckExist", nodeIdStr, hashStr, 1800).Return(true, true, true, size, false, false)
	mockDao.On("FileReuse", mock.Anything, nodeIdStr, hashStr, mock.Anything, size, ts-1000, parentId)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
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

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return(existId, false, "other")
	mockDao.On("FileCheckExist", nodeIdStr, hashStr, 1800).Return(true, true, true, size, false, false)
	mockDao.On("FileReuse", existId, nodeIdStr, hashStr, mock.Anything, size, ts-1000, parentId)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: false,
		NewVersion:  true}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(0), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return(existId, false, "other")
	mockDao.On("FileCheckExist", nodeIdStr, hashStr, 1800).Return(true, true, true, size, false, false)
	mockDao.On("FileReuse", mock.Anything, nodeIdStr, hashStr, mock.Anything, size, ts-1000, parentId)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
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

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return(existId, true, "")
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: true,
		NewVersion:  true}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(12), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return(existId, false, "other")
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		FileData:    hash,
		Interactive: true,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(8), resp.Code)
	mockDao.AssertExpectations(t)

	size = 8000

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true, "")
	mockDao.On("FileCheckExist", nodeIdStr, hashStr, 1800).Return(false, true, true, size, false, false)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
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
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true, "")
	mockDao.On("FileCheckExist", nodeIdStr, hashStr, 1800).Return(false, true, true, size, false, false)
	mockDao.On("FileSaveTiny", mock.Anything, nodeIdStr, hashStr, hash, mock.Anything, size, ts-1000, parentId).Return(false, true, false, true, size)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
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
	mockChooser = new(chooserMock)
	ms = &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true, "")
	mockDao.On("FileCheckExist", nodeIdStr, hashStr, 1800).Return(false, true, true, size, false, false)
	mockChooser.On("Count").Return(12)
	mockDao.On("FileSaveStep1", nodeIdStr, hashStr, size, uint64(0))
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
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
	mockChooser.AssertExpectations(t)

	size = 933320
	mockDao = new(daoMock)
	mockChooser = new(chooserMock)
	ms = &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), false, hashStr)
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(0), resp.Code)
	mockDao.AssertExpectations(t)
	mockChooser.AssertExpectations(t)

	size = 933320
	mockDao = new(daoMock)
	mockChooser = new(chooserMock)
	ms = &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true, "")
	mockDao.On("FileCheckExist", nodeIdStr, hashStr, 1800).Return(false, true, true, size, false, false)
	mockChooser.On("Count").Return(2)
	mockChooser.On("Choose", uint32(2)).Return(mockProviderInfoSlice(2))
	mockDao.On("FileSaveStep1", nodeIdStr, hashStr, size, uint64(0))
	req = pb.CheckFileExistReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.CheckFileExist(ctx, &req)
	assert.Equal(uint32(1), resp.Code)
	assert.Equal(uint32(2), resp.ReplicaCount)
	assert.Equal(pb.FileStoreType_MultiReplica, resp.StoreType)
	assert.Equal(2, len(resp.Provider))
	mockDao.AssertExpectations(t)
	mockChooser.AssertExpectations(t)
}

func mockProviderInfoSlice(count int) []db.ProviderInfo {
	slice := make([]db.ProviderInfo, 0, count)
	for i := 0; i < count; i++ {
		priKey, _ := rsa.GenerateKey(rand.Reader, 256*8)

		pubKey := &priKey.PublicKey
		pubKeyBytes := x509.MarshalPKCS1PublicKey(pubKey)
		nodeId := util_hash.Sha1(pubKeyBytes)
		nodeIdStr := base64.StdEncoding.EncodeToString(nodeId)
		slice = append(slice, db.ProviderInfo{NodeId: nodeIdStr,
			NodeIdBytes:       nodeId,
			PublicKey:         pubKeyBytes,
			BillEmail:         "test@test.com",
			EncryptKey:        []byte("test-encrypt-key"),
			WalletAddress:     "test-wallet-address",
			UpBandwidth:       2000000,
			DownBandwidth:     10000000,
			TestUpBandwidth:   2000000,
			TestDownBandwidth: 10000000,
			Availability:      0.98,
			Port:              6666,
			Host:              "127.0.0.1",
			StorageVolume:     []uint64{200000000000}})
	}
	return slice
}

func TestUploadFilePrepare(t *testing.T) {
	assert := assert.New(t)
	priKey, err := rsa.GenerateKey(rand.Reader, 256*8)
	if err != nil {
		t.Errorf("failed")
	}
	pubKey := &priKey.PublicKey
	pubKeyBytes := x509.MarshalPKCS1PublicKey(pubKey)
	nodeId := util_hash.Sha1(pubKeyBytes)
	// nodeIdStr := base64.StdEncoding.EncodeToString(nodeId)
	ts := uint64(time.Now().Unix())
	hash := util_hash.Sha1([]byte("test-file"))
	// hashStr := base64.StdEncoding.EncodeToString(hash)
	size := uint64(9998234)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := pb.UploadFilePrepareReq{NodeId: nodeId,
		Timestamp: ts,
		FileHash:  hash,
		FileSize:  size,
		Partition: nil}
	req.SignReq(priKey)
	mockDao := new(daoMock)
	mockChooser := new(chooserMock)
	ms := &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	resp, err := ms.UploadFilePrepare(ctx, &req)
	st, ok := status.FromError(err)
	if resp != nil || !ok || err == nil || st.Code() != codes.InvalidArgument || st.Message() != "partition data is required" {
		t.Error(err)
	}
	mockDao.AssertExpectations(t)

	req = pb.UploadFilePrepareReq{NodeId: nodeId,
		Timestamp: ts,
		FileHash:  hash,
		FileSize:  size,
		Partition: []*pb.SplitPartition{&pb.SplitPartition{}}}
	req.SignReq(priKey)
	mockDao = new(daoMock)
	mockChooser = new(chooserMock)
	ms = &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	resp, err = ms.UploadFilePrepare(ctx, &req)
	st, ok = status.FromError(err)
	if resp != nil || !ok || err == nil || st.Code() != codes.InvalidArgument || st.Message() != "piece data is required" {
		t.Error(err)
	}
	mockDao.AssertExpectations(t)

	req = pb.UploadFilePrepareReq{NodeId: nodeId,
		Timestamp: ts,
		FileHash:  hash,
		FileSize:  size,
		Partition: []*pb.SplitPartition{&pb.SplitPartition{Piece: []*pb.PieceHashAndSize{&pb.PieceHashAndSize{
			Hash: []byte("hash1"),
			Size: 1212121,
		},
			&pb.PieceHashAndSize{
				Hash: []byte("hash2"),
				Size: 1212121,
			},
			&pb.PieceHashAndSize{
				Hash: []byte("hash3"),
				Size: 1212121,
			},
		}}}}
	req.SignReq(priKey)
	mockDao = new(daoMock)
	mockChooser = new(chooserMock)
	ms = &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockChooser.On("Count").Return(2)
	resp, err = ms.UploadFilePrepare(ctx, &req)
	st, ok = status.FromError(err)
	if resp != nil || !ok || err == nil || st.Code() != codes.InvalidArgument || st.Message() != "not enough provider" {
		t.Error(err)
	}
	mockDao.AssertExpectations(t)
	mockChooser.AssertExpectations(t)

	req = pb.UploadFilePrepareReq{NodeId: nodeId,
		Timestamp: ts,
		FileHash:  hash,
		FileSize:  size,
		Partition: []*pb.SplitPartition{&pb.SplitPartition{Piece: []*pb.PieceHashAndSize{&pb.PieceHashAndSize{
			Hash: []byte("hash1"),
			Size: 1212121,
		},
			&pb.PieceHashAndSize{
				Hash: []byte("hash2"),
				Size: 1212121,
			},
			&pb.PieceHashAndSize{
				Hash: []byte("hash3"),
				Size: 1212121,
			},
		}}, &pb.SplitPartition{Piece: []*pb.PieceHashAndSize{&pb.PieceHashAndSize{
			Hash: []byte("hash11"),
			Size: 1212121,
		},
			&pb.PieceHashAndSize{
				Hash: []byte("hash12"),
				Size: 1212121,
			},
		}}}}
	req.SignReq(priKey)
	mockDao = new(daoMock)
	mockChooser = new(chooserMock)
	ms = &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockChooser.On("Count").Return(3)
	resp, err = ms.UploadFilePrepare(ctx, &req)
	st, ok = status.FromError(err)
	if resp != nil || !ok || err == nil || st.Code() != codes.InvalidArgument || st.Message() != "all parition must have same number piece" {
		t.Error(err)
	}
	mockDao.AssertExpectations(t)
	mockChooser.AssertExpectations(t)

	req = pb.UploadFilePrepareReq{NodeId: nodeId,
		Timestamp: ts,
		FileHash:  hash,
		FileSize:  size,
		Partition: []*pb.SplitPartition{&pb.SplitPartition{Piece: []*pb.PieceHashAndSize{&pb.PieceHashAndSize{
			Hash: []byte("hash1"),
			Size: 1212121,
		},
			&pb.PieceHashAndSize{
				Hash: []byte("hash2"),
				Size: 1212121,
			},
			&pb.PieceHashAndSize{
				Hash: []byte("hash3"),
				Size: 1212121,
			},
		}}}}
	req.SignReq(priKey)
	mockDao = new(daoMock)
	mockChooser = new(chooserMock)
	ms = &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockChooser.On("Count").Return(12)
	mockChooser.On("Choose", uint32(6)).Return(mockProviderInfoSlice(6))
	resp, err = ms.UploadFilePrepare(ctx, &req)
	if resp == nil || err != nil {
		t.Error(err)
	}
	mockDao.AssertExpectations(t)
	mockChooser.AssertExpectations(t)
	assert.Equal(1, len(resp.Partition))
	assert.Equal(6, len(resp.Partition[0].ProviderAuth))
	for i, pa := range resp.Partition[0].ProviderAuth {
		if i < 3 {
			assert.False(pa.Spare)
			assert.Equal(1, len(pa.HashAuth))
		} else {
			assert.True(pa.Spare)
			assert.Equal(2, len(pa.HashAuth))
		}
	}

	req = pb.UploadFilePrepareReq{NodeId: nodeId,
		Timestamp: ts,
		FileHash:  hash,
		FileSize:  size,
		Partition: []*pb.SplitPartition{&pb.SplitPartition{Piece: []*pb.PieceHashAndSize{&pb.PieceHashAndSize{
			Hash: []byte("hash1"),
			Size: 1212121,
		},
			&pb.PieceHashAndSize{
				Hash: []byte("hash2"),
				Size: 1212121,
			},
			&pb.PieceHashAndSize{
				Hash: []byte("hash3"),
				Size: 1212121,
			},
		}}}}
	req.SignReq(priKey)
	mockDao = new(daoMock)
	mockChooser = new(chooserMock)
	ms = &MatadataService{c: mockChooser, d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockChooser.On("Count").Return(3)
	mockChooser.On("Choose", uint32(3)).Return(mockProviderInfoSlice(3))
	resp, err = ms.UploadFilePrepare(ctx, &req)
	if resp == nil || err != nil {
		t.Error(err)
	}
	mockDao.AssertExpectations(t)
	mockChooser.AssertExpectations(t)
	assert.Equal(1, len(resp.Partition))
	assert.Equal(3, len(resp.Partition[0].ProviderAuth))
	for _, pa := range resp.Partition[0].ProviderAuth {
		assert.False(pa.Spare)
		assert.Equal(1, len(pa.HashAuth))
	}
	// t.Error(resp.Partition[0].ProviderAuth)
}

func TestUploadFileDone(t *testing.T) {
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
	pathStr := "/folder1/folder2"
	path := &pb.FilePath{&pb.FilePath_Path{pathStr}}
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
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true, "")
	req := pb.UploadFileDoneReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		Partition:   nil,
		Interactive: true,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err := ms.UploadFileDone(ctx, &req)
	assert.Equal(uint32(12), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), false, "other")
	req = pb.UploadFileDoneReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		Partition:   nil,
		Interactive: true,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.UploadFileDone(ctx, &req)
	assert.Equal(uint32(8), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true, "")
	req = pb.UploadFileDoneReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		Partition:   nil,
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.UploadFileDone(ctx, &req)
	assert.Equal(uint32(9), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return([]byte("exist-id"), true, "")
	req = pb.UploadFileDoneReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		Partition:   []*pb.StorePartition{&pb.StorePartition{Block: []*pb.StoreBlock{}}},
		Interactive: false,
		NewVersion:  false}
	req.SignReq(priKey)
	resp, err = ms.UploadFileDone(ctx, &req)
	assert.Equal(uint32(9), resp.Code)
	mockDao.AssertExpectations(t)

	existId := []byte("exist-id")
	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return(existId, false, hashStr)
	req = pb.UploadFileDoneReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		Partition: []*pb.StorePartition{&pb.StorePartition{Block: []*pb.StoreBlock{&pb.StoreBlock{Hash: []byte("test-hash1"),
			Size:        123123,
			BlockSeq:    1,
			Checksum:    false,
			StoreNodeId: [][]byte{[]byte("test-node-id1"), []byte("test-node-id2")},
		}, &pb.StoreBlock{Hash: []byte("test-hash2"),
			Size:        123123,
			BlockSeq:    2,
			Checksum:    false,
			StoreNodeId: [][]byte{[]byte("test-node-id1"), []byte("test-node-id2")},
		}, &pb.StoreBlock{Hash: []byte("test-hash3"),
			Size:        123123,
			BlockSeq:    1,
			Checksum:    true,
			StoreNodeId: [][]byte{[]byte("test-node-id1"), []byte("test-node-id2")},
		},
		}}},
		Interactive: false,
		NewVersion:  true}
	req.SignReq(priKey)
	resp, err = ms.UploadFileDone(ctx, &req)
	assert.Equal(uint32(14), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, parentId, true)
	mockDao.On("FileOwnerFileExists", nodeIdStr, parentId, name).Return(existId, false, "other")
	mockDao.On("FileSaveDone", existId, nodeIdStr, hashStr, mock.Anything, size, ts-1000, parentId, 1, mock.Anything, mock.Anything)
	req = pb.UploadFileDoneReq{NodeId: nodeId,
		Timestamp:   ts,
		Parent:      path,
		FileHash:    hash,
		FileSize:    size,
		FileName:    name,
		FileModTime: ts - 1000,
		Partition: []*pb.StorePartition{&pb.StorePartition{Block: []*pb.StoreBlock{&pb.StoreBlock{Hash: []byte("test-hash1"),
			Size:        123123,
			BlockSeq:    1,
			Checksum:    false,
			StoreNodeId: [][]byte{[]byte("test-node-id1"), []byte("test-node-id2")},
		}, &pb.StoreBlock{Hash: []byte("test-hash2"),
			Size:        123123,
			BlockSeq:    2,
			Checksum:    false,
			StoreNodeId: [][]byte{[]byte("test-node-id1"), []byte("test-node-id2")},
		}, &pb.StoreBlock{Hash: []byte("test-hash3"),
			Size:        123123,
			BlockSeq:    1,
			Checksum:    true,
			StoreNodeId: [][]byte{[]byte("test-node-id1"), []byte("test-node-id2")},
		},
		}}},
		Interactive: false,
		NewVersion:  true}
	req.SignReq(priKey)
	resp, err = ms.UploadFileDone(ctx, &req)
	assert.Equal(uint32(0), resp.Code)
	mockDao.AssertExpectations(t)

}

func TestRetrieveFile(t *testing.T) {
	assert := assert.New(t)
	priKey, err := rsa.GenerateKey(rand.Reader, 256*8)
	if err != nil {
		t.Errorf("failed")
	}
	pubKey := &priKey.PublicKey
	pubKeyBytes := x509.MarshalPKCS1PublicKey(pubKey)
	nodeId := util_hash.Sha1(pubKeyBytes)
	ts := uint64(time.Now().Unix())
	hash := util_hash.Sha1([]byte("test-file"))
	hashStr := base64.StdEncoding.EncodeToString(hash)
	size := uint64(98234)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mockDao := new(daoMock)
	ms := &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileRetrieve", hashStr).Return(false, false, nil, 0, nil, uint64(0))
	req := pb.RetrieveFileReq{NodeId: nodeId,
		Timestamp: ts,
		FileHash:  hash,
		FileSize:  size}
	req.SignReq(priKey)
	resp, err := ms.RetrieveFile(ctx, &req)
	assert.Equal(uint32(6), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileRetrieve", hashStr).Return(true, false, nil, 0, nil, uint64(0))
	req = pb.RetrieveFileReq{NodeId: nodeId,
		Timestamp: ts,
		FileHash:  hash,
		FileSize:  size}
	req.SignReq(priKey)
	resp, err = ms.RetrieveFile(ctx, &req)
	assert.Equal(uint32(7), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileRetrieve", hashStr).Return(true, true, nil, 0, nil, size-1)
	req = pb.RetrieveFileReq{NodeId: nodeId,
		Timestamp: ts,
		FileHash:  hash,
		FileSize:  size}
	req.SignReq(priKey)
	resp, err = ms.RetrieveFile(ctx, &req)
	assert.Equal(uint32(8), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileRetrieve", hashStr).Return(true, true, hash, 0, nil, size)
	req = pb.RetrieveFileReq{NodeId: nodeId,
		Timestamp: ts,
		FileHash:  hash,
		FileSize:  size}
	req.SignReq(priKey)
	resp, err = ms.RetrieveFile(ctx, &req)
	assert.Equal(uint32(0), resp.Code)
	assert.Equal(hash, resp.FileData)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileRetrieve", hashStr).Return(true, true, nil, 1, []string{"dGVzdC1oYXNoMQ==;123123;1;0;dGVzdC1ub2RlLWlkMQ==,dGVzdC1ub2RlLWlkMg==", "dGVzdC1oYXNoMg==;123123;2;0;dGVzdC1ub2RlLWlkMQ==,dGVzdC1ub2RlLWlkMg==", "dGVzdC1oYXNoMw==;123123;1;1;dGVzdC1ub2RlLWlkMQ==,dGVzdC1ub2RlLWlkMg=="}, size)
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])

	req = pb.RetrieveFileReq{NodeId: nodeId,
		Timestamp: ts,
		FileHash:  hash,
		FileSize:  size}
	req.SignReq(priKey)
	resp, err = ms.RetrieveFile(ctx, &req)
	assert.Equal(uint32(0), resp.Code)
	mockDao.AssertExpectations(t)
}

func TestRemove(t *testing.T) {
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
	// hash := util_hash.Sha1([]byte("test-file"))
	// hashStr := base64.StdEncoding.EncodeToString(hash)
	// size := uint64(98234)
	pathStr := "/folder1/folder2"
	path := &pb.FilePath{&pb.FilePath_Path{pathStr}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mockDao := new(daoMock)
	ms := &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	req := pb.RemoveReq{NodeId: nodeId,
		Timestamp: ts,
		Target:    &pb.FilePath{&pb.FilePath_Path{"/"}},
		Recursive: false}
	req.SignReq(priKey)
	resp, err := ms.Remove(ctx, &req)
	assert.Equal(uint32(6), resp.Code)
	mockDao.AssertExpectations(t)

	pathId := []byte("path-id")
	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, pathId, true)
	mockDao.On("FileOwnerRemove", nodeIdStr, pathId, false).Return(false)
	req = pb.RemoveReq{NodeId: nodeId,
		Timestamp: ts,
		Target:    path,
		Recursive: false}
	req.SignReq(priKey)
	resp, err = ms.Remove(ctx, &req)
	assert.Equal(uint32(7), resp.Code)
	mockDao.AssertExpectations(t)

	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, pathId, true)
	mockDao.On("FileOwnerRemove", nodeIdStr, pathId, false).Return(true)
	req = pb.RemoveReq{NodeId: nodeId,
		Timestamp: ts,
		Target:    path,
		Recursive: false}
	req.SignReq(priKey)
	resp, err = ms.Remove(ctx, &req)
	assert.Equal(uint32(0), resp.Code)
	mockDao.AssertExpectations(t)

	path = &pb.FilePath{&pb.FilePath_Id{pathId}}
	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerCheckId", pathId).Return(nodeIdStr, false)
	mockDao.On("FileOwnerRemove", nodeIdStr, pathId, false).Return(true)
	req = pb.RemoveReq{NodeId: nodeId,
		Timestamp: ts,
		Target:    path,
		Recursive: false}
	req.SignReq(priKey)
	resp, err = ms.Remove(ctx, &req)
	assert.Equal(uint32(0), resp.Code)
	mockDao.AssertExpectations(t)
}

func TestListFiles(t *testing.T) {
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
	// hash := util_hash.Sha1([]byte("test-file"))
	// hashStr := base64.StdEncoding.EncodeToString(hash)
	// size := uint64(98234)
	pathStr := "/folder1/folder2"
	path := &pb.FilePath{&pb.FilePath_Path{pathStr}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mockDao := new(daoMock)
	ms := &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	req := pb.ListFilesReq{NodeId: nodeId,
		Timestamp: ts,
		Parent:    path,
		PageSize:  2500,
		PageNum:   1,
		SortType:  pb.SortType_Name,
		AscOrder:  true,
	}
	req.SignReq(priKey)
	resp, err := ms.ListFiles(ctx, &req)
	assert.Equal(uint32(5), resp.Code)

	pathId := []byte("path-id")
	mockDao = new(daoMock)
	ms = &MatadataService{d: mockDao}
	mockDao.On("ClientGetPubKey", nodeId).Return(pubKey)
	mockDao.On("FileOwnerIdOfFilePath", nodeIdStr, pathStr).Return(true, pathId, true)
	mockDao.On("FileOwnerListOfPath", nodeIdStr, mock.Anything, uint32(500), uint32(1), "NAME", true).Return(uint32(0), nil)
	req = pb.ListFilesReq{NodeId: nodeId,
		Timestamp: ts,
		Parent:    path,
		PageSize:  500,
		PageNum:   1,
		AscOrder:  true,
	}
	req.SignReq(priKey)
	resp, err = ms.ListFiles(ctx, &req)
	assert.Equal(uint32(0), resp.Code)
	mockDao.AssertExpectations(t)
}

func TestToRetrievePartition(t *testing.T) {
	assert := assert.New(t)
	priKey, err := rsa.GenerateKey(rand.Reader, 256*8)
	if err != nil {
		t.Errorf("failed")
	}
	pubKey := &priKey.PublicKey
	pubKeyBytes := x509.MarshalPKCS1PublicKey(pubKey)
	nodeId := util_hash.Sha1(pubKeyBytes)
	nodeIdStr := base64.StdEncoding.EncodeToString(nodeId)
	hash := util_hash.Sha1([]byte("test-file"))
	// hashStr := base64.StdEncoding.EncodeToString(hash)
	ts := uint64(time.Now().Unix())

	mockDao := new(daoMock)
	mockChooser := new(chooserMock)
	ms := &MatadataService{c: mockChooser, d: mockDao}
	blocks := []string{"6PH4p/r6lfeda015UsGUimTQQx4=;69632672;0;0;C4dbshTe5MGCwVzBTtl9kF2j/zs=", "z4cD2ZvO1Rm9iEbt6U6GKLYCc90=;69632672;1;0;FOO58qPQuakQqjoGr7s3soczexs=", "4oUhBG58oaTmeAyomggi2qoUoIU=;69632672;2;1;t4ofGQu2D6JaRmizTWqkJ1Eh6T0=", "yRctnsgmP3uHE5QOXneMgoBmPf8=;69632672;0;0;C4dbshTe5MGCwVzBTtl9kF2j/zs=", "7Tgc7U4ab6v8nJRw+WZu1yJbY0U=;69632672;1;0;FOO58qPQuakQqjoGr7s3soczexs=", "Ke1p6JLqLM7FX5+HM4CEd/SAcxc=;69632672;2;1;t4ofGQu2D6JaRmizTWqkJ1Eh6T0="}
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])
	mockDao.On("ProviderFindOne", mock.Anything).Return(&mockProviderInfoSlice(1)[0])
	partitions, err := ms.toRetrievePartition(nodeIdStr, hash, 823243, blocks, 2, ts)
	assert.Nil(err)
	assert.Equal(2, len(partitions))
	assert.Equal(3, len(partitions[0].Block))
	assert.Equal(3, len(partitions[1].Block))
	assert.Equal("6PH4p/r6lfeda015UsGUimTQQx4=", base64.StdEncoding.EncodeToString(partitions[0].Block[0].Hash))
	assert.Equal("z4cD2ZvO1Rm9iEbt6U6GKLYCc90=", base64.StdEncoding.EncodeToString(partitions[0].Block[1].Hash))
	assert.Equal("4oUhBG58oaTmeAyomggi2qoUoIU=", base64.StdEncoding.EncodeToString(partitions[0].Block[2].Hash))
	assert.Equal("yRctnsgmP3uHE5QOXneMgoBmPf8=", base64.StdEncoding.EncodeToString(partitions[1].Block[0].Hash))
	assert.Equal("7Tgc7U4ab6v8nJRw+WZu1yJbY0U=", base64.StdEncoding.EncodeToString(partitions[1].Block[1].Hash))
	assert.Equal("Ke1p6JLqLM7FX5+HM4CEd/SAcxc=", base64.StdEncoding.EncodeToString(partitions[1].Block[2].Hash))
	// t.Error(partitions)
}
