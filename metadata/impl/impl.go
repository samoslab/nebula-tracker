package impl

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"nebula-tracker/db"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	log "github.com/sirupsen/logrus"
	pb "github.com/spolabs/nebula/tracker/metadata/pb"
	util_bytes "github.com/spolabs/nebula/util/bytes"
)

type MatadataService struct {
}

func NewMatadataService() *MatadataService {
	return &MatadataService{}
}

func verifySignMkFolderReq(req *pb.MkFolderReq, pubKey *rsa.PublicKey) error {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	hasher.Write([]byte(req.Path))
	for _, f := range req.Folder {
		hasher.Write([]byte(f))
	}
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
}

func (self *MatadataService) MkFolder(ctx context.Context, req *pb.MkFolderReq) (*pb.MkFolderResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.MkFolderResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if err := verifySignMkFolderReq(req, pubKey); err != nil {
		return &pb.MkFolderResp{Code: 5, ErrMsg: "Verify Sign failed"}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	resobj, parentId := findPathId(nodeIdStr, req.Path)
	if resobj != nil {
		return &pb.MkFolderResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	}
	firstDuplicationName := db.FileOwnerMkFolders(nodeIdStr, parentId, req.Folder)
	if firstDuplicationName != "" {
		return &pb.MkFolderResp{Code: 8, ErrMsg: "duplication of folder name: " + firstDuplicationName}, nil
	}
	return &pb.MkFolderResp{Code: 0}, nil
}

type resObj struct {
	Code   uint32
	ErrMsg string
}

func checkNodeId(nodeId []byte) (*resObj, *rsa.PublicKey) {
	if nodeId == nil {
		return &resObj{Code: 100, ErrMsg: "NodeId is required"}, nil
	}
	if len(nodeId) != 20 {
		return &resObj{Code: 101, ErrMsg: "NodeId length must be 20"}, nil
	}
	pubKey := db.ClientGetPubKey(nodeId)
	if pubKey == nil {
		return &resObj{Code: 102, ErrMsg: "this node id is not been registered"}, nil
	}
	return nil, pubKey
}

func verifySignCheckFileExistReq(req *pb.CheckFileExistReq, pubKey *rsa.PublicKey) error {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	hasher.Write([]byte(req.FilePath))
	hasher.Write(req.FileHash)
	hasher.Write(util_bytes.FromUint64(req.FileSize))
	hasher.Write([]byte(req.FileName))
	hasher.Write(util_bytes.FromUint64(req.FileModTime))
	hasher.Write(req.FileData)
	if req.Interactive {
		hasher.Write([]byte{1})
	} else {
		hasher.Write([]byte{0})
	}
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
}
func (self *MatadataService) CheckFileExist(ctx context.Context, req *pb.CheckFileExistReq) (*pb.CheckFileExistResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.CheckFileExistResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}

	if err := verifySignCheckFileExistReq(req, pubKey); err != nil {
		return &pb.CheckFileExistResp{Code: 5, ErrMsg: "Verify Sign failed"}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	resobj, parentId := findPathId(nodeIdStr, req.FilePath)
	if resobj != nil {
		return &pb.CheckFileExistResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	}
	fileName := req.FileName
	existId, _ := db.FileOwnerFileExists(nodeIdStr, parentId, req.FileName)
	if existId != nil {
		if req.Interactive {
			return &pb.CheckFileExistResp{Code: 8, ErrMsg: "exist same name file or folder"}, nil
		} else {
			fileName = fixFileName(req.FileName)
		}
	}
	//check available space
	hashStr := base64.StdEncoding.EncodeToString(req.FileHash)
	exist, active, _, done, size := db.FileCheckExist(hashStr)
	if exist {
		if size != req.FileSize {
			log.Warnf("hash: %s size is %d, new upload file size is %d", hashStr, size, req.FileSize)
		}
		if !active {
			return &pb.CheckFileExistResp{Code: 9, ErrMsg: "this file can not upload Because of laws and regulations"}, nil
		}
		if !done {
			return &pb.CheckFileExistResp{Code: 10, ErrMsg: "this file is being uploaded by other user, please wait a moment to retry"}, nil
		}
		db.FileReuse(nodeIdStr, hashStr, fileName, req.FileSize, req.FileModTime, parentId)
		return &pb.CheckFileExistResp{Code: 0}, nil
	} else {
		if req.FileSize <= embed_metadata_max_file_size {
			if req.FileSize != uint64(len(req.FileData)) {
				return &pb.CheckFileExistResp{Code: 11, ErrMsg: "file data size is not equal fileSize"}, nil
			}
			db.FileSaveTiny(nodeIdStr, hashStr, req.FileData, fileName, req.FileSize, req.FileModTime, parentId)
			return &pb.CheckFileExistResp{Code: 0}, nil
		}
		resp := pb.CheckFileExistResp{Code: 1}
		if req.FileSize <= multi_replica_max_file_size {
			resp.StoreType = pb.FileStoreType_MultiReplica
			resp.ReplicaCount = 5
			// TODO set provider
			db.FileSaveStep1(nodeIdStr, hashStr, req.FileSize, 5*req.FileSize)
		} else {
			resp.StoreType = pb.FileStoreType_ErasureCode
			resp.DataPieceCount = 16  // TODO
			resp.VerifyPieceCount = 8 // TODO
			db.FileSaveStep1(nodeIdStr, hashStr, req.FileSize, 0)
		}
		return &resp, nil
	}
}

const embed_metadata_max_file_size = 8192

const multi_replica_max_file_size = 1024 * 1024

func fixFileName(name string) string {
	pos := strings.LastIndex(name, ".")
	if pos == -1 {
		return name + "_" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	return name[0:pos] + "_" + strconv.FormatInt(time.Now().Unix(), 10) + name[pos:]
}
func verifySignUploadFilePrepareReq(req *pb.UploadFilePrepareReq, pubKey *rsa.PublicKey) error {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	hasher.Write(req.FileHash)
	hasher.Write(util_bytes.FromUint64(req.FileSize))
	for _, p := range req.Piece {
		hasher.Write(p.Hash)
		hasher.Write(util_bytes.FromUint32(p.Size))
	}
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
}
func (self *MatadataService) UploadFilePrepare(ctx context.Context, req *pb.UploadFilePrepareReq) (*pb.UploadFilePrepareResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return nil, errors.New(checkRes.ErrMsg)
	}

	if err := verifySignUploadFilePrepareReq(req, pubKey); err != nil {
		return nil, err
	}
	// nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	// TODO set provider
	return nil, nil
}

func verifySignUploadFileDoneReq(req *pb.UploadFileDoneReq, pubKey *rsa.PublicKey) (storeVolume uint64, err error) {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	hasher.Write([]byte(req.FilePath))
	hasher.Write(req.FileHash)
	hasher.Write(util_bytes.FromUint64(req.FileSize))
	hasher.Write([]byte(req.FileName))
	hasher.Write(util_bytes.FromUint64(req.FileModTime))
	for _, p := range req.Partition {
		for _, b := range p.Block {
			hasher.Write(b.Hash)
			hasher.Write(util_bytes.FromUint32(b.Size))
			hasher.Write(util_bytes.FromUint32(b.BlockSeq))
			if b.Checksum {
				hasher.Write([]byte{1})
			} else {
				hasher.Write([]byte{0})
			}
			for _, by := range b.StoreNodeId {
				hasher.Write(by)
			}
			// check size cheating, verify by auth
			storeVolume += uint64(b.Size) * uint64(len(b.StoreNodeId))
		}
	}
	if req.Interactive {
		hasher.Write([]byte{1})
	} else {
		hasher.Write([]byte{0})
	}
	err = rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
	return
}

func (self *MatadataService) UploadFileDone(ctx context.Context, req *pb.UploadFileDoneReq) (*pb.UploadFileDoneResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.UploadFileDoneResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	storeVolume, err := verifySignUploadFileDoneReq(req, pubKey)
	if err != nil {
		return &pb.UploadFileDoneResp{Code: 5, ErrMsg: "Verify Sign failed"}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	resobj, parentId := findPathId(nodeIdStr, req.FilePath)
	if resobj != nil {
		return &pb.UploadFileDoneResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	}
	fileName := req.FileName
	existId, _ := db.FileOwnerFileExists(nodeIdStr, parentId, req.FileName)
	if existId != nil {
		if req.Interactive {
			return &pb.UploadFileDoneResp{Code: 8, ErrMsg: "exist same name file or folder"}, nil
		} else {
			fileName = fixFileName(req.FileName)
		}
	}
	//check available space
	hashStr := base64.StdEncoding.EncodeToString(req.FileHash)
	blocks, err := fromPartitions(req.Partition)
	if err != nil {
		return &pb.UploadFileDoneResp{Code: 9, ErrMsg: err.Error()}, nil
	}
	db.FileSaveDone(nodeIdStr, hashStr, fileName, req.FileSize, req.FileModTime, parentId, len(req.Partition), blocks, storeVolume)
	return &pb.UploadFileDoneResp{Code: 0}, nil
}
func verifySignListFilesReq(req *pb.ListFilesReq, pubKey *rsa.PublicKey) error {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	hasher.Write([]byte(req.Path))
	hasher.Write(util_bytes.FromUint32(req.PageSize))
	hasher.Write(util_bytes.FromUint32(req.PageNum))
	hasher.Write([]byte(req.SortType.String()))
	if req.AscOrder {
		hasher.Write([]byte{1})
	} else {
		hasher.Write([]byte{0})
	}
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
}

func (self *MatadataService) ListFiles(ctx context.Context, req *pb.ListFilesReq) (*pb.ListFilesResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.ListFilesResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if req.PageSize > 2000 {
		return &pb.ListFilesResp{Code: 5, ErrMsg: "page size can not more than 2000"}, nil
	}

	if err := verifySignListFilesReq(req, pubKey); err != nil {
		return &pb.ListFilesResp{Code: 6, ErrMsg: "Verify Sign failed"}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	resobj, parentId := findPathId(nodeIdStr, req.Path)
	if resobj != nil {
		return &pb.ListFilesResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	}
	var sortField string
	if req.SortType == pb.SortType_Name {
		sortField = "NAME"
	} else if req.SortType == pb.SortType_ModTime {
		sortField = "MOD_TIME"
	} else if req.SortType == pb.SortType_Size {
		sortField = "SIZE"
	} else {
		return &pb.ListFilesResp{Code: 9, ErrMsg: "must specified sortType"}, nil
	}
	total, fofs := db.FileOwnerListOfPath(nodeIdStr, parentId, req.PageSize, req.PageNum, sortField, req.AscOrder)
	return &pb.ListFilesResp{Code: 0, TotalRecord: total, Fof: toFileOrFolderSlice(fofs)}, nil
}

func toFileOrFolderSlice(fofs []*db.Fof) []*pb.FileOrFolder {
	if fofs == nil || len(fofs) == 0 {
		return nil
	}
	res := make([]*pb.FileOrFolder, 0, len(fofs))
	for _, fof := range fofs {
		res = append(res, &pb.FileOrFolder{Folder: fof.IsFolder, Name: fof.Name, FileHash: fof.FileHash, FileSize: fof.FileSize, ModTime: fof.ModTime})
	}
	return res
}

func verifySignRetrieveFileReq(req *pb.RetrieveFileReq, pubKey *rsa.PublicKey) error {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	hasher.Write(req.FileHash)
	hasher.Write(util_bytes.FromUint64(req.FileSize))
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
}

func (self *MatadataService) RetrieveFile(ctx context.Context, req *pb.RetrieveFileReq) (*pb.RetrieveFileResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.RetrieveFileResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if err := verifySignRetrieveFileReq(req, pubKey); err != nil {
		return &pb.RetrieveFileResp{Code: 5, ErrMsg: "Verify Sign failed"}, nil
	}
	hash := base64.StdEncoding.EncodeToString(req.FileHash)
	exist, active, fileData, partitionCount, blocks, size := db.FileRetrieve(hash)
	if !exist {
		return &pb.RetrieveFileResp{Code: 6, ErrMsg: "file not exist"}, nil
	}
	if !active {
		return &pb.RetrieveFileResp{Code: 7, ErrMsg: "file offline Because of laws and regulations"}, nil
	}
	if size != req.FileSize {
		return &pb.RetrieveFileResp{Code: 8, ErrMsg: "file data size is not equal fileSize"}, nil
	}
	if fileData != nil && len(fileData) > 0 {
		return &pb.RetrieveFileResp{Code: 0, FileData: fileData}, nil
	}
	return &pb.RetrieveFileResp{Code: 0, Partition: toPartitions(hash, blocks, partitionCount)}, nil
}

func verifySignRemoveReq(req *pb.RemoveReq, pubKey *rsa.PublicKey) error {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	hasher.Write([]byte(req.Path))
	if req.Recursive {
		hasher.Write([]byte{1})
	} else {
		hasher.Write([]byte{0})
	}
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
}
func (self *MatadataService) Remove(ctx context.Context, req *pb.RemoveReq) (*pb.RemoveResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.RemoveResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if err := verifySignRemoveReq(req, pubKey); err != nil {
		return &pb.RemoveResp{Code: 5, ErrMsg: "Verify Sign failed"}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	resobj, pathId := findPathId(nodeIdStr, req.Path)
	if resobj != nil {
		return &pb.RemoveResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	}
	if pathId == nil {
		// error
	}
	if !db.FileOwnerRemove(nodeIdStr, pathId, req.Recursive) {
		// TODO
	}
	return nil, nil
}

func findPathId(nodeId string, path string) (res *resObj, pathId []byte) {
	if path == "" || path == "/" {
		return
	}
	if path[0] != '/' {
		return &resObj{Code: 200, ErrMsg: "path must start with slash /"}, nil
	}
	var found bool
	found, pathId = db.FileOwnerIdOfFilePath(nodeId, path)
	if !found {
		return &resObj{Code: 201, ErrMsg: "path is not exists"}, nil
	}
	return
}

const block_sep = ";"
const block_node_id_sep = ","

func fromPartitions(partitions []*pb.Partition) ([]string, error) {
	if partitions == nil || len(partitions) == 0 {
		return nil, nil
	}
	res := make([]string, 0, len(partitions))
	for _, p := range partitions {
		for _, b := range p.Block {
			if b.StoreNodeId == nil || len(b.StoreNodeId) == 0 {
				return nil, errors.New("empty store nodeId")
			}
			str := base64.StdEncoding.EncodeToString(b.Hash) + block_sep + strconv.Itoa(int(b.Size)) + block_sep + strconv.Itoa(int(b.BlockSeq)) + block_sep
			if b.Checksum {
				str += "1"
			} else {
				str += "0"
			}
			str += block_sep
			first := true
			for _, by := range b.StoreNodeId {
				if first {
					first = false
				} else {
					str += block_node_id_sep
				}
				str += base64.StdEncoding.EncodeToString(by)

			}
			res = append(res, str)
		}
	}
	return res, nil
}

func toPartitions(fileHash string, blocks []string, partitionsCount int) []*pb.Partition {
	if partitionsCount == 0 {
		return nil
	}
	if len(blocks)%partitionsCount != 0 {
		log.Errorf("parse file: %s block error, blocks length: %d, partitions count:%d", fileHash, len(blocks), partitionsCount)
		return nil
	}
	var err error
	var intVal int
	slice := make([]*pb.Block, 0, len(blocks))
	for _, str := range blocks {
		arr := strings.Split(str, block_sep)
		if len(arr) != 5 {
			log.Errorf("parse file: %s block str %s length error", fileHash, str)
			return nil
		}
		b := pb.Block{}
		b.Hash, err = base64.StdEncoding.DecodeString(arr[0])
		if err != nil {
			log.Errorf("parse file: %s block str %s error: %s", fileHash, str, err.Error())
			return nil
		}
		intVal, err = strconv.Atoi(arr[1])
		if err != nil {
			log.Errorf("parse file: %s block str %s error: %s", fileHash, str, err.Error())
			return nil
		}
		b.Size = uint32(intVal)
		intVal, err = strconv.Atoi(arr[2])
		if err != nil {
			log.Errorf("parse file: %s block str %s error: %s", fileHash, str, err.Error())
			return nil
		}
		b.BlockSeq = uint32(intVal)
		if arr[3] == "1" {
			b.Checksum = true
		} else if arr[3] == "0" {
			b.Checksum = false
		} else {
			log.Errorf("parse file: %s block str %s error: %s", fileHash, str, err.Error())
			return nil
		}
		nodeIds := strings.Split(arr[4], block_node_id_sep)
		if nodeIds == nil || len(nodeIds) == 0 {
			log.Errorf("parse file: %s block str %s error, no store nodeId", fileHash, str)
			return nil
		}
		store := make([][]byte, 0, len(nodeIds))
		for _, n := range nodeIds {
			bytes, err := base64.StdEncoding.DecodeString(n)
			if err != nil {
				log.Errorf("parse file: %s block str %s error: %s", fileHash, str, err.Error())
			}
			store = append(store, bytes)
		}
		b.StoreNodeId = store
		slice = append(slice, &b)
	}
	res := make([]*pb.Partition, 0, partitionsCount)
	blockCount := len(blocks) / partitionsCount
	for i := 0; i < partitionsCount; i++ {
		res = append(res, &pb.Partition{Block: slice[i : i+blockCount]})
	}
	return res
}
