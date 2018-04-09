package impl

import (
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"nebula-tracker/config"
	"nebula-tracker/db"
	chooser "nebula-tracker/metadata/provider_chooser"
	"strconv"
	"strings"
	"time"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	pb "github.com/spolabs/nebula/tracker/metadata/pb"
	util_bytes "github.com/spolabs/nebula/util/bytes"
	"golang.org/x/net/context"
)

type MatadataService struct {
}

func NewMatadataService() *MatadataService {
	return &MatadataService{}
}

func (self *MatadataService) MkFolder(ctx context.Context, req *pb.MkFolderReq) (*pb.MkFolderResp, error) {
	// TODO support path Id
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.MkFolderResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.MkFolderResp{Code: 4, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.MkFolderResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
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

func (self *MatadataService) CheckFileExist(ctx context.Context, req *pb.CheckFileExistReq) (*pb.CheckFileExistResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.CheckFileExistResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.CheckFileExistResp{Code: 4, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	// TODO new Version
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.CheckFileExistResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
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
		providerCnt := chooser.Count()
		conf := config.GetTrackerConfig()
		if req.FileSize <= multi_replica_max_file_size || (!conf.TestMode && providerCnt < 12) || (conf.TestMode && providerCnt < 3) {
			resp.StoreType = pb.FileStoreType_MultiReplica
			resp.ReplicaCount = 5
			if providerCnt < 5 {
				resp.ReplicaCount = uint32(providerCnt)
			}
			resp.Provider = prepareReplicaProvider(resp.ReplicaCount, req.FileHash, req.FileSize)
			db.FileSaveStep1(nodeIdStr, hashStr, req.FileSize, 5*req.FileSize)
		} else {
			resp.StoreType = pb.FileStoreType_ErasureCode
			if providerCnt >= 40 {
				resp.DataPieceCount = 32
				resp.VerifyPieceCount = 8
			} else if providerCnt >= 22 {
				resp.DataPieceCount = 16
				resp.VerifyPieceCount = 6
			} else if providerCnt >= 12 {
				resp.DataPieceCount = 8
				resp.VerifyPieceCount = 4
			} else if conf.TestMode && providerCnt >= 6 {
				resp.DataPieceCount = 4
				resp.VerifyPieceCount = 2
			} else if conf.TestMode && providerCnt >= 3 {
				resp.DataPieceCount = 2
				resp.VerifyPieceCount = 1
			}
			db.FileSaveStep1(nodeIdStr, hashStr, req.FileSize, 0)
		}
		return &resp, nil
	}
}
func uuidStr() string {
	return uuid.Must(uuid.NewV4()).String()
}
func prepareReplicaProvider(num uint32, fileHash []byte, fileSize uint64) []*pb.ReplicaProvider {
	pis := chooser.Choose(num)
	res := make([]*pb.ReplicaProvider, 0, len(pis))
	ts := uint64(time.Now().Unix())
	for _, pi := range pis {
		ticket := uuidStr()
		res = append(res, &pb.ReplicaProvider{NodeId: pi.NodeIdBytes,
			Port:      pi.Port,
			Server:    pi.Server(),
			Timestamp: ts,
			Ticket:    ticket,
			Auth:      genProviderStoreAuth(pi.PublicKey, fileHash, fileSize, ts, ticket)})
	}
	return res
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

const verify_sign_expired = 15

func (self *MatadataService) UploadFilePrepare(ctx context.Context, req *pb.UploadFilePrepareReq) (*pb.UploadFilePrepareResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return nil, errors.New(checkRes.ErrMsg)
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return nil, errors.New("auth info expired， please check your system time")
	}
	if err := req.VerifySign(pubKey); err != nil {
		return nil, err
	}
	if len(req.Partition) == 0 {
		return nil, errors.New("partition Data is required")
	}
	providerCnt := chooser.Count()
	pieceCnt := len(req.Partition[0].Piece)
	for _, p := range req.Partition {
		if len(p.Piece) == 0 {
			return nil, errors.New("piece Data is required")
		}
		if len(p.Piece) > providerCnt {
			return nil, errors.New("not enough provider")
		}
		if len(p.Piece) != pieceCnt {
			return nil, errors.New("all parition must have same number piece")
		}
	}
	backupProCnt := 10
	if providerCnt-pieceCnt < backupProCnt {
		backupProCnt = providerCnt - pieceCnt
	}
	return &pb.UploadFilePrepareResp{Partition: prepareErasureCodeProvider(req.Partition, pieceCnt, backupProCnt)}, nil
}

func prepareErasureCodeProvider(partition []*pb.SplitPartition, pieceCnt int, backupProCnt int) []*pb.ErasureCodePartition {
	ts := uint64(time.Now().Unix())
	res := make([]*pb.ErasureCodePartition, 0, len(partition))
	for _, part := range partition {
		pis := chooser.Choose(uint32(pieceCnt + backupProCnt))
		if len(pis) < pieceCnt {
			panic("not enough provider")
		}
		proAuth := make([]*pb.BlockProviderAuth, 0, len(pis))
		for i, piece := range part.Piece {
			pi := pis[i]
			ticket := uuidStr()
			proAuth = append(proAuth, &pb.BlockProviderAuth{NodeId: pi.NodeIdBytes,
				Server: pi.Server(),
				Port:   pi.Port,
				Spare:  false,
				HashAuth: []*pb.PieceHashAuth{&pb.PieceHashAuth{Hash: piece.Hash,
					Size:   piece.Size,
					Ticket: ticket,
					Auth:   genProviderStoreAuth(pi.PublicKey, piece.Hash, uint64(piece.Size), ts, ticket)}}})
		}
		if len(pis) == pieceCnt {
			continue
		}
		each := pieceCnt * 2 / (len(pis) - pieceCnt)
		if len(pis)-pieceCnt == 1 {
			each = pieceCnt
		} else if pieceCnt*2%(len(pis)-pieceCnt) != 0 {
			each += 1
		}
		for i := pieceCnt; i < len(pis); i++ {
			pi := pis[i]
			proAuth = append(proAuth, &pb.BlockProviderAuth{NodeId: pi.NodeIdBytes,
				Server:   pi.Server(),
				Port:     pi.Port,
				Spare:    true,
				HashAuth: make([]*pb.PieceHashAuth, 0, each)})
		}

		for i := 0; i < pieceCnt*2; i++ {
			pi := pis[pieceCnt+i/each]
			bpa := proAuth[pieceCnt+i/each]
			piece := part.Piece[i%pieceCnt]
			ticket := uuidStr()
			bpa.HashAuth = append(bpa.HashAuth, &pb.PieceHashAuth{Hash: piece.Hash,
				Size:   piece.Size,
				Ticket: ticket,
				Auth:   genProviderStoreAuth(pi.PublicKey, piece.Hash, uint64(piece.Size), ts, ticket)})
		}
		res = append(res, &pb.ErasureCodePartition{ProviderAuth: proAuth, Timestamp: ts})
	}
	return res
}

func (self *MatadataService) UploadFileDone(ctx context.Context, req *pb.UploadFileDoneReq) (*pb.UploadFileDoneResp, error) {
	// TODO newVersion
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.UploadFileDoneResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.UploadFileDoneResp{Code: 4, ErrMsg: "auth info expired， please check your system time"}, nil
	}

	if err := req.VerifySign(pubKey); err != nil {
		return &pb.UploadFileDoneResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
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
	var storeVolume uint64
	for _, p := range req.Partition {
		for _, b := range p.Block {
			// check size cheating, verify by auth
			storeVolume += uint64(b.Size) * uint64(len(b.StoreNodeId))
		}
	}
	db.FileSaveDone(nodeIdStr, hashStr, fileName, req.FileSize, req.FileModTime, parentId, len(req.Partition), blocks, storeVolume)
	return &pb.UploadFileDoneResp{Code: 0}, nil
}

func (self *MatadataService) ListFiles(ctx context.Context, req *pb.ListFilesReq) (*pb.ListFilesResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.ListFilesResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.ListFilesResp{Code: 4, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if req.PageSize > 2000 {
		return &pb.ListFilesResp{Code: 5, ErrMsg: "page size can not more than 2000"}, nil
	}

	if err := req.VerifySign(pubKey); err != nil {
		return &pb.ListFilesResp{Code: 6, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
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

func (self *MatadataService) RetrieveFile(ctx context.Context, req *pb.RetrieveFileReq) (*pb.RetrieveFileResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.RetrieveFileResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.RetrieveFileResp{Code: 4, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.RetrieveFileResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
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
	ts := uint64(time.Now().Unix())
	parts, err := toRetrievePartition(hash, blocks, partitionCount, ts)
	if err != nil {
		return &pb.RetrieveFileResp{Code: 9, ErrMsg: err.Error()}, nil
	}
	return &pb.RetrieveFileResp{Code: 0, Partition: parts, Timestamp: ts}, nil
}

func toRetrievePartition(fileHash string, blocks []string, partitionsCount int, ts uint64) ([]*pb.RetrievePartition, error) {
	if partitionsCount == 0 {
		return nil, errors.New("empty blocks")
	}
	if len(blocks)%partitionsCount != 0 {
		return nil, fmt.Errorf("parse file: %s block error, blocks length: %d, partitions count:%d", fileHash, len(blocks), partitionsCount)
	}
	var err error
	var intVal int
	slice := make([]*pb.RetrieveBlock, 0, len(blocks))
	providerMap := make(map[string]*db.ProviderInfo, 50)
	for _, str := range blocks {
		arr := strings.Split(str, block_sep)
		if len(arr) != 5 {
			return nil, fmt.Errorf("parse file: %s block str %s length error", fileHash, str)
		}
		b := pb.RetrieveBlock{}
		b.Hash, err = base64.StdEncoding.DecodeString(arr[0])
		if err != nil {
			return nil, err
		}
		intVal, err = strconv.Atoi(arr[1])
		if err != nil {
			return nil, err
		}
		b.Size = uint64(intVal)
		intVal, err = strconv.Atoi(arr[2])
		if err != nil {
			return nil, err
		}
		b.BlockSeq = uint32(intVal)
		if arr[3] == "1" {
			b.Checksum = true
		} else if arr[3] == "0" {
			b.Checksum = false
		} else {
			return nil, errors.New("can not parse chechsum char:" + arr[3])
		}
		nodeIds := strings.Split(arr[4], block_node_id_sep)
		if nodeIds == nil || len(nodeIds) == 0 {
			return nil, fmt.Errorf("parse file: %s block str %s error, no store nodeId", fileHash, str)
		}
		store := make([]*pb.RetrieveNode, 0, len(nodeIds))
		for _, n := range nodeIds {
			bytes, err := base64.StdEncoding.DecodeString(n)
			if err != nil {
				return nil, err
			}
			var pro *db.ProviderInfo
			if v, ok := providerMap[n]; ok {
				pro = v
			} else {
				pro = db.ProviderFindOne(n)
				if pro == nil {
					return nil, errors.New("can not find provider, nodeId: " + n)
				}
				providerMap[n] = pro
			}
			ticket := uuidStr()
			store = append(store, &pb.RetrieveNode{NodeId: bytes,
				Server: pro.Server(),
				Port:   pro.Port,
				Ticket: ticket,
				Auth:   genProviderRetrieveAuth(pro.PublicKey, b.Hash, b.Size, ts, ticket)})
		}
		b.StoreNode = store
		slice = append(slice, &b)
	}
	res := make([]*pb.RetrievePartition, 0, partitionsCount)
	blockCount := len(blocks) / partitionsCount
	for i := 0; i < partitionsCount; i++ {
		res = append(res, &pb.RetrievePartition{Block: slice[i : i+blockCount]})
	}
	return res, nil
}

func genProviderRetrieveAuth(publicKey []byte, hash []byte, size uint64, timestamp uint64, ticket string) []byte {
	return genProviderAuth(publicKey, hash, size, timestamp, ticket, "Retrieve")
}
func genProviderStoreAuth(publicKey []byte, hash []byte, size uint64, timestamp uint64, ticket string) []byte {
	return genProviderAuth(publicKey, hash, size, timestamp, ticket, "Store")
}

func genProviderAuth(publicKey []byte, hash []byte, size uint64, timestamp uint64, ticket string, method string) []byte {
	hasher := hmac.New(sha256.New, publicKey)
	hasher.Write([]byte(method))
	hasher.Write(hash)
	hasher.Write(util_bytes.FromUint64(size))
	hasher.Write(util_bytes.FromUint64(timestamp))
	hasher.Write([]byte(ticket))
	return hasher.Sum(nil)
}

func (self *MatadataService) Remove(ctx context.Context, req *pb.RemoveReq) (*pb.RemoveResp, error) {
	checkRes, pubKey := checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.RemoveResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.RemoveResp{Code: 4, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.RemoveResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	resobj, pathId := findPathId(nodeIdStr, req.Path)
	if resobj != nil {
		return &pb.RemoveResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	}
	if pathId == nil {
		return &pb.RemoveResp{Code: 6, ErrMsg: "path not exists"}, nil
	}
	if !db.FileOwnerRemove(nodeIdStr, pathId, req.Recursive) {
		return &pb.RemoveResp{Code: 7, ErrMsg: "folder not empty"}, nil
	}
	return &pb.RemoveResp{Code: 0}, nil
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

func fromPartitions(partitions []*pb.StorePartition) ([]string, error) {
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
