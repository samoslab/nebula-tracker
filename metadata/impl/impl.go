package impl

import (
	"crypto/rsa"
	"encoding/base64"
	"errors"
	"fmt"
	"nebula-tracker/config"
	"nebula-tracker/db"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	provider_pb "github.com/samoslab/nebula/provider/pb"
	pb "github.com/samoslab/nebula/tracker/metadata/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type MatadataService struct {
	c providerChooser
	d dao
}

func NewMatadataService() *MatadataService {
	return &MatadataService{c: &chooserImpl{}, d: &daoImpl{}}
}

func (self *MatadataService) MkFolder(ctx context.Context, req *pb.MkFolderReq) (resp *pb.MkFolderResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			resp = &pb.MkFolderResp{Code: 300, ErrMsg: fmt.Sprintf("System error: %s", er)}
		}
	}()
	checkRes, pubKey := self.checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.MkFolderResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.MkFolderResp{Code: 4, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.MkFolderResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
	}
	if len(req.Folder) == 0 {
		return &pb.MkFolderResp{Code: 6, ErrMsg: "Folder is required"}, nil
	}
	for _, name := range req.Folder {
		if len(name) == 0 {
			return &pb.MkFolderResp{Code: 7, ErrMsg: "folder name can not be empty"}, nil
		}
		if strings.ContainsAny(name, "/") {
			return &pb.MkFolderResp{Code: 13, ErrMsg: "folder name can not contains slash /"}, nil
		}
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	inService, emailVerified, _, _, _, _, _, _, _, _, _, _ := self.d.UsageAmount(nodeIdStr)
	if !emailVerified {
		return &pb.MkFolderResp{Code: 400, ErrMsg: "email not verified"}, nil
	}
	if !inService {
		return &pb.MkFolderResp{Code: 401, ErrMsg: "not buy any package order"}, nil
	}
	resobj, parentId := self.findPathId(nodeIdStr, req.Parent, true)
	if resobj != nil {
		return &pb.MkFolderResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	}
	duplicateFile, duplicateFolder := self.d.FileOwnerMkFolders(req.Interactive, nodeIdStr, parentId, req.Folder)
	if req.Interactive {
		if len(duplicateFile)+len(duplicateFolder) > 0 {
			if len(duplicateFile) == 0 {
				return &pb.MkFolderResp{Code: 8, ErrMsg: "duplication of folder name: " + strings.Join(duplicateFolder, ", ")}, nil
			} else if len(duplicateFolder) == 0 {
				return &pb.MkFolderResp{Code: 9, ErrMsg: "duplication of folder name, aleady exist file: " + strings.Join(duplicateFile, ", ")}, nil
			} else {
				return &pb.MkFolderResp{Code: 10, ErrMsg: "duplication of folder name, aleady exist folder: " + strings.Join(duplicateFolder, ", ") + ", aleady exist file:" + strings.Join(duplicateFile, ", ")}, nil
			}
		}
	} else {
		if len(duplicateFile) > 0 {
			return &pb.MkFolderResp{Code: 9, ErrMsg: "duplication of folder name, aleady exist file: " + strings.Join(duplicateFile, ", ")}, nil
		}
	}
	return &pb.MkFolderResp{Code: 0}, nil
}

type resObj struct {
	Code   uint32
	ErrMsg string
}

func (self *MatadataService) checkNodeId(nodeId []byte) (*resObj, *rsa.PublicKey) {
	if len(nodeId) == 0 {
		return &resObj{Code: 100, ErrMsg: "NodeId is required"}, nil
	}
	if len(nodeId) != 20 {
		return &resObj{Code: 101, ErrMsg: "NodeId length must be 20"}, nil
	}
	pubKey := self.d.ClientGetPubKey(nodeId)
	if pubKey == nil {
		return &resObj{Code: 102, ErrMsg: "this node id is not been registered"}, nil
	}
	return nil, pubKey
}

func (self *MatadataService) CheckFileExist(ctx context.Context, req *pb.CheckFileExistReq) (resp *pb.CheckFileExistResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			resp = &pb.CheckFileExistResp{Code: 300, ErrMsg: fmt.Sprintf("System error: %s", er)}
		}
	}()
	checkRes, pubKey := self.checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.CheckFileExistResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.CheckFileExistResp{Code: 4, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.CheckFileExistResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	inService, emailVerified, _, volume, netflow, upNetflow,
		_, usageVolume, usageNetflow, usageUpNetflow, _, _ := self.d.UsageAmount(nodeIdStr)
	if !emailVerified {
		return &pb.CheckFileExistResp{Code: 400, ErrMsg: "email not verified"}, nil
	}
	if !inService {
		return &pb.CheckFileExistResp{Code: 401, ErrMsg: "not buy any package order"}, nil
	}
	if volume <= usageVolume {
		return &pb.CheckFileExistResp{Code: 410, ErrMsg: "storage volume exceed"}, nil
	}
	if netflow <= usageNetflow {
		return &pb.CheckFileExistResp{Code: 411, ErrMsg: "netflow exceed"}, nil
	}
	if upNetflow <= usageUpNetflow {
		return &pb.CheckFileExistResp{Code: 412, ErrMsg: "upload netflow exceed"}, nil
	}
	// if downNetflow <= usageDownNetflow {
	// 	return &pb.CheckFileExistResp{Code: 413, ErrMsg: "download netflow exceed"}, nil
	// }
	resobj, parentId := self.findPathId(nodeIdStr, req.Parent, true)
	if resobj != nil {
		return &pb.CheckFileExistResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	}
	if strings.ContainsAny(req.FileName, "/") {
		return &pb.CheckFileExistResp{Code: 13, ErrMsg: "filename can not contains slash /"}, nil
	}
	fileName := req.FileName
	hashStr := base64.StdEncoding.EncodeToString(req.FileHash)
	existId, isFolder, hash := self.d.FileOwnerFileExists(nodeIdStr, parentId, req.FileName)
	if len(existId) > 0 {
		if isFolder {
			if req.Interactive {
				return &pb.CheckFileExistResp{Code: 12, ErrMsg: "exist same name folder"}, nil
			} else {
				fileName = fixFileName(req.FileName)
				existId = nil
			}
		} else {
			if hash == hashStr {
				return &pb.CheckFileExistResp{Code: 0, ErrMsg: "file aleady exists"}, nil
			}
			if !req.NewVersion {
				if req.Interactive {
					return &pb.CheckFileExistResp{Code: 8, ErrMsg: "exist same name file"}, nil
				} else {
					fileName = fixFileName(req.FileName)
					existId = nil
				}
			}
		}
	}
	exist, active, done, size, selfCreate, doneExpired := self.d.FileCheckExist(nodeIdStr, hashStr, done_expired)
	if exist {
		if size != req.FileSize {
			log.Warnf("hash: %s size is %d, new upload file size is %d", hashStr, size, req.FileSize)
		}
		if !active {
			return &pb.CheckFileExistResp{Code: 9, ErrMsg: "this file can not upload because of laws and regulations"}, nil
		}
		if done {
			self.d.FileReuse(existId, nodeIdStr, hashStr, fileName, req.FileSize, req.FileModTime, parentId)
			return &pb.CheckFileExistResp{Code: 0}, nil
		} else {
			if !selfCreate && !doneExpired {
				return &pb.CheckFileExistResp{Code: 10, ErrMsg: "this file is being uploaded by other user, please wait a moment to retry"}, nil
			}
		}
	}
	if req.FileSize <= embed_metadata_max_file_size {
		if req.FileSize != uint64(len(req.FileData)) {
			return &pb.CheckFileExistResp{Code: 11, ErrMsg: "file data size is not equal fileSize"}, nil
		}
		self.d.FileSaveTiny(existId, nodeIdStr, hashStr, req.FileData, fileName, req.FileSize, req.FileModTime, parentId)
		return &pb.CheckFileExistResp{Code: 0}, nil
	}

	resp = &pb.CheckFileExistResp{Code: 1}
	providerCnt := self.c.Count()
	conf := config.GetTrackerConfig()
	if req.FileSize <= multi_replica_max_file_size || (!conf.TestMode && providerCnt < 12) || (conf.TestMode && providerCnt < 3) {
		resp.StoreType = pb.FileStoreType_MultiReplica
		resp.ReplicaCount = 5
		if providerCnt < 5 {
			resp.ReplicaCount = uint32(providerCnt)
		}
		resp.Provider = self.prepareReplicaProvider(nodeIdStr, int(resp.ReplicaCount), req.FileHash, req.FileSize)
		if !exist {
			self.d.FileSaveStep1(nodeIdStr, hashStr, req.FileSize, 0)
		}
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
		if !exist {
			self.d.FileSaveStep1(nodeIdStr, hashStr, req.FileSize, 0)
		}
	}
	return resp, nil

}

const done_expired = 1800

func uuidStr() string {
	u := uuid.Must(uuid.NewV4())
	return "-" + base64.StdEncoding.EncodeToString(u[:])
}
func (self *MatadataService) prepareReplicaProvider(nodeId string, num int, fileHash []byte, fileSize uint64) []*pb.ReplicaProvider {
	pis := self.c.Choose(num)
	res := make([]*pb.ReplicaProvider, 0, len(pis))
	ts := uint64(time.Now().Unix())
	for _, pi := range pis {
		ticket := nodeId + uuidStr()
		res = append(res, &pb.ReplicaProvider{NodeId: pi.NodeIdBytes,
			Port:      pi.Port,
			Server:    pi.Server(),
			Timestamp: ts,
			Ticket:    ticket,
			Auth:      provider_pb.GenStoreAuth(pi.PublicKey, fileHash, fileSize, fileHash, fileSize, ts, ticket)})
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

func (self *MatadataService) UploadFilePrepare(ctx context.Context, req *pb.UploadFilePrepareReq) (resp *pb.UploadFilePrepareResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			err = status.Errorf(codes.Internal, "System error: %s", er)
		}
	}()
	checkRes, pubKey := self.checkNodeId(req.NodeId)
	if checkRes != nil {
		return nil, status.Error(codes.InvalidArgument, checkRes.ErrMsg)
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return nil, status.Error(codes.Unauthenticated, "auth info expired， please check your system time")
	}
	if err := req.VerifySign(pubKey); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "verify sign failed， error: %s", err)
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	inService, emailVerified, _, volume, netflow, upNetflow,
		_, usageVolume, usageNetflow, usageUpNetflow, _, _ := self.d.UsageAmount(nodeIdStr)
	if !emailVerified {
		return nil, status.Error(codes.PermissionDenied, "email not verified")
	}
	if !inService {
		return nil, status.Error(codes.PermissionDenied, "not buy any package order")
	}

	if volume <= usageVolume {
		return nil, status.Error(codes.OutOfRange, "storage volume exceed")
	}
	if netflow <= usageNetflow {
		return nil, status.Error(codes.OutOfRange, "netflow exceed")
	}
	if upNetflow <= usageUpNetflow {
		return nil, status.Error(codes.OutOfRange, "upload netflow exceed")
	}
	// if downNetflow <= usageDownNetflow {
	// 	return nil, status.Error(codes.OutOfRange, "download netflow exceed")
	// }

	if len(req.Partition) == 0 {
		return nil, status.Error(codes.InvalidArgument, "partition data is required")
	}
	pieceCnt := len(req.Partition[0].Piece)
	if pieceCnt == 0 {
		return nil, status.Error(codes.InvalidArgument, "piece data is required")
	}
	providerCnt := self.c.Count()
	for _, p := range req.Partition {
		if len(p.Piece) > providerCnt {
			return nil, status.Error(codes.InvalidArgument, "not enough provider")
		}
		if len(p.Piece) != pieceCnt {
			return nil, status.Error(codes.InvalidArgument, "all parition must have same number piece")
		}
	}
	backupProCnt := 10
	if backupProCnt > pieceCnt {
		backupProCnt = pieceCnt
	}
	if providerCnt-pieceCnt < backupProCnt {
		backupProCnt = providerCnt - pieceCnt
	}
	return &pb.UploadFilePrepareResp{Partition: self.prepareErasureCodeProvider(base64.StdEncoding.EncodeToString(req.NodeId), req.FileHash, req.FileSize, req.Partition, pieceCnt, backupProCnt)}, nil
}

func (self *MatadataService) prepareErasureCodeProvider(nodeId string, fileHash []byte, fileSize uint64, partition []*pb.SplitPartition, pieceCnt int, backupProCnt int) []*pb.ErasureCodePartition {
	ts := uint64(time.Now().Unix())
	res := make([]*pb.ErasureCodePartition, 0, len(partition))
	for _, part := range partition {
		pis := self.c.Choose(pieceCnt + backupProCnt)
		if len(pis) < pieceCnt {
			panic("not enough provider")
		}
		proAuth := make([]*pb.BlockProviderAuth, 0, len(pis))
		for i, piece := range part.Piece {
			pi := pis[i]
			ticket := nodeId + uuidStr()
			proAuth = append(proAuth, &pb.BlockProviderAuth{NodeId: pi.NodeIdBytes,
				Server: pi.Server(),
				Port:   pi.Port,
				Spare:  false,
				HashAuth: []*pb.PieceHashAuth{&pb.PieceHashAuth{Hash: piece.Hash,
					Size:   piece.Size,
					Ticket: ticket,
					Auth:   provider_pb.GenStoreAuth(pi.PublicKey, fileHash, fileSize, piece.Hash, uint64(piece.Size), ts, ticket)}}})
		}
		if len(pis) == pieceCnt {
			res = append(res, &pb.ErasureCodePartition{ProviderAuth: proAuth, Timestamp: ts})
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
			ticket := nodeId + uuidStr()
			bpa.HashAuth = append(bpa.HashAuth, &pb.PieceHashAuth{Hash: piece.Hash,
				Size:   piece.Size,
				Ticket: ticket,
				Auth:   provider_pb.GenStoreAuth(pi.PublicKey, fileHash, fileSize, piece.Hash, uint64(piece.Size), ts, ticket)})
		}
		res = append(res, &pb.ErasureCodePartition{ProviderAuth: proAuth, Timestamp: ts})
	}
	return res
}

func (self *MatadataService) UploadFileDone(ctx context.Context, req *pb.UploadFileDoneReq) (resp *pb.UploadFileDoneResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			resp = &pb.UploadFileDoneResp{Code: 300, ErrMsg: fmt.Sprintf("System error: %s", er)}
		}
	}()
	checkRes, pubKey := self.checkNodeId(req.NodeId)
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
	inService, emailVerified, _, volume, netflow, upNetflow,
		_, usageVolume, usageNetflow, usageUpNetflow, _, _ := self.d.UsageAmount(nodeIdStr)
	if !emailVerified {
		return &pb.UploadFileDoneResp{Code: 400, ErrMsg: "email not verified"}, nil
	}
	if !inService {
		return &pb.UploadFileDoneResp{Code: 401, ErrMsg: "not buy any package order"}, nil
	}
	if volume <= usageVolume {
		return &pb.UploadFileDoneResp{Code: 410, ErrMsg: "storage volume exceed"}, nil
	}
	if netflow <= usageNetflow {
		return &pb.UploadFileDoneResp{Code: 411, ErrMsg: "netflow exceed"}, nil
	}
	if upNetflow <= usageUpNetflow {
		return &pb.UploadFileDoneResp{Code: 412, ErrMsg: "upload netflow exceed"}, nil
	}
	// if downNetflow <= usageDownNetflow {
	// 	return &pb.UploadFileDoneResp{Code: 413, ErrMsg: "download netflow exceed"}, nil
	// }
	resobj, parentId := self.findPathId(nodeIdStr, req.Parent, true)
	if resobj != nil {
		return &pb.UploadFileDoneResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	}
	if strings.ContainsAny(req.FileName, "/") {
		return &pb.UploadFileDoneResp{Code: 13, ErrMsg: "filename can not contains slash /"}, nil
	}
	fileName := req.FileName
	hashStr := base64.StdEncoding.EncodeToString(req.FileHash)
	existId, isFolder, hash := self.d.FileOwnerFileExists(nodeIdStr, parentId, req.FileName)
	if len(existId) > 0 {
		if isFolder {
			if req.Interactive {
				return &pb.UploadFileDoneResp{Code: 12, ErrMsg: "exist same name folder"}, nil
			} else {
				fileName = fixFileName(req.FileName)
				existId = nil
			}
		} else {
			if hash == hashStr {
				return &pb.UploadFileDoneResp{Code: 14, ErrMsg: "file aleady exists"}, nil
			}
			if !req.NewVersion {
				if req.Interactive {
					return &pb.UploadFileDoneResp{Code: 8, ErrMsg: "exist same name file"}, nil
				} else {
					fileName = fixFileName(req.FileName)
					existId = nil
				}
			}
		}
	}

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
	self.d.FileSaveDone(existId, nodeIdStr, hashStr, fileName, req.FileSize, req.FileModTime, parentId, len(req.Partition), blocks, storeVolume)
	return &pb.UploadFileDoneResp{Code: 0}, nil
}

func (self *MatadataService) ListFiles(ctx context.Context, req *pb.ListFilesReq) (resp *pb.ListFilesResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			resp = &pb.ListFilesResp{Code: 300, ErrMsg: fmt.Sprintf("System error: %s", er)}
		}
	}()
	checkRes, pubKey := self.checkNodeId(req.NodeId)
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
	inService, emailVerified, _, _, netflow, _,
		downNetflow, _, usageNetflow, _, usageDownNetflow, _ := self.d.UsageAmount(nodeIdStr)
	if !emailVerified {
		return &pb.ListFilesResp{Code: 400, ErrMsg: "email not verified"}, nil
	}
	if !inService {
		return &pb.ListFilesResp{Code: 401, ErrMsg: "not buy any package order"}, nil
	}
	// if volume <= usageVolume {
	// 	return &pb.ListFilesResp{Code: 410, ErrMsg: "storage volume exceed"}, nil
	// }
	if netflow <= usageNetflow {
		return &pb.ListFilesResp{Code: 411, ErrMsg: "netflow exceed"}, nil
	}
	// if upNetflow <= usageUpNetflow {
	// 	return &pb.ListFilesResp{Code: 412, ErrMsg: "upload netflow exceed"}, nil
	// }
	if downNetflow <= usageDownNetflow {
		return &pb.ListFilesResp{Code: 413, ErrMsg: "download netflow exceed"}, nil
	}
	resobj, parentId := self.findPathId(nodeIdStr, req.Parent, true)
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
	total, fofs := self.d.FileOwnerListOfPath(nodeIdStr, parentId, req.PageSize, req.PageNum, sortField, req.AscOrder)
	return &pb.ListFilesResp{Code: 0, TotalRecord: total, Fof: toFileOrFolderSlice(fofs)}, nil
}

func toFileOrFolderSlice(fofs []*db.Fof) []*pb.FileOrFolder {
	if len(fofs) == 0 {
		return nil
	}
	res := make([]*pb.FileOrFolder, 0, len(fofs))
	for _, fof := range fofs {
		res = append(res, &pb.FileOrFolder{Id: fof.Id, Folder: fof.IsFolder, Name: fof.Name, FileHash: fof.FileHash, FileSize: fof.FileSize, ModTime: fof.ModTime})
	}
	return res
}

func (self *MatadataService) RetrieveFile(ctx context.Context, req *pb.RetrieveFileReq) (resp *pb.RetrieveFileResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			resp = &pb.RetrieveFileResp{Code: 300, ErrMsg: fmt.Sprintf("System error: %s", er)}
		}
	}()
	checkRes, pubKey := self.checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.RetrieveFileResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.RetrieveFileResp{Code: 4, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.RetrieveFileResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	inService, emailVerified, _, _, netflow, _,
		downNetflow, _, usageNetflow, _, usageDownNetflow, _ := self.d.UsageAmount(nodeIdStr)
	if !emailVerified {
		return &pb.RetrieveFileResp{Code: 400, ErrMsg: "email not verified"}, nil
	}
	if !inService {
		return &pb.RetrieveFileResp{Code: 401, ErrMsg: "not buy any package order"}, nil
	}
	// if volume <= usageVolume {
	// 	return &pb.RetrieveFileResp{Code: 410, ErrMsg: "storage volume exceed"}, nil
	// }
	if netflow <= usageNetflow {
		return &pb.RetrieveFileResp{Code: 411, ErrMsg: "netflow exceed"}, nil
	}
	// if upNetflow <= usageUpNetflow {
	// 	return &pb.RetrieveFileResp{Code: 412, ErrMsg: "upload netflow exceed"}, nil
	// }
	if downNetflow <= usageDownNetflow {
		return &pb.RetrieveFileResp{Code: 413, ErrMsg: "download netflow exceed"}, nil
	}
	hash := base64.StdEncoding.EncodeToString(req.FileHash)
	exist, active, fileData, partitionCount, blocks, size := self.d.FileRetrieve(hash)
	if !exist {
		return &pb.RetrieveFileResp{Code: 6, ErrMsg: "file not exist"}, nil
	}
	if !active {
		return &pb.RetrieveFileResp{Code: 7, ErrMsg: "file offline Because of laws and regulations"}, nil
	}
	if size != req.FileSize {
		return &pb.RetrieveFileResp{Code: 8, ErrMsg: "file data size is not equal fileSize"}, nil
	}
	if size == 0 {
		return &pb.RetrieveFileResp{Code: 0, FileData: []byte{}}, nil
	}
	if len(fileData) > 0 {
		return &pb.RetrieveFileResp{Code: 0, FileData: fileData}, nil
	}
	ts := uint64(time.Now().Unix())
	parts, err := self.toRetrievePartition(nodeIdStr, req.FileHash, req.FileSize, blocks, partitionCount, ts)
	if err != nil {
		return &pb.RetrieveFileResp{Code: 9, ErrMsg: err.Error()}, nil
	}
	return &pb.RetrieveFileResp{Code: 0, Partition: parts, Timestamp: ts}, nil
}

func (self *MatadataService) toRetrievePartition(nodeId string, fileHash []byte, fileSize uint64, blocks []string, partitionsCount int, ts uint64) ([]*pb.RetrievePartition, error) {
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
			return nil, fmt.Errorf("parse file: %s block str %s length error", base64.StdEncoding.EncodeToString(fileHash), str)
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
		if len(nodeIds) == 0 {
			return nil, fmt.Errorf("parse file: %s block str %s error, no store nodeId", base64.StdEncoding.EncodeToString(fileHash), str)
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
				pro = self.d.ProviderFindOne(n)
				if pro == nil {
					return nil, errors.New("can not find provider, nodeId: " + n)
				}
				providerMap[n] = pro
			}
			ticket := nodeId + uuidStr()
			store = append(store, &pb.RetrieveNode{NodeId: bytes,
				Server: pro.Server(),
				Port:   pro.Port,
				Ticket: ticket,
				Auth:   provider_pb.GenRetrieveAuth(pro.PublicKey, fileHash, fileSize, b.Hash, b.Size, ts, ticket)})
		}
		b.StoreNode = store
		slice = append(slice, &b)
	}
	res := make([]*pb.RetrievePartition, 0, partitionsCount)
	blockCount := len(blocks) / partitionsCount
	for i := 0; i < partitionsCount; i++ {
		res = append(res, &pb.RetrievePartition{Block: slice[i*blockCount : i*blockCount+blockCount]})
	}
	return res, nil
}

func (self *MatadataService) Remove(ctx context.Context, req *pb.RemoveReq) (resp *pb.RemoveResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			resp = &pb.RemoveResp{Code: 300, ErrMsg: fmt.Sprintf("System error: %s", er)}
		}
	}()
	checkRes, pubKey := self.checkNodeId(req.NodeId)
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
	inService, emailVerified, _, _, _, _,
		_, _, _, _, _, _ := self.d.UsageAmount(nodeIdStr)
	if !emailVerified {
		return &pb.RemoveResp{Code: 400, ErrMsg: "email not verified"}, nil
	}
	if !inService {
		return &pb.RemoveResp{Code: 401, ErrMsg: "not buy any package order"}, nil
	}

	resobj, pathId := self.findPathId(nodeIdStr, req.Target, false)
	if resobj != nil {
		return &pb.RemoveResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	}
	if len(pathId) == 0 {
		return &pb.RemoveResp{Code: 6, ErrMsg: "path not exists"}, nil
	}
	if !self.d.FileOwnerRemove(nodeIdStr, pathId, req.Recursive) {
		return &pb.RemoveResp{Code: 7, ErrMsg: "folder not empty"}, nil
	}
	return &pb.RemoveResp{Code: 0}, nil
}

func (self *MatadataService) findPathId(nodeId string, filePath *pb.FilePath, needFolder bool) (res *resObj, pathId []byte) {
	switch v := filePath.OneOfPath.(type) {
	case *pb.FilePath_Id:
		if len(v.Id) == 0 {
			return
		}
		ni, isFolder := self.d.FileOwnerCheckId(v.Id)
		if len(ni) == 0 {
			return &resObj{Code: 201, ErrMsg: "path is not exists"}, nil
		} else if ni != nodeId {
			return &resObj{Code: 202, ErrMsg: "wrong path owner"}, nil
		}
		if needFolder && !isFolder {
			return &resObj{Code: 203, ErrMsg: "path is not a folder"}, nil
		}
		return nil, v.Id
	case *pb.FilePath_Path:
		path := v.Path
		if path == "" || path == "/" {
			return
		}
		if path[0] != '/' {
			return &resObj{Code: 200, ErrMsg: "path must start with slash /"}, nil
		}
		var found, isFolder bool
		found, pathId, isFolder = self.d.FileOwnerIdOfFilePath(nodeId, path)
		if !found {
			return &resObj{Code: 201, ErrMsg: "path is not exists"}, nil
		}
		if needFolder && !isFolder {
			return &resObj{Code: 203, ErrMsg: "path is not a folder"}, nil
		}
		return
	}
	return &resObj{Code: 204, ErrMsg: "path is not exists"}, nil
}

const block_sep = ";"
const block_node_id_sep = ","

func fromPartitions(partitions []*pb.StorePartition) ([]string, error) {
	if len(partitions) == 0 {
		return nil, errors.New("empty partition")
	}
	res := make([]string, 0, len(partitions))
	for _, p := range partitions {
		if len(p.Block) == 0 {
			return nil, errors.New("empty block")
		}
		for _, b := range p.Block {
			if len(b.StoreNodeId) == 0 {
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

func (self *MatadataService) Move(ctx context.Context, req *pb.MoveReq) (resp *pb.MoveResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			resp = &pb.MoveResp{Code: 300, ErrMsg: fmt.Sprintf("System error: %s", er)}
		}
	}()
	checkRes, pubKey := self.checkNodeId(req.NodeId)
	if checkRes != nil {
		return &pb.MoveResp{Code: checkRes.Code, ErrMsg: checkRes.ErrMsg}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.MoveResp{Code: 4, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.MoveResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	inService, emailVerified, _, _, _, _,
		_, _, _, _, _, _ := self.d.UsageAmount(nodeIdStr)
	if !emailVerified {
		return &pb.MoveResp{Code: 400, ErrMsg: "email not verified"}, nil
	}
	if !inService {
		return &pb.MoveResp{Code: 401, ErrMsg: "not buy any package order"}, nil
	}

	resobj, source := self.findPathId(nodeIdStr, req.Source, false)
	if resobj != nil {
		return &pb.MoveResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	}
	if len(source) == 0 {
		return &pb.MoveResp{Code: 6, ErrMsg: "source path not exists"}, nil
	}

	// resobj, dest := self.findPathId(nodeIdStr, req.Dest, false)
	// if resobj != nil {
	// 	return &pb.MoveResp{Code: resobj.Code, ErrMsg: resobj.ErrMsg}, nil
	// }
	// if len(dest) == 0 {
	// 	return &pb.MoveResp{Code: 7, ErrMsg: " destination path not exists"}, nil
	// }

	return &pb.MoveResp{Code: 0}, nil

}
