package impl

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
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

const embed_metadata_max_file_size = 8192

func (self *MatadataService) CheckFileExist(ctx context.Context, req *pb.CheckFileExistReq) (*pb.CheckFileExistResp, error) {
	if req.NodeId == nil {
		return &pb.CheckFileExistResp{Code: 2, ErrMsg: "NodeId is required"}, nil
	}
	if len(req.NodeId) != 20 {
		return &pb.CheckFileExistResp{Code: 3, ErrMsg: "NodeId length must be 20"}, nil
	}
	pubKey := db.ClientGetPubKey(req.NodeId)
	if pubKey == nil {
		return &pb.CheckFileExistResp{Code: 4, ErrMsg: "this node id is not been registered"}, nil
	}
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
	if err := rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign); err != nil {
		return &pb.CheckFileExistResp{Code: 5, ErrMsg: "Verify Sign failed"}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	var parentId []byte
	if req.FilePath != "" {
		if req.FilePath[0] != '/' {
			return &pb.CheckFileExistResp{Code: 6, ErrMsg: "filePath must start with slash /"}, nil
		}
		if len(req.FilePath) > 1 {
			var found bool
			found, parentId = db.FileOwnerIdOfFilePath(nodeIdStr, req.FilePath)
			if !found {
				return &pb.CheckFileExistResp{Code: 7, ErrMsg: "filepath is not exists"}, nil
			}
		}
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
			return &pb.CheckFileExistResp{Code: 5, ErrMsg: "this file can not upload Because of laws and regulations"}, nil
		}
		if !done {
			return &pb.CheckFileExistResp{Code: 5, ErrMsg: "this file is being uploaded by other user, please wait a moment to retry"}, nil
		}
		db.FileReuse(nodeIdStr, hashStr, fileName, req.FileSize, req.FileModTime, parentId)
	} else {

	}
	// TODO
	return nil, nil
}

func fixFileName(name string) string {
	pos := strings.LastIndex(name, ".")
	if pos == -1 {
		return name + "_" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	return name[0:pos] + "_" + strconv.FormatInt(time.Now().Unix(), 10) + name[pos:]
}

func (self *MatadataService) UploadFilePrepare(ctx context.Context, req *pb.UploadFilePrepareReq) (*pb.UploadFilePrepareResp, error) {

	return nil, nil
}

func (self *MatadataService) UploadFileDone(ctx context.Context, req *pb.UploadFileDoneReq) (*pb.UploadFileDoneResp, error) {

	return nil, nil
}

func (self *MatadataService) ListFiles(ctx context.Context, req *pb.ListFilesReq) (*pb.ListFilesResp, error) {

	return nil, nil
}

func (self *MatadataService) RetrieveFile(ctx context.Context, req *pb.RetrieveFileReq) (*pb.RetrieveFileResp, error) {

	return nil, nil
}
