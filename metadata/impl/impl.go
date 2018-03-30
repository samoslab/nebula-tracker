package impl

import (
	"golang.org/x/net/context"

	pb "github.com/spolabs/nebula/tracker/metadata/pb"
)

type MatadataService struct {
}

func NewMatadataService() *MatadataService {

	return &MatadataService{}
}

func (self *MatadataService) CheckFileExist(ctx context.Context, req *pb.CheckFileExistReq) (*pb.CheckFileExistResp, error) {

	return nil, nil
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
