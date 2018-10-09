package main

import (
	"fmt"
	"io"
	"nebula-tracker/test/upload/server/pb"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type UploadService struct {
}

func (self *UploadService) Upload(stream pb.UploadService_UploadServer) error {
	var file *os.File
	first := true
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if first {
			first = false
			if _, err := os.Stat(req.Name); !os.IsNotExist(err) {
				fmt.Println("file exist, filename: " + req.Name)
				return status.Errorf(codes.AlreadyExists, "file exist, filename: %s", req.Name)
			}
			if file, err = os.OpenFile(
				req.Name,
				os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
				0600); err != nil {
				return err
			}
			defer file.Close()
		}
		if len(req.Data) == 0 {
			break
		}
		if _, err = file.Write(req.Data); err != nil {
			return err
		}
	}
	if err := stream.SendAndClose(&pb.UploadResp{}); err != nil {
		return err
	}
	return nil
}

func (self *UploadService) Download(req *pb.DownloadReq, stream pb.UploadService_DownloadServer) error {
	if _, err := os.Stat(req.Name); os.IsNotExist(err) {
		fmt.Println("file not exist, filename: " + req.Name)
		return status.Errorf(codes.NotFound, "file not exist, filename: %s", req.Name)
	}
	file, err := os.Open(req.Name)
	if err != nil {
		return err
	}
	defer file.Close()
	buf := make([]byte, req.BatchSize)
	for {
		bytesRead, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if bytesRead > 0 {
			stream.Send(&pb.DownloadResp{Data: buf[:bytesRead]})
		}
		if uint32(bytesRead) < req.BatchSize {
			break
		}
	}
	return nil
}

func main() {
	// portMapping(6666)
	listen := ":6666"
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		fmt.Printf("failed to listen: %s, error: %s\n", listen, err.Error())
		return
	}
	grpcServer := grpc.NewServer()
	uploadService := &UploadService{}
	pb.RegisterUploadServiceServer(grpcServer, uploadService)
	grpcServer.Serve(lis)
}
