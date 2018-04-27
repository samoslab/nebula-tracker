package main

import (
	"fmt"
	"io"
	"nebula-tracker/test/upload/server/pb"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/prestonTao/upnp"
	"google.golang.org/grpc"
)

type UploadService struct {
}

func (self *UploadService) Upload(stream pb.UploadService_UploadServer) error {
	file, err := os.OpenFile(
		strconv.FormatInt(time.Now().Unix(), 16)+".file",
		os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		0600)
	if err != nil {
		return err
	}
	defer file.Close()
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
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
	file, err := os.Open("test.file")
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

func portMapping(port int) {
	upnpMan := new(upnp.Upnp)
	if err := upnpMan.AddPortMapping(port, port, "TCP"); err != nil {
		fmt.Println("use upnp port mapping failed: " + err.Error())
	} else {
		fmt.Println("use upnp port mapping success.")
	}
}

func main() {
	portMapping(6666)
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
