package main

import (
	"fmt"
	"log"
	"nebula-tracker/config"
	"nebula-tracker/db"
	"nebula-tracker/task/impl"
	"net"

	pb "github.com/samoslab/nebula/tracker/task/pb"
	"google.golang.org/grpc"
)

func main() {
	conf := config.GetTaskConfig()

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.Server.ListenIp, conf.Server.ListenPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	dbo := db.OpenDb(&conf.Db)
	defer dbo.Close()
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(520 * 1024))
	pb.RegisterProviderTaskServiceServer(grpcServer, impl.NewProviderTaskService())
	grpcServer.Serve(lis)

}
