package main

import (
	"fmt"
	"log"
	"net"

	collector_cimpl "nebula-tracker/collector/client/impl"
	"nebula-tracker/collector/config"
	collector_pimpl "nebula-tracker/collector/provider/impl"

	pbcc "github.com/samoslab/nebula/tracker/collector/client/pb"
	pbcp "github.com/samoslab/nebula/tracker/collector/provider/pb"
	"google.golang.org/grpc"
)

func main() {
	conf := config.GetCollectorConfig()
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.Server.ListenIp, conf.Server.ListenPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// dbo := db.OpenDb(&conf.Db)
	// defer dbo.Close()

	grpcServer := grpc.NewServer()
	pbcc.RegisterClientCollectorServiceServer(grpcServer, collector_cimpl.NewClientCollectorService())
	pbcp.RegisterProviderCollectorServiceServer(grpcServer, collector_pimpl.NewProviderCollectorService())

	grpcServer.Serve(lis)
}
