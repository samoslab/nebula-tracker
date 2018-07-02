package main

import (
	"fmt"
	"net"

	collector_cimpl "nebula-tracker/collector/client/impl"
	"nebula-tracker/collector/config"
	collector_pimpl "nebula-tracker/collector/provider/impl"

	nsq "github.com/nsqio/go-nsq"
	pbcc "github.com/samoslab/nebula/tracker/collector/client/pb"
	pbcp "github.com/samoslab/nebula/tracker/collector/provider/pb"
	log "github.com/sirupsen/logrus"
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
	if len(conf.NsqAddrs) == 0 {
		log.Fatal("nsq addr is required")
	}
	nsqConf := nsq.NewConfig()
	producer, err := nsq.NewProducer(conf.NsqAddrs[0], nsqConf)
	if err != nil {
		log.Fatalf("failed to new producer[%s]: %v", conf.NsqAddrs[0], err)
	}
	grpcServer := grpc.NewServer()
	pbcc.RegisterClientCollectorServiceServer(grpcServer, collector_cimpl.NewClientCollectorService(producer))
	pbcp.RegisterProviderCollectorServiceServer(grpcServer, collector_pimpl.NewProviderCollectorService(producer))

	grpcServer.Serve(lis)
}
