package main

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"log"
	"net"

	"nebula-tracker/config"
	"nebula-tracker/db"
	metadata_impl "nebula-tracker/metadata/impl"
	chooser "nebula-tracker/metadata/provider_chooser"
	register_cimpl "nebula-tracker/register/client/impl"
	register_pimpl "nebula-tracker/register/provider/impl"

	pbm "github.com/samoslab/nebula/tracker/metadata/pb"
	pbrc "github.com/samoslab/nebula/tracker/register/client/pb"
	pbrp "github.com/samoslab/nebula/tracker/register/provider/pb"

	"google.golang.org/grpc"
)

func main() {
	conf := config.GetTrackerConfig()
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.Server.ListenIp, conf.Server.ListenPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	pk, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatalf("GenerateKey failed:%s", err.Error())
	}
	dbo := db.OpenDb(&conf.Db)
	defer dbo.Close()
	chooser.StartAutoUpdate()
	defer chooser.StopAutoUpdate()
	grpcServer := grpc.NewServer()
	pbrp.RegisterProviderRegisterServiceServer(grpcServer, register_pimpl.NewProviderRegisterService(pk))
	pbrc.RegisterClientRegisterServiceServer(grpcServer, register_cimpl.NewClientRegisterService(pk))
	pbrc.RegisterOrderServiceServer(grpcServer, register_cimpl.NewClientOrderService(pk))
	pbm.RegisterMatadataServiceServer(grpcServer, metadata_impl.NewMatadataService(pk))

	grpcServer.Serve(lis)

}
