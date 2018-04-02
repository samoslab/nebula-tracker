package main

import (
	"fmt"
	"log"
	"net"

	metadata_impl "nebula-tracker/metadata/impl"
	register_cimpl "nebula-tracker/register/client/impl"
	register_pimpl "nebula-tracker/register/provider/impl"

	pbm "github.com/spolabs/nebula/tracker/metadata/pb"
	pbrc "github.com/spolabs/nebula/tracker/register/client/pb"
	pbrp "github.com/spolabs/nebula/tracker/register/provider/pb"

	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 6666))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pbrp.RegisterProviderRegisterServiceServer(grpcServer, register_pimpl.NewProviderRegisterService())

	pbrc.RegisterClientRegisterServiceServer(grpcServer, register_cimpl.NewClientRegisterService())
	pbm.RegisterMatadataServiceServer(grpcServer, metadata_impl.NewMatadataService())
	grpcServer.Serve(lis)
}
