package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	pb "nebula-tracker/api/check-availability/pb"

	"nebula-tracker/db"
	"net"
	"net/http"
	"runtime/debug"

	log "github.com/sirupsen/logrus"
	"golang.org/x/protobuf/proto"
	"google.golang.org/grpc"

	"nebula-tracker/config"

	util_aes "github.com/samoslab/nebula/util/aes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var encryptKey []byte

func main() {
	// fmt.Println(randAesKey(16))
	conf := config.GetInterfaceConfig()
	var err error
	encryptKey, err = hex.DecodeString(conf.EncryptKeyHex)
	if err != nil {
		log.Fatalf("decode encrypt key Error： %s", err)
	}
	if len(encryptKey) != 16 && len(encryptKey) != 24 && len(encryptKey) != 32 {
		log.Fatalf("encrypt key length Error： %d", len(encryptKey))
	}
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", conf.ListenIp, conf.ListenPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	dbo := db.OpenDb(&conf.Db)
	defer dbo.Close()

	grpcServer := grpc.NewServer()
	pb.RegisterCheckavAilabilityServiceServer(grpcServer, newCheckavAilabilityService())

	grpcServer.Serve(lis)

}

type CheckavAilabilityService struct {
}

func newCheckavAilabilityService() *CheckavAilabilityService {
	return &CheckavAilabilityService{}
}

func (self *CheckavAilabilityService) FindProvider(ctx context.Context, req *pb.FindProviderReq) (resp *pb.FindProviderResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			err = status.Errorf(codes.Internal, "System error: %s", er)
		}
	}()
	conf := config.GetInterfaceConfig()
	if err = req.CheckAuth([]byte(conf.AuthToken), int64(conf.AuthValidSec)); err != nil {
		return
	}
	pis := db.FindProviderForCheck(req.Locality)
	ps := make([]*pb.Provider, 0, len(pis))
	for _, p := range pis {
		ps = append(ps, &pb.Provider{NodeId: p.NodeId,
			PublicKey:     p.PublicKey,
			Port:          p.Port,
			Host:          p.Host,
			DynamicDomain: p.DynamicDomain,
			LastConnect:   p.LastConnect})
	}
	data, err := proto.Marshal(&pb.BatchProvider{P: ps})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	en, err := util_aes.Encrypt(data, encryptKey)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.FindProviderResp{Data: en}, nil
}

func (self *CheckavAilabilityService) UpdateStatus(ctx context.Context, req *pb.UpdateStatusReq) (resp *pb.UpdateStatusResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			err = status.Errorf(codes.Internal, "System error: %s", er)
		}
	}()
	conf := config.GetInterfaceConfig()
	if err = req.CheckAuth([]byte(conf.AuthToken), int64(conf.AuthValidSec)); err != nil {
		return
	}
	bs, err := util_aes.Decrypt(req.Data, encryptKey)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	batch := &pb.BatchProviderStatus{}
	err = proto.Unmarshal(bs, batch)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	db.ProviderUpdateStatus(req.Locality, batch.Ps...)
	return &pb.UpdateStatusResp{}, nil
}

func recoverErr(w http.ResponseWriter, r *http.Request) {
	if err := recover(); err != nil {
		debug.PrintStack()
		log.Println(string(debug.Stack()))
		//TODO
	}
}

func randAesKey(bits int) string {
	token := make([]byte, bits)
	_, err := rand.Read(token)
	if err != nil {
		log.Errorf("generate AES key err: %s", err)
	}
	return hex.EncodeToString(token)
}
