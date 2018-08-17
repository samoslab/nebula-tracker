package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"nebula-tracker/db"
	"net"
	"runtime/debug"

	"github.com/gogo/protobuf/proto"
	util_aes "github.com/samoslab/nebula/util/aes"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "nebula-tracker/api/collector/pb"
	"nebula-tracker/config"
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
	pb.RegisterForCollectorServiceServer(grpcServer, newForCollectorService())

	grpcServer.Serve(lis)
}

func randAesKey(bits int) string {
	token := make([]byte, bits)
	_, err := rand.Read(token)
	if err != nil {
		log.Errorf("generate AES key err: %s", err)
	}
	return hex.EncodeToString(token)
}

type ForCollectorService struct{}

func newForCollectorService() *ForCollectorService {
	return &ForCollectorService{}
}

func (self *ForCollectorService) ClientPubKey(ctx context.Context, req *pb.ClientPubKeyReq) (resp *pb.ClientPubKeyResp, err error) {
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
	if len(req.NodeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "node id is required.")
	}

	pubKey := db.ClientGetPubKeyBytesByNodeId(req.NodeId)
	if len(pubKey) == 0 {
		return nil, status.Error(codes.NotFound, "node id not exist: "+req.NodeId)
	}
	en, err := util_aes.Encrypt(pubKey, encryptKey)
	if err != nil {
		return nil, status.Error(codes.Internal, "encrypt public key failed: "+err.Error())
	} else {
		return &pb.ClientPubKeyResp{PubKeyEnc: en}, nil
	}
}

func (self *ForCollectorService) ProviderPubKey(ctx context.Context, req *pb.ProviderPubKeyReq) (resp *pb.ProviderPubKeyResp, err error) {
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
	if len(req.NodeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "node id is required.")
	}

	pubKey := db.ProviderGetPubKeyBytesByNodeId(req.NodeId)
	if len(pubKey) == 0 {
		return nil, status.Error(codes.NotFound, "node id not exist: "+req.NodeId)
	}
	en, err := util_aes.Encrypt(pubKey, encryptKey)
	if err != nil {
		return nil, status.Error(codes.Internal, "encrypt public key failed: "+err.Error())
	} else {
		return &pb.ProviderPubKeyResp{PubKeyEnc: en}, nil
	}
}

const KEY_LAST_START string = "collector-last-summarize"

func (self *ForCollectorService) GetLastSummary(ctx context.Context, req *pb.GetLastSummaryReq) (resp *pb.GetLastSummaryResp, err error) {
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
	intVal, _, _ := db.GetKvStore(KEY_LAST_START)
	return &pb.GetLastSummaryResp{LastSummary: intVal}, nil
}

func (self *ForCollectorService) HourlyUpdate(ctx context.Context, req *pb.HourlyUpdateReq) (resp *pb.HourlyUpdateResp, err error) {
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
	data, err := util_aes.Decrypt(req.Data, encryptKey)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	hs := &pb.HourlySummary{}
	err = proto.Unmarshal(data, hs)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	db.SaveHourlySummary(KEY_LAST_START, hs)
	return &pb.HourlyUpdateResp{}, nil
}
