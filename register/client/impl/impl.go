package impl

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"

	log "github.com/sirupsen/logrus"
	pb "github.com/spolabs/nebula/tracker/register/client/pb"
	"golang.org/x/net/context"
)

type ClientRegisterService struct {
	PubKey      *rsa.PublicKey
	PriKey      *rsa.PrivateKey
	PubKeyBytes []byte
}

func NewClientRegisterService() *ClientRegisterService {
	crs := &ClientRegisterService{}
	pk, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatalf("GenerateKey failed:%s", err.Error())
	}
	crs.PriKey = pk
	crs.PubKey = &pk.PublicKey
	crs.PubKeyBytes = x509.MarshalPKCS1PublicKey(crs.PubKey)
	return crs
}

func (self *ClientRegisterService) GetPublicKey(ctx context.Context, req *pb.GetPublicKeyReq) (*pb.GetPublicKeyResp, error) {
	return &pb.GetPublicKeyResp{PublicKey: self.PubKeyBytes}, nil
}

func (self *ClientRegisterService) Register(ctx context.Context, req *pb.RegisterReq) (*pb.RegisterResp, error) {

	return nil, nil
}

func (self *ClientRegisterService) VerifyContactEmail(ctx context.Context, req *pb.VerifyContactEmailReq) (*pb.VerifyContactEmailResp, error) {

	return nil, nil
}

func (self *ClientRegisterService) GetTrackerServer(ctx context.Context, req *pb.GetTrackerServerReq) (*pb.GetTrackerServerResp, error) {

	return nil, nil
}
