package impl

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"

	log "github.com/sirupsen/logrus"

	"golang.org/x/net/context"

	pb "github.com/spolabs/nebula/tracker/register/provider/pb"
)

type ProviderRegisterService struct {
	PubKey      *rsa.PublicKey
	PriKey      *rsa.PrivateKey
	PubKeyBytes []byte
}

func NewProviderRegisterService() *ProviderRegisterService {
	prs := &ProviderRegisterService{}
	pk, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		log.Fatalf("GenerateKey failed:%s", err.Error())
	}
	prs.PriKey = pk
	prs.PubKey = &pk.PublicKey
	prs.PubKeyBytes = x509.MarshalPKCS1PublicKey(prs.PubKey)
	return prs
}

func (self *ProviderRegisterService) GetPublicKey(ctx context.Context, req *pb.GetPublicKeyReq) (*pb.GetPublicKeyResp, error) {
	return &pb.GetPublicKeyResp{PublicKey: self.PubKeyBytes}, nil
}

func (self *ProviderRegisterService) Register(ctx context.Context, req *pb.RegisterReq) (*pb.RegisterResp, error) {

	return nil, nil
}

func (self *ProviderRegisterService) VerifyBillEmail(ctx context.Context, req *pb.VerifyBillEmailReq) (*pb.VerifyBillEmailResp, error) {

	return nil, nil
}

func (self *ProviderRegisterService) ResendVerifyCode(ctx context.Context, req *pb.ResendVerifyCodeReq) (*pb.ResendVerifyCodeResp, error) {

	return nil, nil
}

func (self *ProviderRegisterService) GetTrackerServer(ctx context.Context, req *pb.GetTrackerServerReq) (*pb.GetTrackerServerResp, error) {

	return nil, nil
}
