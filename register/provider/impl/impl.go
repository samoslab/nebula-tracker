package impl

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"nebula-tracker/db"
	"nebula-tracker/register/random"
	"time"

	log "github.com/sirupsen/logrus"

	"golang.org/x/net/context"

	pb "github.com/spolabs/nebula/tracker/register/provider/pb"
	util_bytes "github.com/spolabs/nebula/util/bytes"
	util_hash "github.com/spolabs/nebula/util/hash"
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

func (self *ProviderRegisterService) decrypt(data []byte) ([]byte, error) {
	return rsa.DecryptPKCS1v15(rand.Reader, self.PriKey, data)
}

func (self *ProviderRegisterService) GetPublicKey(ctx context.Context, req *pb.GetPublicKeyReq) (*pb.GetPublicKeyResp, error) {
	return &pb.GetPublicKeyResp{PublicKey: self.PubKeyBytes}, nil
}

func verifySignRegisterReq(req *pb.RegisterReq, pubKey *rsa.PublicKey) error {

	// TODO
	return nil
}

func (self *ProviderRegisterService) Register(ctx context.Context, req *pb.RegisterReq) (*pb.RegisterResp, error) {
	if req.NodeIdEnc == nil || len(req.NodeIdEnc) == 0 {
		return &pb.RegisterResp{Code: 2, ErrMsg: "NodeIdEnc is required"}, nil
	}
	nodeId, err := self.decrypt(req.NodeIdEnc)
	if err != nil {
		return &pb.RegisterResp{Code: 3, ErrMsg: "decrypt NodeIdEnc error: " + err.Error()}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(nodeId)
	if db.ProviderExistsNodeId(nodeIdStr) {
		return &pb.RegisterResp{Code: 4, ErrMsg: "This NodeId is already registered"}, nil
	}
	if req.PublicKeyEnc == nil || len(req.PublicKeyEnc) == 0 {
		return &pb.RegisterResp{Code: 5, ErrMsg: "PublicKeyEnc is required"}, nil
	}
	publicKey, err := self.decrypt(req.PublicKeyEnc)
	if err != nil {
		return &pb.RegisterResp{Code: 6, ErrMsg: "decrypt PublicKeyEnc error: " + err.Error()}, nil
	}
	if len(nodeId) != 20 || !util_bytes.SameBytes(util_hash.Sha1(publicKey), nodeId) {
		return &pb.RegisterResp{Code: 7, ErrMsg: "Public Key is not match NodeId"}, nil
	}
	pubKey, err := x509.ParsePKCS1PublicKey(publicKey)
	if err != nil {
		return &pb.RegisterResp{Code: 8, ErrMsg: "Public Key can not be parsed"}, nil
	}
	if req.BillEmailEnc == nil || len(req.BillEmailEnc) == 0 {
		return &pb.RegisterResp{Code: 9, ErrMsg: "BillEmailEnc is required"}, nil
	}
	billEmail, err := self.decrypt(req.BillEmailEnc)
	if err != nil {
		return &pb.RegisterResp{Code: 10, ErrMsg: "decrypt BillEmailEnc error: " + err.Error()}, nil
	}
	if db.ProviderExistsBillEmail(string(billEmail)) {
		return &pb.RegisterResp{Code: 11, ErrMsg: "This Bill Email is already registered"}, nil
	}
	if req.EncryptKeyEnc == nil || len(req.EncryptKeyEnc) == 0 {
		return &pb.RegisterResp{Code: 12, ErrMsg: "EncryptKeyEnc is required"}, nil
	}
	encryptKey, err := self.decrypt(req.EncryptKeyEnc)
	if err != nil {
		return &pb.RegisterResp{Code: 13, ErrMsg: "decrypt EncryptKeyEnc error: " + err.Error()}, nil
	}
	// TODDO
	randomCode := random.RandomStr(8)
	db.ProviderRegister(nodeIdStr, publicKey, pubKey, string(billEmail), encryptKey, randomCode)
	self.sendVerifyCodeToBillEmail(nodeIdStr, string(billEmail), randomCode)
	return &pb.RegisterResp{Code: 0}, nil
}

func (self *ProviderRegisterService) sendVerifyCodeToBillEmail(nodeId string, email string, randomCode string) {
	// TODO
}

func (self *ProviderRegisterService) reGenerateVerifyCode(nodeId string, email string) {
	randomCode := random.RandomStr(8)
	db.ProviderUpdateVerifyCode(nodeId, randomCode)
	self.sendVerifyCodeToBillEmail(nodeId, email, randomCode)
}

func verifySignVerifyVerifyBillEmailReq(req *pb.VerifyBillEmailReq, pubKey *rsa.PublicKey) error {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	hasher.Write([]byte(req.VerifyCode))
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
}

func (self *ProviderRegisterService) VerifyBillEmail(ctx context.Context, req *pb.VerifyBillEmailReq) (*pb.VerifyBillEmailResp, error) {
	if req.NodeId == nil {
		return &pb.VerifyBillEmailResp{Code: 2, ErrMsg: "NodeId is required"}, nil
	}
	if len(req.NodeId) != 20 {
		return &pb.VerifyBillEmailResp{Code: 3, ErrMsg: "NodeId length must be 20"}, nil
	}
	pubKey := db.ProviderGetPubKey(req.NodeId)
	if pubKey == nil {
		return &pb.VerifyBillEmailResp{Code: 4, ErrMsg: "this node id is not been registered"}, nil
	}

	if err := verifySignVerifyVerifyBillEmailReq(req, pubKey); err != nil {
		return &pb.VerifyBillEmailResp{Code: 5, ErrMsg: "Verify Sign failed"}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	found, billEmail, emailVerified, randomCode, sendTime := db.ProviderGetRandomCode(nodeIdStr)
	if !found {
		return &pb.VerifyBillEmailResp{Code: 6, ErrMsg: "this node id is not been registered"}, nil
	}
	if emailVerified {
		return &pb.VerifyBillEmailResp{Code: 7, ErrMsg: "already verified contact email"}, nil
	}
	if req.VerifyCode != randomCode {
		self.reGenerateVerifyCode(nodeIdStr, billEmail)
		return &pb.VerifyBillEmailResp{Code: 8, ErrMsg: "wrong verified code, will send verify email again"}, nil
	}
	if subM := time.Now().Sub(sendTime).Minutes(); subM > 120 {
		self.reGenerateVerifyCode(nodeIdStr, billEmail)
		return &pb.VerifyBillEmailResp{Code: 9, ErrMsg: "verify code expired, will send verify email again"}, nil
	}
	db.ProviderUpdateEmailVerified(nodeIdStr)
	return &pb.VerifyBillEmailResp{Code: 0}, nil
}
func verifySignResendVerifyCodeReq(req *pb.ResendVerifyCodeReq, pubKey *rsa.PublicKey) error {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
}
func (self *ProviderRegisterService) ResendVerifyCode(ctx context.Context, req *pb.ResendVerifyCodeReq) (*pb.ResendVerifyCodeResp, error) {
	pubKey := db.ProviderGetPubKey(req.NodeId)
	if pubKey == nil {
		return nil, errors.New("this node id is not been registered")
	}

	if err := verifySignResendVerifyCodeReq(req, pubKey); err != nil {
		return nil, err
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	found, billEmail, emailVerified, _, _ := db.ProviderGetRandomCode(nodeIdStr)
	if !found {
		return nil, errors.New("this node id is not been registered")
	}
	if emailVerified {
		return nil, errors.New("already virefied！")
	}
	self.reGenerateVerifyCode(nodeIdStr, billEmail)
	return &pb.ResendVerifyCodeResp{Success: true}, nil
}

func (self *ProviderRegisterService) GetTrackerServer(ctx context.Context, req *pb.GetTrackerServerReq) (*pb.GetTrackerServerResp, error) {
	// TODO
	return nil, nil
}
