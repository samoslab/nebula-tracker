package impl

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"time"

	"nebula-tracker/db"
	"nebula-tracker/register/random"

	log "github.com/sirupsen/logrus"
	pb "github.com/spolabs/nebula/tracker/register/client/pb"
	"golang.org/x/net/context"

	util_bytes "github.com/spolabs/nebula/util/bytes"
	util_hash "github.com/spolabs/nebula/util/hash"
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

func (self *ClientRegisterService) decrypt(data []byte) ([]byte, error) {
	return rsa.DecryptPKCS1v15(rand.Reader, self.PriKey, data)
}

func (self *ClientRegisterService) Register(ctx context.Context, req *pb.RegisterReq) (*pb.RegisterResp, error) {
	if req.NodeId == nil || len(req.NodeId) == 0 {
		return &pb.RegisterResp{Code: 2, ErrMsg: "NodeId is required"}, nil
	}
	if len(req.NodeId) != 20 {
		return &pb.RegisterResp{Code: 3, ErrMsg: "NodeId length must be 20"}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	if db.ClientExistsNodeId(nodeIdStr) {
		return &pb.RegisterResp{Code: 4, ErrMsg: "This NodeId is already registered"}, nil
	}
	if req.PublicKeyEnc == nil || len(req.PublicKeyEnc) == 0 {
		return &pb.RegisterResp{Code: 5, ErrMsg: "PublicKeyEnc is required"}, nil
	}
	publicKey, err := self.decrypt(req.PublicKeyEnc)
	if err != nil {
		return &pb.RegisterResp{Code: 6, ErrMsg: "decrypt PublicKeyEnc error: " + err.Error()}, nil
	}
	if !util_bytes.SameBytes(util_hash.Sha1(publicKey), req.NodeId) {
		return &pb.RegisterResp{Code: 7, ErrMsg: "Public Key is not match NodeId"}, nil
	}
	pubKey, err := x509.ParsePKCS1PublicKey(publicKey)
	if err != nil {
		return &pb.RegisterResp{Code: 8, ErrMsg: "Public Key can not be parsed"}, nil
	}
	if req.ContactEmailEnc == nil || len(req.ContactEmailEnc) == 0 {
		return &pb.RegisterResp{Code: 9, ErrMsg: "ContactEmailEnc is required"}, nil
	}
	contactEmail, err := self.decrypt(req.ContactEmailEnc)
	if err != nil {
		return &pb.RegisterResp{Code: 10, ErrMsg: "decrypt ContactEmailEnc error: " + err.Error()}, nil
	}
	if db.ClientExistsContactEmail(string(contactEmail)) {
		return &pb.RegisterResp{Code: 11, ErrMsg: "This Contact Email is already registered"}, nil
	}
	randomCode := random.RandomStr(8)
	db.ClientRegister(nodeIdStr, publicKey, pubKey, string(contactEmail), randomCode)
	self.sendVerifyCodeToContactEmail(nodeIdStr, string(contactEmail), randomCode)
	return &pb.RegisterResp{Code: 0}, nil
}

func (self *ClientRegisterService) sendVerifyCodeToContactEmail(nodeId string, email string, randomCode string) {
	// TODO
}

func (self *ClientRegisterService) reGenerateVerifyCode(nodeId string, email string) {
	randomCode := random.RandomStr(8)
	db.ClientUpdateVerifyCode(nodeId, randomCode)
	self.sendVerifyCodeToContactEmail(nodeId, email, randomCode)
}

func verifySignVerifyContactEmailReq(req *pb.VerifyContactEmailReq, pubKey *rsa.PublicKey) error {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	hasher.Write([]byte(req.VerifyCode))
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
}

func (self *ClientRegisterService) VerifyContactEmail(ctx context.Context, req *pb.VerifyContactEmailReq) (*pb.VerifyContactEmailResp, error) {
	if req.NodeId == nil {
		return &pb.VerifyContactEmailResp{Code: 2, ErrMsg: "NodeId is required"}, nil
	}
	if len(req.NodeId) != 20 {
		return &pb.VerifyContactEmailResp{Code: 3, ErrMsg: "NodeId length must be 20"}, nil
	}
	pubKey := db.ClientGetPubKey(req.NodeId)
	if pubKey == nil {
		return &pb.VerifyContactEmailResp{Code: 4, ErrMsg: "this node id is not been registered"}, nil
	}

	if err := verifySignVerifyContactEmailReq(req, pubKey); err != nil {
		return &pb.VerifyContactEmailResp{Code: 5, ErrMsg: "Verify Sign failed"}, nil
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	found, contactEmail, emailVerified, randomCode, sendTime := db.ClientGetRandomCode(nodeIdStr)
	if !found {
		return &pb.VerifyContactEmailResp{Code: 6, ErrMsg: "this node id is not been registered"}, nil
	}
	if emailVerified {
		return &pb.VerifyContactEmailResp{Code: 7, ErrMsg: "already verified contact email"}, nil
	}
	if req.VerifyCode != randomCode {
		self.reGenerateVerifyCode(nodeIdStr, contactEmail)
		return &pb.VerifyContactEmailResp{Code: 8, ErrMsg: "wrong verified code, will send verify email again"}, nil
	}
	if subM := time.Now().Sub(sendTime).Minutes(); subM > 120 {
		self.reGenerateVerifyCode(nodeIdStr, contactEmail)
		return &pb.VerifyContactEmailResp{Code: 9, ErrMsg: "verify code expired, will send verify email again"}, nil
	}
	db.ClientUpdateEmailVerified(nodeIdStr)
	return &pb.VerifyContactEmailResp{Code: 0}, nil
}

func verifySignResendVerifyCodeReq(req *pb.ResendVerifyCodeReq, pubKey *rsa.PublicKey) error {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
}
func (self *ClientRegisterService) ResendVerifyCode(ctx context.Context, req *pb.ResendVerifyCodeReq) (*pb.ResendVerifyCodeResp, error) {
	pubKey := db.ClientGetPubKey(req.NodeId)
	if pubKey == nil {
		return nil, errors.New("this node id is not been registered")
	}

	if err := verifySignResendVerifyCodeReq(req, pubKey); err != nil {
		return nil, err
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	found, contactEmail, emailVerified, _, _ := db.ClientGetRandomCode(nodeIdStr)
	if !found {
		return nil, errors.New("this node id is not been registered")
	}
	if emailVerified {
		return nil, errors.New("already virefied！")
	}
	self.reGenerateVerifyCode(nodeIdStr, contactEmail)
	return &pb.ResendVerifyCodeResp{Success: true}, nil
}

func (self *ClientRegisterService) GetTrackerServer(ctx context.Context, req *pb.GetTrackerServerReq) (*pb.GetTrackerServerResp, error) {
	// TODO
	return nil, nil
}
