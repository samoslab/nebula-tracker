package impl

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"nebula-tracker/db"
	"nebula-tracker/register/random"
	"nebula-tracker/register/sendmail"
	"net"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"golang.org/x/net/context"

	provider_pb "github.com/spolabs/nebula/provider/pb"
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

func getClientIp(ctx context.Context) (string, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "", fmt.Errorf("get client ip failed")
	}
	if pr.Addr == net.Addr(nil) {
		return "", fmt.Errorf("client ip peer.Addr is nil")
	}
	fmt.Println(pr.Addr.String())
	addSlice := strings.Split(pr.Addr.String(), ":")
	return addSlice[0], nil
}

func (self *ProviderRegisterService) GetPublicKey(ctx context.Context, req *pb.GetPublicKeyReq) (*pb.GetPublicKeyResp, error) {
	ip, err := getClientIp(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.GetPublicKeyResp{PublicKey: self.PubKeyBytes, Ip: ip}, nil
}

func verifySignRegisterReq(req *pb.RegisterReq, pubKey *rsa.PublicKey) error {
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return errors.New("auth info expired， please check your system time")
	}
	hasher := sha256.New()
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	hasher.Write(req.NodeIdEnc)
	hasher.Write(req.PublicKeyEnc)
	hasher.Write(req.EncryptKeyEnc)
	hasher.Write(req.WalletAddressEnc)
	hasher.Write(req.BillEmailEnc)
	hasher.Write(util_bytes.FromUint64(req.MainStorageVolume))
	hasher.Write(util_bytes.FromUint64(req.UpBandwidth))
	hasher.Write(util_bytes.FromUint64(req.DownBandwidth))
	hasher.Write(util_bytes.FromUint64(req.TestUpBandwidth))
	hasher.Write(util_bytes.FromUint64(req.TestDownBandwidth))
	hasher.Write(util_bytes.FromUint64(math.Float64bits(req.Availability)))
	hasher.Write(util_bytes.FromUint32(req.Port))
	hasher.Write(req.HostEnc)
	hasher.Write(req.DynamicDomainEnc)
	for _, val := range req.ExtraStorageVolume {
		hasher.Write(util_bytes.FromUint64(val))
	}
	return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
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
	if err = verifySignRegisterReq(req, pubKey); err != nil {
		return &pb.RegisterResp{Code: 9, ErrMsg: "verify sign failed: " + err.Error()}, nil
	}
	if req.BillEmailEnc == nil || len(req.BillEmailEnc) == 0 {
		return &pb.RegisterResp{Code: 10, ErrMsg: "BillEmailEnc is required"}, nil
	}
	billEmail, err := self.decrypt(req.BillEmailEnc)
	if err != nil {
		return &pb.RegisterResp{Code: 11, ErrMsg: "decrypt BillEmailEnc error: " + err.Error()}, nil
	}
	email_re := regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
	if !email_re.MatchString(string(billEmail)) {
		return &pb.RegisterResp{Code: 12, ErrMsg: "Bill Email is invalid."}, nil
	}
	if db.ProviderExistsBillEmail(string(billEmail)) {
		return &pb.RegisterResp{Code: 13, ErrMsg: "This Bill Email is already registered"}, nil
	}
	if req.EncryptKeyEnc == nil || len(req.EncryptKeyEnc) == 0 {
		return &pb.RegisterResp{Code: 14, ErrMsg: "EncryptKeyEnc is required"}, nil
	}
	encryptKey, err := self.decrypt(req.EncryptKeyEnc)
	if err != nil {
		return &pb.RegisterResp{Code: 15, ErrMsg: "decrypt EncryptKeyEnc error: " + err.Error()}, nil
	}
	if req.WalletAddressEnc == nil || len(req.WalletAddressEnc) == 0 {
		return &pb.RegisterResp{Code: 16, ErrMsg: "WalletAddressEnc is required"}, nil
	}
	walletAddress, err := self.decrypt(req.WalletAddressEnc)
	if err != nil {
		return &pb.RegisterResp{Code: 17, ErrMsg: "decrypt WalletAddressEnc error: " + err.Error()}, nil
	}
	if req.MainStorageVolume < 10000000000 {
		return &pb.RegisterResp{Code: 18, ErrMsg: "storage volume is too low"}, nil
	}
	if req.UpBandwidth < 1000000 || req.TestUpBandwidth < 500000 {
		return &pb.RegisterResp{Code: 19, ErrMsg: "upload bandwidth is too low"}, nil
	}
	if req.DownBandwidth < 4000000 || req.TestDownBandwidth < 2000000 {
		return &pb.RegisterResp{Code: 20, ErrMsg: "download bandwidth is too low"}, nil
	}
	if req.Availability < 0.98 {
		return &pb.RegisterResp{Code: 21, ErrMsg: "availability must more than 98%."}, nil
	}
	if req.Port < 1 || req.Port > 65535 {
		return &pb.RegisterResp{Code: 22, ErrMsg: "port must between 1 to 65535."}, nil
	}
	host, err := self.decrypt(req.HostEnc)
	if err != nil {
		return &pb.RegisterResp{Code: 23, ErrMsg: "decrypt HostEnc error: " + err.Error()}, nil
	}
	dynamicDomain, err := self.decrypt(req.DynamicDomainEnc)
	if err != nil {
		return &pb.RegisterResp{Code: 24, ErrMsg: "decrypt DynamicDomainEnc error: " + err.Error()}, nil
	}
	if (host == nil || len(host) == 0) && (dynamicDomain == nil || len(dynamicDomain) == 0) {
		return &pb.RegisterResp{Code: 25, ErrMsg: "host is required"}, nil
	}
	var hostStr string
	if host != nil && len(host) > 0 {
		hostStr = string(host)
	} else if dynamicDomain != nil && len(dynamicDomain) > 0 {
		hostStr = string(dynamicDomain)
	}
	providerAddr := fmt.Sprintf("%s:%d", hostStr, req.Port)
	conn, err := grpc.Dial(providerAddr, grpc.WithInsecure())
	if err != nil {
		return &pb.RegisterResp{Code: 26, ErrMsg: "can not connect, error: " + err.Error()}, nil
	}
	defer conn.Close()
	psc := provider_pb.NewProviderServiceClient(conn)
	err = pingProvider(psc)
	if err != nil {
		return &pb.RegisterResp{Code: 27, ErrMsg: "ping failed, error: " + err.Error()}, nil
	}
	storageVolume := []uint64{req.MainStorageVolume}
	if req.ExtraStorageVolume != nil && len(req.ExtraStorageVolume) > 0 {
		storageVolume = make([]uint64, 1, 1+len(req.ExtraStorageVolume))
		storageVolume[0] = req.MainStorageVolume
		for i, v := range req.ExtraStorageVolume {
			storageVolume[i+1] = v
		}
	}
	randomCode := random.RandomStr(8)
	db.ProviderRegister(nodeIdStr, publicKey, pubKey, string(billEmail), encryptKey, string(walletAddress), storageVolume, req.UpBandwidth,
		req.DownBandwidth, req.TestUpBandwidth, req.TestDownBandwidth, req.Availability,
		req.Port, string(host), string(dynamicDomain), randomCode)
	self.sendVerifyCodeToBillEmail(nodeIdStr, string(billEmail), randomCode)
	return &pb.RegisterResp{Code: 0}, nil
}

func pingProvider(client provider_pb.ProviderServiceClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := client.Ping(ctx, &provider_pb.PingReq{})
	return err
}

func (self *ProviderRegisterService) sendVerifyCodeToBillEmail(nodeId string, email string, randomCode string) {
	sendmail.Send(email, "Nebula Provider Register Bill Email Verify Code", fmt.Sprintf("verify code is %s, sent at %s",
		randomCode, time.Now().UTC().Format("2006-01-02 15:04:05 UTC")))
}

func (self *ProviderRegisterService) reGenerateVerifyCode(nodeId string, email string) {
	randomCode := random.RandomStr(8)
	db.ProviderUpdateVerifyCode(nodeId, randomCode)
	self.sendVerifyCodeToBillEmail(nodeId, email, randomCode)
}

const verify_sign_expired = 15

func verifySignVerifyVerifyBillEmailReq(req *pb.VerifyBillEmailReq, pubKey *rsa.PublicKey) error {
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return errors.New("auth info expired， please check your system time")
	}
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
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return errors.New("auth info expired， please check your system time")
	}
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
		return nil, errors.New("already verified！")
	}
	self.reGenerateVerifyCode(nodeIdStr, billEmail)
	return &pb.ResendVerifyCodeResp{Success: true}, nil
}

func (self *ProviderRegisterService) GetTrackerServer(ctx context.Context, req *pb.GetTrackerServerReq) (*pb.GetTrackerServerResp, error) {
	// TODO
	return nil, nil
}
