package impl

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"nebula-tracker/db"
	"nebula-tracker/register/random"
	"nebula-tracker/register/sendmail"
	"net"
	"regexp"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"golang.org/x/net/context"

	"github.com/samoslab/nebula/provider/node"
	provider_pb "github.com/samoslab/nebula/provider/pb"
	pb "github.com/samoslab/nebula/tracker/register/provider/pb"
	util_hash "github.com/samoslab/nebula/util/hash"
	util_rsa "github.com/samoslab/nebula/util/rsa"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ProviderRegisterService struct {
	PubKey      *rsa.PublicKey
	PriKey      *rsa.PrivateKey
	PubKeyBytes []byte
	PubKeyHash  []byte
}

func NewProviderRegisterService(pk *rsa.PrivateKey) *ProviderRegisterService {
	prs := &ProviderRegisterService{}
	prs.PriKey = pk
	prs.PubKey = &pk.PublicKey
	prs.PubKeyBytes = x509.MarshalPKCS1PublicKey(prs.PubKey)
	prs.PubKeyHash = util_hash.Sha1(prs.PubKeyBytes)
	return prs
}

func (self *ProviderRegisterService) decrypt(data []byte) ([]byte, error) {
	return util_rsa.DecryptLong(self.PriKey, data, node.RSA_KEY_BYTES)
}

func getClientIp(ctx context.Context) (string, error) {
	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "", fmt.Errorf("get client ip failed")
	}
	if pr.Addr == net.Addr(nil) {
		return "", fmt.Errorf("client ip peer.Addr is nil")
	}
	addSlice := strings.Split(pr.Addr.String(), ":")
	return addSlice[0], nil
}

func (self *ProviderRegisterService) GetPublicKey(ctx context.Context, req *pb.GetPublicKeyReq) (*pb.GetPublicKeyResp, error) {
	ip, err := getClientIp(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.GetPublicKeyResp{PublicKey: self.PubKeyBytes, PublicKeyHash: self.PubKeyHash, Ip: ip}, nil
}

func (self *ProviderRegisterService) Register(ctx context.Context, req *pb.RegisterReq) (*pb.RegisterResp, error) {
	if !bytes.Equal(self.PubKeyHash, req.PublicKeyHash) {
		return &pb.RegisterResp{Code: 500, ErrMsg: "tracker public key expired"}, nil
	}
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
	if len(nodeId) != 20 || !bytes.Equal(util_hash.Sha1(publicKey), nodeId) {
		return &pb.RegisterResp{Code: 7, ErrMsg: "Public Key is not match NodeId"}, nil
	}
	pubKey, err := x509.ParsePKCS1PublicKey(publicKey)
	if err != nil {
		return &pb.RegisterResp{Code: 8, ErrMsg: "Public Key can not be parsed"}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.RegisterResp{Code: 28, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err = req.VerifySign(pubKey); err != nil {
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
	// if db.ProviderExistsBillEmail(string(billEmail)) {
	// 	return &pb.RegisterResp{Code: 13, ErrMsg: "This Bill Email is already registered"}, nil
	// }
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
	if host != nil && len(host) > 0 { // prefer
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.VerifyBillEmailResp{Code: 10, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.VerifyBillEmailResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
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

func (self *ProviderRegisterService) ResendVerifyCode(ctx context.Context, req *pb.ResendVerifyCodeReq) (*pb.ResendVerifyCodeResp, error) {
	pubKey := db.ProviderGetPubKey(req.NodeId)
	if pubKey == nil {
		return nil, status.Error(codes.InvalidArgument, "this node id is not been registered")
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return nil, status.Error(codes.Unauthenticated, "auth info expired， please check your system time")
	}
	if err := req.VerifySign(pubKey); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "verify sign failed， error: %s", err)
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	found, billEmail, emailVerified, _, _ := db.ProviderGetRandomCode(nodeIdStr)
	if !found {
		return nil, status.Error(codes.InvalidArgument, "this node id is not been registered")
	}
	if emailVerified {
		return nil, status.Error(codes.AlreadyExists, "already verified！")
	}
	self.reGenerateVerifyCode(nodeIdStr, billEmail)
	return &pb.ResendVerifyCodeResp{Success: true}, nil
}

func (self *ProviderRegisterService) AddExtraStorage(ctx context.Context, req *pb.AddExtraStorageReq) (*pb.AddExtraStorageResp, error) {
	pubKey := db.ProviderGetPubKey(req.NodeId)
	if pubKey == nil {
		return nil, status.Error(codes.InvalidArgument, "this node id is not been registered")
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return nil, status.Error(codes.Unauthenticated, "auth info expired， please check your system time")
	}
	if err := req.VerifySign(pubKey); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "verify sign failed， error: %s", err)
	}
	if req.Volume <= 10000000000 {
		return nil, status.Error(codes.OutOfRange, "storage volume must more than 10G")
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	db.ProviderAddExtraStorage(nodeIdStr, req.Volume)
	return &pb.AddExtraStorageResp{Success: true}, nil
}

func (self *ProviderRegisterService) GetTrackerServer(ctx context.Context, req *pb.GetTrackerServerReq) (*pb.GetTrackerServerResp, error) {
	// TODO
	return nil, nil
}

func (self *ProviderRegisterService) GetCollectorServer(ctx context.Context, req *pb.GetCollectorServerReq) (*pb.GetCollectorServerResp, error) {
	// TODO
	return nil, nil
}

func (self *ProviderRegisterService) RefreshIp(ctx context.Context, req *pb.RefreshIpReq) (*pb.RefreshIpResp, error) {
	pubKey := db.ProviderGetPubKey(req.NodeId)
	if pubKey == nil {
		return nil, status.Error(codes.InvalidArgument, "this node id is not been registered")
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return nil, status.Error(codes.Unauthenticated, "auth info expired， please check your system time")
	}
	if err := req.VerifySign(pubKey); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "verify sign failed， error: %s", err)
	}
	ip, err := getClientIp(ctx)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "get provider ip failed， error: %s", err)
	}
	if len(ip) >= 7 {
		resp := &pb.RefreshIpResp{Ip: ip}
		addr := fmt.Sprintf("%s:%d", ip, req.Port)
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return resp, status.Errorf(codes.Unavailable, "can not connect to %s, error: %s", addr, err)
		}
		defer conn.Close()
		psc := provider_pb.NewProviderServiceClient(conn)
		err = pingProvider(psc)
		if err != nil {
			return resp, status.Errorf(codes.Unavailable, "ping %s failed, error: %s", addr, err)
		}
		nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
		db.UpdateProviderHost(nodeIdStr, ip)
		return resp, nil
	} else {
		return &pb.RefreshIpResp{}, nil
	}
}
