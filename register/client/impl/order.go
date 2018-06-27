package impl

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"nebula-tracker/db"
	"time"

	"golang.org/x/net/context"

	pb "github.com/samoslab/nebula/tracker/register/client/pb"
)

type ClientOrderService struct {
	PubKey      *rsa.PublicKey
	PriKey      *rsa.PrivateKey
	PubKeyBytes []byte
}

func NewClientOrderService(pk *rsa.PrivateKey) *ClientOrderService {
	cos := &ClientOrderService{}
	cos.PriKey = pk
	cos.PubKey = &pk.PublicKey
	cos.PubKeyBytes = x509.MarshalPKCS1PublicKey(cos.PubKey)
	return cos
}

func (self *ClientOrderService) AllPackage(ctx context.Context, req *pb.AllPackageReq) (*pb.AllPackageResp, error) {
	ap := db.AllPackageInfo()
	res := make([]*pb.Package, 0, len(ap))
	for _, p := range ap {
		res = append(res, convertPackageInfo(p))
	}
	return &pb.AllPackageResp{AllPackage: res}, nil
}

func convertPackageInfo(p *db.PackageInfo) *pb.Package {
	return &pb.Package{Id: p.Id,
		Name:        p.Name,
		Price:       p.Price,
		Volume:      p.Volume,
		Netflow:     p.Netflow,
		UpNetflow:   p.UpNetflow,
		DownNetflow: p.DownNetflow,
		ValidDays:   p.ValidDays,
		Remark:      p.Remark}
}

func convertOrderInfo(o *db.OrderInfo) *pb.Order {
	return &pb.Order{Id: o.Id,
		Creation:    uint64(o.Creation.Unix()),
		PackageId:   o.PackageId,
		Package:     convertPackageInfo(o.Package),
		Quanlity:    o.Quanlity,
		TotalAmount: o.TotalAmount,
		Upgraded:    o.Upgraded,
		Discount:    o.Discount.String(),
		Volume:      o.Volume,
		Netflow:     o.Netflow,
		UpNetflow:   o.UpNetflow,
		DownNetflow: o.DownNetflow,
		ValidDays:   o.ValidDays,
		StartTime:   o.StartTime,
		EndTime:     o.EndTime,
		PayTime:     o.PayTime,
		Remark:      o.Remark}
}

func (self *ClientOrderService) PackageInfo(ctx context.Context, req *pb.PackageInfoReq) (*pb.PackageInfoResp, error) {
	pi := db.GetPackageInfo(req.PackageId)
	if pi == nil {
		return nil, nil
	}
	return &pb.PackageInfoResp{Package: convertPackageInfo(pi)}, nil
}

func (self *ClientOrderService) PackageDiscount(ctx context.Context, req *pb.PackageDiscountReq) (*pb.PackageDiscountResp, error) {
	m := db.GetPackageDiscount(req.PackageId)
	res := make(map[uint32]string, len(m))
	for k, v := range m {
		res[k] = v.String()
	}
	return &pb.PackageDiscountResp{Discount: res}, nil
}

func (self *ClientOrderService) BuyPackage(ctx context.Context, req *pb.BuyPackageReq) (*pb.BuyPackageResp, error) {
	if req.NodeId == nil {
		return &pb.BuyPackageResp{Code: 2, ErrMsg: "NodeId is required"}, nil
	}
	if len(req.NodeId) != 20 {
		return &pb.BuyPackageResp{Code: 3, ErrMsg: "NodeId length must be 20"}, nil
	}
	pubKey := db.ClientGetPubKey(req.NodeId)
	if pubKey == nil {
		return &pb.BuyPackageResp{Code: 4, ErrMsg: "this node id is not been registered"}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.BuyPackageResp{Code: 10, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.BuyPackageResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
	}
	nodeId := base64.StdEncoding.EncodeToString(req.NodeId)
	found, _, emailVerified, _, _ := db.ClientGetRandomCode(nodeId)
	if !found || !emailVerified {
		return &pb.BuyPackageResp{Code: 9, ErrMsg: "email not verified"}, nil
	}
	pi := db.GetPackageInfo(req.PackageId)
	if pi == nil {
		return &pb.BuyPackageResp{Code: 21, ErrMsg: "package not found"}, nil
	}

	inService, _, packageId, volume, _, _, _, endTime := db.GetCurrentPackage(nodeId)
	var renew, upgrade bool
	if inService {
		if pi.Volume < volume {
			return &pb.BuyPackageResp{Code: 22, ErrMsg: "can not buy a package which volume is less than current"}, nil
		} else if pi.Volume == volume {
			renew = true
		} else {
			upgrade = true
		}
	}
	oi := db.BuyPackage(nodeId, req.PackageId, req.Quanlity, req.CancelUnpaid, renew, endTime, upgrade, packageId)
	return &pb.BuyPackageResp{Order: convertOrderInfo(oi)}, nil
}

func (self *ClientOrderService) MyAllOrder(ctx context.Context, req *pb.MyAllOrderReq) (*pb.MyAllOrderResp, error) {
	if req.NodeId == nil {
		return &pb.MyAllOrderResp{Code: 2, ErrMsg: "NodeId is required"}, nil
	}
	if len(req.NodeId) != 20 {
		return &pb.MyAllOrderResp{Code: 3, ErrMsg: "NodeId length must be 20"}, nil
	}
	pubKey := db.ClientGetPubKey(req.NodeId)
	if pubKey == nil {
		return &pb.MyAllOrderResp{Code: 4, ErrMsg: "this node id is not been registered"}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.MyAllOrderResp{Code: 10, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.MyAllOrderResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
	}
	nodeId := base64.StdEncoding.EncodeToString(req.NodeId)
	found, _, emailVerified, _, _ := db.ClientGetRandomCode(nodeId)
	if !found || !emailVerified {
		return &pb.MyAllOrderResp{Code: 9, ErrMsg: "email not verified"}, nil
	}
	all := db.MyAllOrder(nodeId, req.OnlyNotExpired)
	res := make([]*pb.Order, 0, len(all))
	for _, o := range all {
		res = append(res, convertOrderInfo(o))
	}
	return &pb.MyAllOrderResp{MyAllOrder: res}, nil
}

func (self *ClientOrderService) OrderInfo(ctx context.Context, req *pb.OrderInfoReq) (*pb.OrderInfoResp, error) {
	if req.NodeId == nil {
		return &pb.OrderInfoResp{Code: 2, ErrMsg: "NodeId is required"}, nil
	}
	if len(req.NodeId) != 20 {
		return &pb.OrderInfoResp{Code: 3, ErrMsg: "NodeId length must be 20"}, nil
	}
	pubKey := db.ClientGetPubKey(req.NodeId)
	if pubKey == nil {
		return &pb.OrderInfoResp{Code: 4, ErrMsg: "this node id is not been registered"}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.OrderInfoResp{Code: 10, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.OrderInfoResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
	}
	if len(req.OrderId) == 0 {
		return &pb.OrderInfoResp{Code: 15, ErrMsg: "orderId is required"}, nil
	}
	nodeId := base64.StdEncoding.EncodeToString(req.NodeId)
	found, _, emailVerified, _, _ := db.ClientGetRandomCode(nodeId)
	if !found || !emailVerified {
		return &pb.OrderInfoResp{Code: 9, ErrMsg: "email not verified"}, nil
	}

	oi := db.GetOrderInfo(nodeId, req.OrderId)
	return &pb.OrderInfoResp{Order: convertOrderInfo(oi)}, nil
}

func (self *ClientOrderService) RechargeAddress(ctx context.Context, req *pb.RechargeAddressReq) (*pb.RechargeAddressResp, error) {
	if req.NodeId == nil {
		return &pb.RechargeAddressResp{Code: 2, ErrMsg: "NodeId is required"}, nil
	}
	if len(req.NodeId) != 20 {
		return &pb.RechargeAddressResp{Code: 3, ErrMsg: "NodeId length must be 20"}, nil
	}
	pubKey := db.ClientGetPubKey(req.NodeId)
	if pubKey == nil {
		return &pb.RechargeAddressResp{Code: 4, ErrMsg: "this node id is not been registered"}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.RechargeAddressResp{Code: 10, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.RechargeAddressResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
	}
	nodeId := base64.StdEncoding.EncodeToString(req.NodeId)
	found, _, emailVerified, _, _ := db.ClientGetRandomCode(nodeId)
	if !found || !emailVerified {
		return &pb.RechargeAddressResp{Code: 9, ErrMsg: "email not verified"}, nil
	}
	addr := db.GetRechargeAddress(nodeId)
	return &pb.RechargeAddressResp{RechargeAddress: addr, Balance: db.GetBalance(nodeId)}, nil
}

func (self *ClientOrderService) PayOrder(ctx context.Context, req *pb.PayOrderReq) (*pb.PayOrderResp, error) {
	if req.NodeId == nil {
		return &pb.PayOrderResp{Code: 2, ErrMsg: "NodeId is required"}, nil
	}
	if len(req.NodeId) != 20 {
		return &pb.PayOrderResp{Code: 3, ErrMsg: "NodeId length must be 20"}, nil
	}
	pubKey := db.ClientGetPubKey(req.NodeId)
	if pubKey == nil {
		return &pb.PayOrderResp{Code: 4, ErrMsg: "this node id is not been registered"}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.PayOrderResp{Code: 10, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.PayOrderResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
	}
	if len(req.OrderId) == 0 {
		return &pb.PayOrderResp{Code: 15, ErrMsg: "orderId is required"}, nil
	}
	nodeId := base64.StdEncoding.EncodeToString(req.NodeId)
	found, _, emailVerified, _, _ := db.ClientGetRandomCode(nodeId)
	if !found || !emailVerified {
		return &pb.PayOrderResp{Code: 9, ErrMsg: "email not verified"}, nil
	}
	oi := db.GetOrderInfo(nodeId, req.OrderId)
	if oi.Paid {
		return &pb.PayOrderResp{Code: 16, ErrMsg: "order is paid"}, nil
	}
	balance := db.GetBalance(nodeId)
	if balance < oi.TotalAmount {
		return &pb.PayOrderResp{Code: 20, ErrMsg: "balance is not enough"}, nil
	}
	db.PayOrder(nodeId, req.OrderId, oi.TotalAmount, oi.ValidDays, oi.PackageId, oi.Volume, oi.Netflow, oi.UpNetflow, oi.DownNetflow)
	return &pb.PayOrderResp{}, nil
}

func (self *ClientOrderService) UsageAmount(ctx context.Context, req *pb.UsageAmountReq) (*pb.UsageAmountResp, error) {
	if req.NodeId == nil {
		return &pb.UsageAmountResp{Code: 2, ErrMsg: "NodeId is required"}, nil
	}
	if len(req.NodeId) != 20 {
		return &pb.UsageAmountResp{Code: 3, ErrMsg: "NodeId length must be 20"}, nil
	}
	pubKey := db.ClientGetPubKey(req.NodeId)
	if pubKey == nil {
		return &pb.UsageAmountResp{Code: 4, ErrMsg: "this node id is not been registered"}, nil
	}
	if uint64(time.Now().Unix())-req.Timestamp > verify_sign_expired {
		return &pb.UsageAmountResp{Code: 10, ErrMsg: "auth info expired， please check your system time"}, nil
	}
	if err := req.VerifySign(pubKey); err != nil {
		return &pb.UsageAmountResp{Code: 5, ErrMsg: "Verify Sign failed: " + err.Error()}, nil
	}
	nodeId := base64.StdEncoding.EncodeToString(req.NodeId)
	inService, _, packageId, volume, netflow, upNetflow, downNetflow, usageVolume, usageNetflow, usageUpNetflow, usageDownNetflow, endTime := db.UsageAmount(nodeId)
	if !inService {
		return &pb.UsageAmountResp{}, nil
	}
	return &pb.UsageAmountResp{PackageId: packageId, Volume: volume,
		Netflow:          netflow,
		UpNetflow:        upNetflow,
		DownNetflow:      downNetflow,
		UsageVolume:      usageVolume,
		UsageNetflow:     usageNetflow,
		UsageUpNetflow:   usageUpNetflow,
		UsageDownNetflow: usageDownNetflow,
		EndTime:          uint64(endTime.Unix())}, nil
}
