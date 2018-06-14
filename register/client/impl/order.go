package impl

import (
	"encoding/base64"
	"nebula-tracker/db"
	"time"

	"golang.org/x/net/context"

	pb "github.com/samoslab/nebula/tracker/register/client/pb"
)

type ClientOrderService struct {
}

func NewClientOrderService() *ClientOrderService {
	cos := &ClientOrderService{}

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
		Level:       p.Level,
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
		Creation:    o.Creation,
		PackageId:   o.PackageId,
		Package:     convertPackageInfo(o.Package),
		Quanlity:    o.Quanlity,
		TotalAmount: o.TotalAmount,
		Upgraded:    o.Upgraded,
		Discount:    o.Discount,
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
	pi := db.GetPackageInfo(req.PackageId)
	if pi == nil {
		return &pb.BuyPackageResp{Code: 21, ErrMsg: "package not found"}, nil
	}
	nodeId := base64.StdEncoding.EncodeToString(req.NodeId)
	inService, level, volume, netflow, upNetflow, downNetflow, endTime := db.GetCurrentPackage(nodeId)
	var renew, upgrade bool
	if inService {
		if pi.Level < level {
			return &pb.BuyPackageResp{Code: 22, ErrMsg: "can not buy a package which level is less than current"}, nil
		} else if pi.Level == level {
			renew = true
		} else {
			upgrade = true
		}
	}
	oi := db.BuyPackage(nodeId, req.PackageId, req.Quanlity, req.CancelUnpaid, renew, endTime, upgrade)
	return &pb.BuyPackageResp{Order: convertOrderInfo(oi)}, nil
}

func (self *ClientOrderService) MyAllOrder(ctx context.Context, req *pb.MyAllOrderReq) (*pb.MyAllOrderResp, error) {
	return nil, nil
}

func (self *ClientOrderService) OrderInfo(ctx context.Context, req *pb.OrderInfoReq) (*pb.OrderInfoResp, error) {
	return nil, nil
}

func (self *ClientOrderService) UsageAmount(ctx context.Context, req *pb.UsageAmountReq) (*pb.UsageAmountResp, error) {
	return nil, nil
}
