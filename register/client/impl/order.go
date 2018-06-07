package impl

import (
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

	return nil, nil
}

func (self *ClientOrderService) PackageInfo(ctx context.Context, req *pb.PackageInfoReq) (*pb.PackageInfoResp, error) {

	return nil, nil
}
func (self *ClientOrderService) BuyPackage(ctx context.Context, req *pb.BuyPackageReq) (*pb.BuyPackageResp, error) {
	return nil, nil
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
