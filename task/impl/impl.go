package impl

import (
	"context"

	pb "github.com/samoslab/nebula/tracker/task/pb"
)

type ProviderTaskService struct {
}

func NewProviderTaskService() *ProviderTaskService {

	return &ProviderTaskService{}
}

func (self *ProviderTaskService) TaskList(context.Context, *pb.TaskListReq) (resp *pb.TaskListResp, err error) {

	return
}
func (self *ProviderTaskService) GetOppositeInfo(context.Context, *pb.GetOppositeInfoReq) (resp *pb.GetOppositeInfoResp, err error) {

	return
}
func (self *ProviderTaskService) GetProveInfo(context.Context, *pb.GetProveInfoReq) (resp *pb.GetProveInfoResp, err error) {

	return
}
func (self *ProviderTaskService) FinishProve(context.Context, *pb.FinishProveReq) (resp *pb.FinishProveResp, err error) {

	return
}

func (self *ProviderTaskService) FinishTask(context.Context, *pb.FinishTaskReq) (resp *pb.FinishTaskResp, err error) {

	return
}
