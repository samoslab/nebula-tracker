package impl

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"nebula-tracker/db"
	"nebula-tracker/metadata/impl"
	"runtime/debug"
	"time"

	chooser "nebula-tracker/metadata/provider_chooser"

	pb "github.com/samoslab/nebula/tracker/task/pb"
	"github.com/yanzay/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const verify_sign_expired = 300

type ProviderTaskService struct {
}

func NewProviderTaskService() *ProviderTaskService {
	return &ProviderTaskService{}
}

func (self *ProviderTaskService) TaskList(ctx context.Context, req *pb.TaskListReq) (resp *pb.TaskListResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			err = status.Errorf(codes.Internal, "System error: %s", er)
		}
	}()
	pubKey := db.ProviderGetPubKey(req.NodeId)
	if pubKey == nil {
		return nil, status.Error(codes.InvalidArgument, "this node id is not been registered")
	}
	interval := time.Now().Unix() - int64(req.Timestamp)
	if interval > verify_sign_expired || interval < 0-verify_sign_expired {
		return nil, status.Error(codes.Unauthenticated, "auth info expired， please check your system time")
	}
	if err := req.VerifySign(pubKey); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "verify sign failed， error: %s", err)
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	resp = &pb.TaskListResp{Task: db.GetTasksByProviderId(nodeIdStr), Timestamp: uint64(time.Now().Unix())}
	resp.GenAuth(x509.MarshalPKCS1PublicKey(pubKey))
	return
}
func (self *ProviderTaskService) GetOppositeInfo(ctx context.Context, req *pb.GetOppositeInfoReq) (resp *pb.GetOppositeInfoResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			err = status.Errorf(codes.Internal, "System error: %s", er)
		}
	}()
	pubKey := db.ProviderGetPubKey(req.NodeId)
	if pubKey == nil {
		return nil, status.Error(codes.InvalidArgument, "this node id is not been registered")
	}
	interval := time.Now().Unix() - int64(req.Timestamp)
	if interval > verify_sign_expired || interval < 0-verify_sign_expired {
		return nil, status.Error(codes.Unauthenticated, "auth info expired， please check your system time")
	}
	if err := req.VerifySign(pubKey); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "verify sign failed， error: %s", err)
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	task := db.GetTask(req.TaskId, nodeIdStr)
	if task == nil {
		return nil, status.Errorf(codes.NotFound, "not found or task expired")
	}
	if len(task.OppositeId) == 0 {
		return nil, status.Errorf(codes.DataLoss, "no opposite id")
	}
	info := make([]*pb.OppositeInfo, 0, len(task.OppositeId))
	ts := uint64(time.Now().Unix())
	for _, pid := range task.OppositeId {
		pro := chooser.Get(pid)
		if pro == nil {
			fmt.Println("can not find provider, nodeId: " + pid)
			continue
		}
		if pro.Port == 0 {
			continue
		}
		ticket := impl.GenTicket(nodeIdStr, pid)
		info = append(info, &pb.OppositeInfo{NodeId:pid,
			Host  :pro.Server(),
			Port  :pro.Port,
			Auth  :provider_pb.GenRetrieveAuth(pro.PublicKey, task.FileHash, task.FileSize, task.BlockHash, task.BlockSize, ts, ticket)
			Ticket:ticket})
	}
	return &pb.GetOppositeInfoResp{Timestamp:ts,OppositeId:info}
}

func (self *ProviderTaskService) FinishTask(ctx context.Context, req *pb.FinishTaskReq) (resp *pb.FinishTaskResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			err = status.Errorf(codes.Internal, "System error: %s", er)
		}
	}()
	pubKey := db.ProviderGetPubKey(req.NodeId)
	if pubKey == nil {
		return nil, status.Error(codes.InvalidArgument, "this node id is not been registered")
	}
	interval := time.Now().Unix() - int64(req.Timestamp)
	if interval > verify_sign_expired || interval < 0-verify_sign_expired {
		return nil, status.Error(codes.Unauthenticated, "auth info expired， please check your system time")
	}
	if err := req.VerifySign(pubKey); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "verify sign failed， error: %s", err)
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	db.TaskFinish(nodeIdStr,req.TaskId,req.FinishedTime,req.Success,req.Remark)
	return &pb.FinishTaskResp{},nil
}


func (self *ProviderTaskService) GetProveInfo(ctx context.Context, req *pb.GetProveInfoReq) (resp *pb.GetProveInfoResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			err = status.Errorf(codes.Internal, "System error: %s", er)
		}
	}()
	pubKey := db.ProviderGetPubKey(req.NodeId)
	if pubKey == nil {
		return nil, status.Error(codes.InvalidArgument, "this node id is not been registered")
	}
	interval := time.Now().Unix() - int64(req.Timestamp)
	if interval > verify_sign_expired || interval < 0-verify_sign_expired {
		return nil, status.Error(codes.Unauthenticated, "auth info expired， please check your system time")
	}
	if err := req.VerifySign(pubKey); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "verify sign failed， error: %s", err)
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	task := db.GetTask(req.TaskId, nodeIdStr)
	if task == nil {
		return nil, status.Errorf(codes.NotFound, "not found or task expired")
	}
	if len(task.ProofId)==0{
		return nil, status.Errorf(codes.DataLoss, "this task haven't proof id")
	}
	return
}
func (self *ProviderTaskService) FinishProve(ctx context.Context, req *pb.FinishProveReq) (resp *pb.FinishProveResp, err error) {
	defer func() {
		if er := recover(); er != nil {
			log.Errorf("Panic Error: %s, detail: %s", er, string(debug.Stack()))
			err = status.Errorf(codes.Internal, "System error: %s", er)
		}
	}()
	pubKey := db.ProviderGetPubKey(req.NodeId)
	if pubKey == nil {
		return nil, status.Error(codes.InvalidArgument, "this node id is not been registered")
	}
	interval := time.Now().Unix() - int64(req.Timestamp)
	if interval > verify_sign_expired || interval < 0-verify_sign_expired {
		return nil, status.Error(codes.Unauthenticated, "auth info expired， please check your system time")
	}
	if err := req.VerifySign(pubKey); err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "verify sign failed， error: %s", err)
	}
	nodeIdStr := base64.StdEncoding.EncodeToString(req.NodeId)
	return
}

