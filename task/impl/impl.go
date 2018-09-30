package impl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"math/big"
	"math/rand"
	"nebula-tracker/db"
	"nebula-tracker/metadata/impl"
	"runtime/debug"
	"strings"
	"time"

	chooser "nebula-tracker/metadata/provider_chooser"

	"github.com/Nik-U/pbc"
	provider_pb "github.com/samoslab/nebula/provider/pb"
	pb "github.com/samoslab/nebula/tracker/task/pb"
	util_bytes "github.com/samoslab/nebula/util/bytes"
	log "github.com/sirupsen/logrus"
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
		info = append(info, &pb.OppositeInfo{NodeId: pid,
			Host:   pro.Server(),
			Port:   pro.Port,
			Auth:   provider_pb.GenRetrieveAuth(pro.PublicKey, task.FileHash, task.FileSize, task.BlockHash, task.BlockSize, ts, ticket),
			Ticket: ticket})
	}
	return &pb.GetOppositeInfoResp{Timestamp: ts, Info: info}, nil
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
	remark := req.Remark
	if len(remark) > 250 {
		remark = remark[:250]
	}
	db.TaskFinish(req.TaskId, nodeIdStr, req.FinishedTime, req.Success, remark)
	return &pb.FinishTaskResp{}, nil
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
	if len(task.ProofId) > 0 {
		chunkSize, seq, randomNum := db.GetProofInfo(task.ProofId)
		m := make(map[uint32][]byte, len(seq))
		for i, q := range seq {
			m[q] = randomNum[i]
		}
		return &pb.GetProveInfoResp{ProofId: task.ProofId,
			ChunkSize: chunkSize,
			ChunkSeq:  m}, nil
	} else {
		chunkSize, paramStr, _, _, _, _ := db.GetProofMetadata(task.FileId, base64.StdEncoding.EncodeToString(task.BlockHash))
		count := int((task.BlockSize + uint64(chunkSize) - 1) / uint64(chunkSize))
		choose := 80
		if count < choose {
			choose = count
		}
		m, seq, randNum, er := challenge(paramStr, count, choose)
		if er != nil {
			db.TaskRemove(task.Id, nodeIdStr, "PROVE")
			err = status.Errorf(codes.Unavailable, "this prove task unavailable, task id: %x, error: %s", task.Id, er)
			fmt.Println(err)
			return
		}
		db.SaveProofInfo(task.Id, nodeIdStr, task.FileId, task.BlockHash, task.BlockSize, seq, randNum)
		return &pb.GetProveInfoResp{ProofId: task.ProofId,
			ChunkSize: chunkSize,
			ChunkSeq:  m}, nil
	}

}

func challenge(paramStr string, count int, choose int) (q map[uint32][]byte, seq []uint32, randomNum [][]byte, err error) {
	pairing, er := pbc.NewPairingFromString(paramStr)
	if er != nil {
		err = er
		return
	}
	if choose > count {
		choose = count
	}
	q = make(map[uint32][]byte, choose)
	seq = make([]uint32, 0, choose)
	randomNum = make([][]byte, 0, choose)
	for _, i := range rand.Perm(count)[0:choose] {
		bs := pairing.NewZr().Rand().Bytes()
		q[uint32(i)] = bs
		seq = append(seq, uint32(i))
		randomNum = append(randomNum, bs)
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
	task := db.GetTask(req.TaskId, nodeIdStr)
	if task == nil {
		return nil, status.Errorf(codes.NotFound, "not found or task expired")
	}
	if !bytes.Equal(req.ProofId, task.ProofId) {
		return nil, status.Errorf(codes.InvalidArgument, "task proof id not same")
	}
	pass := false
	if len(req.Result) > 0 {
		_, seq, randomNum := db.GetProofInfo(task.ProofId)
		m := make(map[uint32][]byte, len(seq))
		for i, q := range seq {
			m[q] = randomNum[i]
		}
		_, paramStr, generator, provePubKey, random, phi := db.GetProofMetadata(task.FileId, base64.StdEncoding.EncodeToString(task.BlockHash))
		pass, err = verify(paramStr, generator, provePubKey, phi, m, random, req.Result)
		if err != nil {
			db.TaskRemove(task.Id, nodeIdStr, "PROVE")
			err = status.Errorf(codes.Unavailable, "this prove task unavailable, task id: %x, error: %s", task.Id, err)
			fmt.Println(err)
		}

	}
	remark := req.Remark
	if len(remark) > 250 {
		remark = remark[:250]
	}
	db.ProofFinish(req.TaskId, nodeIdStr, req.FinishedTime, pass, remark, req.ProofId, req.Result)
	return &pb.FinishProveResp{}, nil
}
func verify(paramStr string, generator []byte, pubKey []byte, d [][]byte, q map[uint32][]byte, u []byte, proveRes []byte) (pass bool, err error) {
	pairing, er := pbc.NewPairingFromString(paramStr)
	if er != nil {
		err = er
		return
	}
	var r string
	for _, str := range strings.Split(paramStr, "\n") {
		if len(str) > 2 && str[:2] == "r " {
			r = str[2:]
		}
	}
	if len(r) == 0 {
		err = fmt.Errorf("can not get r")
		return
	}
	or := new(big.Int)
	or.SetString(r, 10)
	prove := new(big.Int)
	prove.SetBytes(proveRes)
	prove.Mod(prove, or)
	cal := pairing.NewG1().Set1()
	s := pairing.NewG1().Set1()
	for k, v := range q {
		ev := pairing.NewZr().SetBytes(v)
		e4 := pairing.NewG1().SetBytes(d[k-1])
		e5 := pairing.NewG1().PowZn(e4, ev)
		cal = pairing.NewG1().Mul(cal, e5)

		e1 := pairing.NewG1().SetFromHash(hash(pubKey, k))
		e7 := pairing.NewG1().PowZn(e1, ev)
		s = pairing.NewG1().Mul(s, e7)
	}
	eu := pairing.NewG1().SetBytes(u)

	//
	// // uBig := new(big.Int)
	// // uBig.SetBytes(u)
	// // big := new(big.Int)
	// // big.Exp(uBig, prove, or)
	// // s = pairing.NewG1().MulBig(s, big)
	s = pairing.NewG1().Mul(s, pairing.NewG1().PowBig(eu, prove))

	// s = pairing.NewG1().Mul(s, pairing.NewG1().PowZn(eu, pairing.NewZr().SetBytes(proveRes)))

	temp1 := pairing.NewGT().Pair(cal, pairing.NewG2().SetBytes(generator))
	temp2 := pairing.NewGT().Pair(s, pairing.NewG2().SetBytes(pubKey))
	// fmt.Printf("%s\n", temp1)
	// fmt.Printf("%s\n", temp2)
	return temp1.Equals(temp2), nil
}
func hash(pubKeyBytes []byte, i uint32) []byte {
	hasher := sha256.New()
	hasher.Write(pubKeyBytes)
	hasher.Write(util_bytes.FromUint32(i))
	return hasher.Sum(nil)
}
