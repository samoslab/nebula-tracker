package impl

import (
	"crypto/rsa"
	"crypto/sha256"
	"errors"
	"io"
	"nebula-tracker/db"

	log "github.com/sirupsen/logrus"
	pb "github.com/spolabs/nebula/tracker/collector/client/pb"
	util_bytes "github.com/spolabs/nebula/util/bytes"
)

type ClientCollectorService struct {
}

func NewClientCollectorService() *ClientCollectorService {

	return nil
}

func verifySignCollectReq(req *pb.CollectReq, pubKey *rsa.PublicKey) error {
	hasher := sha256.New()
	hasher.Write(req.NodeId)
	hasher.Write(util_bytes.FromUint64(req.Timestamp))
	for _, al := range req.ActionLog {
		hasher.Write(util_bytes.FromUint32(al.Type))
		// TODO
	}
	// return rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hasher.Sum(nil), req.Sign)
	return nil
}

func (self *ClientCollectorService) Collect(stream pb.ClientCollectorService_CollectServer) error {
	var pubKey *rsa.PublicKey
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			stream.SendAndClose(&pb.CollectResp{})
			return nil
		}
		if err != nil {
			log.Errorf("failed to recv: %v", err)
			return err
		}
		if pubKey == nil {
			pubKey := db.ClientGetPubKey(in.NodeId)
			if pubKey == nil {
				return errors.New("this node id is not been registered")
			}
		}
		if err := verifySignCollectReq(in, pubKey); err != nil {
			return errors.New("Verify Sign failed")
		}
		// TODO
	}
}
