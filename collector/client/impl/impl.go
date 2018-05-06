package impl

import (
	"fmt"
	"io"

	pb "github.com/samoslab/nebula/tracker/collector/client/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ClientCollectorService struct {
}

func NewClientCollectorService() *ClientCollectorService {
	return &ClientCollectorService{}
}

func (self *ClientCollectorService) Collect(stream pb.ClientCollectorService_CollectServer) (er error) {
	// var pubKey *rsa.PublicKey
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return stream.SendAndClose(&pb.CollectResp{})
			}
			er = status.Errorf(codes.Unknown, "failed to recv, error: %v", err)
			log.Errorln(er)
			return
		}
		// if pubKey == nil {
		// 	pubKey = db.ClientGetPubKey(req.NodeId)
		// 	if pubKey == nil {
		// 		err = errors.New("this node id is not been registered")
		// 		log.Warn(err)
		// 		return err
		// 	}
		// }
		// if err = req.VerifySign(pubKey); err != nil {
		// 	log.Warnf("Verify Sign failed, err: %s", err)
		// 	return err
		// }
		fmt.Println(req)
		//TODO
	}
}
