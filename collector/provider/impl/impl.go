package impl

import (
	"fmt"
	"io"

	pb "github.com/samoslab/nebula/tracker/collector/provider/pb"
	log "github.com/sirupsen/logrus"
)

type ProviderCollectorService struct {
}

func NewProviderCollectorService() *ProviderCollectorService {
	return &ProviderCollectorService{}
}

func (self *ProviderCollectorService) Collect(stream pb.ProviderCollectorService_CollectServer) error {
	// var pubKey *rsa.PublicKey
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return stream.SendAndClose(&pb.CollectResp{})
			}
			log.Errorf("failed to recv, error: %v", err)
			return err
		}
		// if pubKey == nil {
		// 	pubKey = db.ProviderGetPubKey(req.NodeId)
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
		// TODO
	}
}
