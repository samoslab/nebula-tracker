package impl

import (
	"io"

	nsq "github.com/nsqio/go-nsq"
	pb "github.com/samoslab/nebula/tracker/collector/provider/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ProviderCollectorService struct {
	producer *nsq.Producer
}

func NewProviderCollectorService(p *nsq.Producer) *ProviderCollectorService {
	return &ProviderCollectorService{producer: p}
}

const topic = "provider"

func (self *ProviderCollectorService) Collect(stream pb.ProviderCollectorService_CollectServer) (er error) {
	// TODO use nsq, add req to nsq, use another app consumer to save to db
	// var nodeIdStr string
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
		// 	nodeIdStr = base64.StdEncoding.EncodeToString(req.NodeId)
		// 	pubKey = getPubKey(nodeIdStr)
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
		// db.SaveFromProvider(nodeIdStr, req.Timestamp, req.ActionLog)
		// fmt.Printf("%+v\n", req.Data)
		if len(req.Data) > 0 {
			err := self.producer.Publish(topic, req.Data)
			if err != nil {
				log.Errorln(er)
			}
		}
		// fmt.Println(len(req.Data))
		// umData := &pb.Batch{}
		// err = proto.Unmarshal(req.Data, umData)
		// if err == nil {
		// 	fmt.Printf("%+v\n", umData)
		// }
	}
}
