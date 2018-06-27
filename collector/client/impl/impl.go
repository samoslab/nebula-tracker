package impl

import (
	"crypto/rsa"
	"crypto/x509"
	"fmt"
	"io"
	"time"

	client "nebula-tracker/collector/tracker_client"

	"github.com/gogo/protobuf/proto"
	cache "github.com/patrickmn/go-cache"
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

var pubKeyCache = cache.New(20*time.Minute, 10*time.Minute)

func getPubKey(nodeIdStr string) *rsa.PublicKey {
	pubKey, found := pubKeyCache.Get(nodeIdStr)
	if found {
		b, ok := pubKey.(*rsa.PublicKey)
		if !ok {
			panic("Error type get from cache")
		}
		return b
	} else {
		for i := 1; ; i++ {
			pk, err := client.ClientPubKey(nodeIdStr)
			if err == nil {
				pubKey, err := x509.ParsePKCS1PublicKey(pk)
				if err != nil {
					panic(err)
				}
				pubKeyCache.Set(nodeIdStr, pubKey, cache.DefaultExpiration)
				// TODO save public key to db
				return pubKey
			}
			log.Errorf("the %d times get client public key failed: %s", i, err)
			time.Sleep(30 * time.Second)
		}
	}

}

func (self *ClientCollectorService) Collect(stream pb.ClientCollectorService_CollectServer) (er error) {
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
		// db.SaveFromClient(nodeIdStr, req.Timestamp, req.ActionLog)
		// fmt.Printf("%+v\n", req)
		fmt.Println(len(req.Data))
		umData := &pb.Batch{}
		err = proto.Unmarshal(req.Data, umData)
		if err == nil {
			fmt.Printf("%+v\n", umData)
		}
	}
}
