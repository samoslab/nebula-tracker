package tracker_client

import (
	"context"
	"encoding/hex"
	"time"

	"nebula-tracker/collector/config"

	pb "nebula-tracker/api/collector/pb"

	util_aes "github.com/samoslab/nebula/util/aes"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var encryptKey []byte
var apiToken []byte
var apiHostAndPort []string

func init() {
	ti := config.GetConsumerConfig().TrackerInterface
	var err error
	encryptKey, err = hex.DecodeString(ti.EncryptKeyHex)
	if err != nil {
		log.Fatalf("decode encrypt key Error： %s", err)
	}
	if len(encryptKey) != 16 && len(encryptKey) != 24 && len(encryptKey) != 32 {
		log.Fatalf("encrypt key length Error： %d", len(encryptKey))
	}
	apiToken = []byte(ti.ApiToken)
	if len(apiToken) == 0 {
		log.Fatalf("ApiToken is required")
	}
	apiHostAndPort = ti.ApiHostAndPort
	if len(apiHostAndPort) == 0 {
		log.Fatalf("ApiHostAndPort is required")
	}
}

func ClientPubKey(nodeId string) ([]byte, error) {
	conn, err := grpc.Dial(apiHostAndPort[0], grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pb.NewForCollectorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.ClientPubKeyReq{Timestamp: uint64(time.Now().Unix()),
		NodeId: nodeId}
	req.GenAuth(apiToken)
	resp, err := client.ClientPubKey(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			if st.Code() == codes.NotFound {
				log.Warnf("Not found %s, error: %v", nodeId, err)
				return nil, nil
			} else if st.Code() == codes.InvalidArgument || st.Code() == codes.Unauthenticated {
				panic(err)
			}
		}
		return nil, err
	}
	bs, er := util_aes.Decrypt(resp.PubKeyEnc, encryptKey)
	if er != nil {
		panic(err)
	}
	return bs, nil
}

func ProviderPubKey(nodeId string) ([]byte, error) {
	conn, err := grpc.Dial(apiHostAndPort[0], grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pb.NewForCollectorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &pb.ProviderPubKeyReq{Timestamp: uint64(time.Now().Unix()),
		NodeId: nodeId}
	req.GenAuth(apiToken)
	resp, err := client.ProviderPubKey(ctx, req)
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			if st.Code() == codes.NotFound {
				log.Warnf("Not found %s, error: %v", nodeId, err)
				return nil, nil
			} else if st.Code() == codes.InvalidArgument || st.Code() == codes.Unauthenticated {
				panic(err)
			}
		}
		return nil, err
	}
	bs, er := util_aes.Decrypt(resp.PubKeyEnc, encryptKey)
	if er != nil {
		panic(err)
	}
	return bs, nil
}
