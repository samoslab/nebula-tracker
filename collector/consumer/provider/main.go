package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"os"
	"os/signal"
	"syscall"
	"time"

	"nebula-tracker/collector/config"
	"nebula-tracker/collector/db"
	client "nebula-tracker/collector/tracker_client"

	proto "github.com/golang/protobuf/proto"
	nsq "github.com/nsqio/go-nsq"
	cache "github.com/patrickmn/go-cache"
	pb "github.com/samoslab/nebula/tracker/collector/provider/pb"
	log "github.com/sirupsen/logrus"
)

func main() {
	// if len(os.Args) != 2 {
	// 	fmt.Printf("usage: %s nsq-address\n", os.Args[0])
	// }
	conf := config.GetConsumerConfig()
	dbo := db.OpenDb(&conf.Db)
	defer dbo.Close()
	consumer := initConsumer(topic, channel, conf.NsqLookupd)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	consumer.Stop()
	<-consumer.StopChan
}

const (
	topic   = "provider"
	channel = "to_db"
)

//see: https://segmentfault.com/a/1190000009194607
func initConsumer(topic string, channel string, address string) (consumer *nsq.Consumer) {
	cfg := nsq.NewConfig()
	cfg.LookupdPollInterval = time.Second
	var err error
	consumer, err = nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		log.Fatalf("failed to new consumer, topic: %s, channel: %s: %v", topic, channel, err)
	}
	consumer.SetLogger(nil, 0)
	consumer.AddHandler(&LogConsumer{})

	if err := consumer.ConnectToNSQLookupd(address); err != nil {
		log.Fatalf("failed to connect to NSQLookupd[%s]: %v", address, err)
	}
	return
}

type LogConsumer struct{}

func (*LogConsumer) HandleMessage(msg *nsq.Message) error {
	batch := &pb.Batch{}
	err := proto.Unmarshal(msg.Body, batch)
	if err != nil {
		log.Warnf("Unmarshal data failed: %v", err)
		return nil
	}
	if len(batch.NodeId) == 0 {
		log.Warnf("nodeId is empty")
		return nil
	}
	nodeId := base64.StdEncoding.EncodeToString(batch.NodeId)
	pubKey := getPubKey(nodeId)
	if pubKey == nil {
		log.Warnf("can not find public key of provider: %s", nodeId)
		return nil
	}
	if err = batch.VerifySign(pubKey); err != nil {
		log.Warnf("Verify Sign failed, nodeId: %s, err: %s", nodeId, err)
		return nil
	}
	db.SaveFromProvider(nodeId, batch.Timestamp, batch.ActionLog)
	return nil
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
		pk := getPubKeyFromDbOrTracker(nodeIdStr)
		if pk != nil {
			pubKeyCache.Set(nodeIdStr, pk, cache.DefaultExpiration)
			return pk
		}
		return nil
	}
}

func getPubKeyFromDbOrTracker(nodeIdStr string) (pk *rsa.PublicKey) {
	pubKey := db.GetProviderPubKey(nodeIdStr)
	var err error
	if len(pubKey) == 0 {
		for i := 1; ; i++ {
			pubKey, err = client.ProviderPubKey(nodeIdStr)
			if err != nil {
				log.Warnf("get provider [%s] public key from tracker error: %v", nodeIdStr, err)
				duration := i
				if i > 30 {
					duration = 30
				}
				time.Sleep(time.Duration(duration) * time.Second)
				continue
			}
			if len(pubKey) == 0 {
				return nil
			} else {
				db.SaveProviderPubKey(nodeIdStr, pubKey)
				break
			}
		}
	}
	pk, err = x509.ParsePKCS1PublicKey(pubKey)
	if err != nil {
		log.Warnf("parse provider [%s] public key [%x] error: %v", nodeIdStr, pubKey, err)
		return nil
	}
	return pk
}
